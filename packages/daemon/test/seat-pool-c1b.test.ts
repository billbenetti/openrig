// c1b of T-005481 (T-005673): S1 and S2 of REVISAO-c1, with N4 and N6. The harness is the e-c1 one (the production
// schema, the real queue, slots, reader, planner, policy, executor and history; tmux, the seats' activity, the host's
// load and the demand command faked), wired as startup.ts wires it: the plan reads the sessions out of provider usage
// and the host's load; the executor reads the usage-limited sessions before the demand command and before each move.
//
//   S1: a parked (or free) seat out of provider usage: receives null and no move (the recheck), as the plan reads it.
//   S2: with parked_max_load_per_cpu_percent and the load above it, the plan leaves the parked seat out of the receivers:
//       the row counts as not served (an add), with no parked_load skip at every pass.
//   N4: a plan "parked" whose seat became free moves it as free; load1 null does not refuse.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { PodRepository } from "../src/domain/pod-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { WatchdogHistoryLog, type WatchdogHistoryEntry } from "../src/domain/watchdog-history-log.js";
import { parseWatchdogSpec, watchdogJobTarget } from "../src/domain/watchdog-policy-engine.js";
import type { HostSample } from "../src/domain/launch-budget.js";
import {
  readSeatPoolState,
  SeatPoolExecutor,
  seatPoolQueueEffects,
  SeatPoolSlots,
  seatSessionName,
  type SeatPoolEffects,
} from "../src/domain/seat-pool-executor.js";
import {
  makeSeatPoolPolicy,
  parseSeatPoolSpec,
  planSeatPool,
  seatPoolCanReceive,
  type SeatPoolIntent,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import type { Policy, PolicyJob } from "../src/domain/policies/types.js";

const RIG = "r";
const MODEL = "dev-impl@r";
const MODEL_LOGICAL = "dev.impl";
const LEAD = "lead@r";
const T0 = Date.parse("2026-11-11T04:00:00.000Z");
const IDLE_SINCE = new Date(T0 - 7_200_000).toISOString();
const iso = (ms: number) => new Date(ms).toISOString();
const QUIET = new Set(["seat_pool_steady", "seat_pool_busy", "not_due"]);
const ON = { dispatch_to_parked: {} };
/** idle_grace_s high: no free seat drains during a case; cooldown 0. */
const BASE: Record<string, unknown> = { min: 0, cooldown_s: 0, idle_grace_s: 86400, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}" };
const DISPATCHED = "seat_pool_dispatched";
const SKIPPED = "seat_pool_dispatch_skipped";
const FAILED = "seat_pool_dispatch_failed";

/** idle: at its prompt, nothing in the background. background: at its prompt with a background shell. working: not at
 *  its prompt. needs_input: a prompt waits for an answer. */
type Mode = "idle" | "working" | "background" | "needs_input";
interface Seat { logicalId: string; session: string; nodeId: string }
interface PoolSeat extends Seat { slotId: number }

class Harness {
  readonly db: Database.Database;
  readonly rigRepo: RigRepository;
  readonly queue: QueueRepository;
  readonly outbox: OutboxHandler;
  readonly jobs: WatchdogJobsRepository;
  readonly history: WatchdogHistoryLog;
  readonly slots: SeatPoolSlots;
  readonly rigId: string;
  readonly podId: string;
  readonly model: Seat;
  private clockMs = T0;
  get nowMs(): number { return this.clockMs; }
  set nowMs(ms: number) { this.clockMs = ms; vi.setSystemTime(ms); }
  jobId = "";
  calls: string[] = [];
  readonly mode = new Map<string, Mode>();
  host: HostSample = { cpuCount: 4, load1: 0.4, freeMemoryBytes: 1e10 };
  demandDesired = 0;
  /** Every OPENRIG_SEAT_POOL_SLOTS the demand command was given, parsed. */
  demandSlots: Array<Array<{ session: string; phase: string; receives?: string | null }>> = [];
  /** Sessions whose provider account is out of usage, as startup's usageLimitedSessions reads them. */
  limited = new Set<string>();
  /** The executor's usage read fails (the provider is down); counts every read. */
  limitedThrows = false;
  limitedReads = 0;
  executor!: SeatPoolExecutor;
  policy!: Policy;

  constructor() {
    this.nowMs = T0;
    this.db = createDb();
    migrate(this.db, ALL_MIGRATIONS);
    const bus = new EventBus(this.db);
    this.rigRepo = new RigRepository(this.db);
    const rig = this.rigRepo.createRig(RIG);
    this.rigId = rig.id;
    this.podId = new PodRepository(this.db).createPod(rig.id, "dev", "Dev").id;
    this.queue = new QueueRepository(this.db, bus);
    this.outbox = new OutboxHandler(this.db);
    this.queue.attachOutbox(this.outbox);
    this.jobs = new WatchdogJobsRepository(this.db);
    this.history = new WatchdogHistoryLog(this.db);
    this.slots = new SeatPoolSlots(this.db);
    this.model = this.addSeat(MODEL_LOGICAL, "working");
    this.boot();
  }

  // ---- the rig ---------------------------------------------------------------------------------------------------
  addSeat(logicalId: string, mode: Mode): Seat {
    const node = this.rigRepo.addNode(this.rigId, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: this.podId });
    const session = seatSessionName(logicalId, RIG);
    this.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${logicalId}`, node.id, session);
    this.mode.set(node.id, mode);
    return { logicalId, session, nodeId: node.id };
  }

  /** A seat the pool added (it has an Operation-Id), live. */
  poolSeat(logicalId: string, mode: Mode): PoolSeat {
    const seat = this.addSeat(logicalId, mode);
    const slot = this.slots.insert({ poolJobId: this.jobId, logicalId, nodeId: seat.nodeId, phase: "live", intent: null, opId: `op-${logicalId}`, at: iso(T0 - 7_200_000) });
    return { ...seat, slotId: slot.slotId };
  }

  setMode(seat: Seat, mode: Mode): void { this.mode.set(seat.nodeId, mode); }

  sessionRunning(nodeId: string): boolean {
    const row = this.db.prepare("SELECT status FROM sessions WHERE node_id = ? ORDER BY rowid DESC LIMIT 1").get(nodeId) as { status: string } | undefined;
    return row?.status === "running";
  }

  endSession(seat: Seat): void {
    this.db.prepare("UPDATE sessions SET status = 'exited' WHERE node_id = ?").run(seat.nodeId);
  }

  // ---- the queue -------------------------------------------------------------------------------------------------
  async row(destination: string, tags: string[] = ["tarefa-nova"], source = LEAD): Promise<string> {
    return (await this.queue.create({ sourceSession: source, destinationSession: destination, body: "work", tags, nudge: false } as never)).qitemId;
  }

  /** Row A, sent to the seat, claimed by it `claimAgoS` ago and parked by it `parkAgoS` ago with a timer of its own. */
  async park(seat: Seat, opts: { gate?: string; wakeAfter?: number; claimAgoS?: number; parkAgoS?: number } = {}): Promise<string> {
    const { gate = "external:x", wakeAfter = 2700, claimAgoS = 3600, parkAgoS = 1800 } = opts;
    const back = this.nowMs;
    this.nowMs = back - claimAgoS * 1000;
    const a = await this.row(seat.session);
    this.queue.claim({ qitemId: a, destinationSession: seat.session });
    this.nowMs = back - parkAgoS * 1000;
    this.queue.update({ qitemId: a, actorSession: seat.session, state: "blocked", blockedOn: gate, wakeAfterSeconds: wakeAfter, transitionNote: `parked on ${gate}` } as never);
    this.nowMs = back;
    return a;
  }

  get(qitemId: string): { destinationSession: string; state: string; tags: string[] | null; resolution: string | null; blockedOn: string | null } {
    return this.queue.getById(qitemId)! as never;
  }

  // ---- the job ---------------------------------------------------------------------------------------------------
  register(context: Record<string, unknown>): void {
    this.jobId = this.jobs.register({
      policy: "seat-pool", specYaml: JSON.stringify({ policy: "seat-pool", target: { session: MODEL }, context }), targetSession: MODEL,
      intervalSeconds: 30, registeredBySession: LEAD,
    } as never).jobId;
  }

  readJob(jobId: string): { active: boolean; targetSession: string; spec: SeatPoolSpec | null } | null {
    const job = this.jobs.getById(jobId);
    if (!job) return null;
    const parsed = parseWatchdogSpec(job.specYaml);
    const pool = parseSeatPoolSpec(parsed.context);
    return { active: job.state === "active", targetSession: watchdogJobTarget(job, parsed.target).session, spec: "spec" in pool ? pool.spec : null };
  }

  spec(): SeatPoolSpec { return this.readJob(this.jobId)!.spec!; }

  /** As startup.ts: the plan's read passes the usage-limited sessions; both reads pass the host's load. */
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }, usageLimited?: ReadonlySet<string>): SeatPoolState {
    return readSeatPoolState({
      usageLimited: usageLimited ? (sessionName: string) => usageLimited.has(sessionName) : undefined,
      hostSample: () => this.host,
      db: this.db,
      slots: this.slots,
      openRows: (sessionName) => this.queue.listOpenObligations(sessionName, 500),
      rowFacts: (sessionName) => this.queue.listPoolSeatRowFacts(sessionName),
      sessionAddresses: (sessionName) => [sessionName],
      heldMessages: (nodeId) => this.outbox.heldCount(nodeId),
      seatStopped: (nodeId) => !this.sessionRunning(nodeId),
      seatState: (nodeId) => {
        if (!this.sessionRunning(nodeId)) return null;
        const mode = this.mode.get(nodeId) ?? "idle";
        return {
          activity: mode === "working" ? "running" : "idle-at-prompt",
          needsInput: { count: mode === "needs_input" ? 1 : 0 },
          backgroundWork: mode === "background" ? { rung: "self-report", observedAt: IDLE_SINCE } : null,
          changedAt: IDLE_SINCE,
        } as never;
      },
      restoring: () => false,
      demand: (jobId) => this.executor.demandSample(jobId),
    }, job);
  }

  state(): SeatPoolState { return this.readState({ jobId: this.jobId, targetSession: MODEL, spec: this.spec() }, this.limited); }

  effects(): SeatPoolEffects {
    const queueEffects = seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "eval" }) });
    const logicalOf = (nodeId: string) =>
      (this.db.prepare("SELECT logical_id FROM nodes WHERE id = ?").get(nodeId) as { logical_id: string } | undefined)?.logical_id ?? null;
    return {
      ...queueEffects,
      addSlot: async ({ memberId }) => { this.calls.push(`add ${memberId}`); return { ok: false, code: "eval_no_add", error: "the eval adds no seat" }; },
      operationNodeId: () => null,
      launchSlot: async () => ({ ok: false, code: "eval_no_launch", error: "the eval launches no seat" }),
      sessionRunning: (nodeId) => this.sessionRunning(nodeId),
      lifecycle: async (_nodeId, fn) => fn(),
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: `sess-${logicalOf(nodeId)}` }) as never,
      stopSlot: async ({ sessionName }) => { this.calls.push(`stop ${sessionName}`); return { ok: false, code: "eval_no_stop", error: "the eval stops no seat" }; },
      removeSlot: async ({ nodeId }) => {
        const logical = logicalOf(nodeId);
        this.calls.push(`remove ${logical ?? nodeId}`);
        if (!logical) return { ok: false, code: "node_not_found", error: "the node is gone" };
        if (this.queue.listOpenObligations(seatSessionName(logical, RIG), 10).length > 0) return { ok: false, code: "active_qitems", error: `${logical} still has open rows` };
        this.db.prepare("DELETE FROM sessions WHERE node_id = ?").run(nodeId);
        this.rigRepo.deleteNode(nodeId);
        return { ok: true };
      },
      runCommand: async (command, env) => {
        this.calls.push(`run ${command.argv.join(" ")}`);
        if (env["OPENRIG_SEAT_POOL_SLOTS"] !== undefined) this.demandSlots.push(JSON.parse(env["OPENRIG_SEAT_POOL_SLOTS"]));
        return { exitCode: 0, stdout: JSON.stringify({ desired: this.demandDesired }) };
      },
      cleanSlot: async ({ sessionName }) => { this.calls.push(`clean ${sessionName}`); return { ok: true }; },
      relaunchSeat: async ({ logicalId }) => { this.calls.push(`relaunch ${logicalId}`); return { ok: false, code: "eval_no_relaunch", error: "the eval relaunches no seat" }; },
    };
  }

  boot(): void {
    const executor = new SeatPoolExecutor({
      slots: this.slots,
      readJob: (jobId) => this.readJob(jobId),
      readState: (job) => this.readState(job),
      effects: this.effects(),
      hostSample: () => this.host,
      usageLimited: async () => {
        this.limitedReads += 1;
        if (this.limitedThrows) throw new Error("provider read failed");
        return this.limited;
      },
      record: (jobId, entry) => {
        this.history.record({ jobId, evaluatedAt: iso(this.nowMs), outcome: entry.failed ? "failed" : "skipped", skipReason: entry.reason, evaluationNotes: entry.notes });
      },
      now: () => new Date(this.nowMs),
      effectTimeoutMs: 15_000,
    });
    this.executor = executor;
    this.policy = makeSeatPoolPolicy({
      readState: async (job) => this.readState(job, this.limited),
      submit: (jobId, intents) => executor.submit(jobId, intents),
      now: () => this.nowMs,
    });
  }

  /** One scheduler pass for the job, as the watchdog engine runs it: evaluate, record a loud skip, wait for the run. */
  async tick(): Promise<void> {
    const job = this.jobs.getByIdOrThrow(this.jobId);
    const parsed = parseWatchdogSpec(job.specYaml);
    const policyJob = {
      jobId: job.jobId, policy: job.policy, target: watchdogJobTarget(job, parsed.target), message: parsed.message ?? undefined,
      intervalSeconds: job.intervalSeconds, activeWakeIntervalSeconds: job.activeWakeIntervalSeconds, scanIntervalSeconds: job.scanIntervalSeconds,
      context: parsed.context, lastEvaluationAt: job.lastEvaluationAt, lastFireAt: job.lastFireAt, registeredBySession: job.registeredBySession,
      registeredAt: job.registeredAt, watchedFilePath: null, thresholdBytes: null, thresholdTokens: null, thresholdPercent: null,
    } as unknown as PolicyJob;
    const outcome = await this.policy.evaluate(policyJob);
    if (outcome.action === "skip" && !QUIET.has(outcome.reason)) {
      this.history.record({ jobId: job.jobId, evaluatedAt: iso(this.nowMs), outcome: "skipped", skipReason: outcome.reason, evaluationNotes: outcome.notes ?? null });
    }
    await this.executor.settled(job.jobId);
  }

  async tickAt(offsetSeconds: number): Promise<void> {
    this.nowMs = T0 + offsetSeconds * 1000;
    await this.tick();
  }

  async submit(intents: SeatPoolIntent[]): Promise<void> {
    expect(this.executor.submit(this.jobId, intents)).toBe(true);
    await this.executor.settled(this.jobId);
  }

  events(reason: string): WatchdogHistoryEntry[] {
    return this.history.listForJob(this.jobId, 100_000).filter((e) => e.skipReason === reason).reverse();
  }

  notes(reason: string): Array<Record<string, unknown>> {
    return this.events(reason).map((e) => e.evaluationNotes as Record<string, unknown>);
  }
}
describe("c1b T-005481 (T-005673): the parked dispatch reads the usage limit and the load as the plan does", () => {
  let h: Harness;

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    h = new Harness();
  });
  afterEach(async () => {
    await h.executor.stop();
    h.db.close();
    vi.useRealTimers();
  });

  it("S1 + N6: receives is null for a parked and for a free seat out of provider usage; a parked seat in usage still says \"parked\"", async () => {
    h.register({ ...BASE, max: 5, ...ON, demand_cmd: { argv: ["demand"] } });
    h.demandDesired = 3;
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const f = h.poolSeat("dev.impl-s2", "idle");
    const q = h.poolSeat("dev.impl-s3", "background");
    await h.park(q);
    h.limited.add(p.session);
    h.limited.add(f.session);
    await h.tickAt(0);
    expect(h.demandSlots.at(-1)).toEqual([
      { session: p.session, phase: "live", receives: null },
      { session: f.session, phase: "live", receives: null },
      { session: q.session, phase: "live", receives: "parked" },
    ]);
    // The plan reads the same: a waiting row goes to the parked seat in usage, never to the two out of it.
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: q.session, mode: "parked", parkedQitemId: expect.any(String) }]);
  });

  it("S1 (recheck): the plan gave the row to a parked seat and its account ran out of usage before the move: no move, parked_changed", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    h.nowMs = T0 + 61_000;
    const b = await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const intent = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    expect(intent).toMatchObject({ kind: "dispatch", qitemId: b, slotId: p.slotId, mode: "parked" });
    h.limited.add(p.session);
    await h.submit([intent]);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.notes(SKIPPED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, code: "parked_changed" }]);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("N6 (recheck): the plan gave the row to a free seat whose account ran out of usage before the move: no move, as a seat that changed", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const f = h.poolSeat("dev.impl-s1", "idle");
    h.nowMs = T0 + 61_000;
    const b = await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const intent = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    expect(intent).toMatchObject({ kind: "dispatch", qitemId: b, slotId: f.slotId });
    h.limited.add(f.session);
    await h.submit([intent]);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.events(SKIPPED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("N6 without dispatch_to_parked: the free seat out of usage takes no row in the recheck and says null in receives", async () => {
    h.register({ ...BASE, max: 2, demand_cmd: { argv: ["demand"] } });
    h.demandDesired = 1;
    const f = h.poolSeat("dev.impl-s1", "idle");
    h.nowMs = T0 + 61_000;
    const b = await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const intent = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    expect(intent).toMatchObject({ kind: "dispatch", qitemId: b, slotId: f.slotId });
    h.limited.add(f.session);
    await h.submit([intent]);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.events(SKIPPED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.demandSlots.at(-1)).toEqual([{ session: f.session, phase: "live", receives: null }]);
  });

  it("the usage read fails: no seat counts as out of usage (as the plan's read), and it is read once per run", async () => {
    h.register({ ...BASE, max: 3, ...ON, demand_cmd: { argv: ["demand"] } });
    h.demandDesired = 2;
    const f = h.poolSeat("dev.impl-s1", "idle");
    const g = h.poolSeat("dev.impl-s2", "idle");
    h.nowMs = T0 + 1;
    const b1 = await h.row(MODEL);
    h.nowMs = T0 + 2;
    const b2 = await h.row(MODEL);
    h.limitedThrows = true;
    h.limitedReads = 0;
    await h.tickAt(61);
    expect(h.get(b1).destinationSession).toBe(f.session);
    expect(h.get(b2).destinationSession).toBe(g.session);
    // Two moves and the demand command in one run: one read.
    expect(h.limitedReads).toBe(1);
  });

  it("S2: load above parked_max_load_per_cpu_percent: the plan leaves the parked seat out, the row is demand (an add), and no parked_load skip repeats", async () => {
    h.register({ ...BASE, max: 3, dispatch_to_parked: { parked_max_load_per_cpu_percent: 80 } });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    h.host = { cpuCount: 4, load1: 3.6, freeMemoryBytes: 1e10 };
    const b = await h.row(MODEL);
    const plan = planSeatPool(h.spec(), h.state(), T0 + 61_000, 600_000);
    expect(plan.intents.filter((i) => i.kind === "dispatch")).toEqual([]);
    // The parked seat still holds its work and the row is demand: one seat more, not two.
    expect(plan.intents.filter((i) => i.kind === "add")).toEqual([{ kind: "add", count: 1 }]);
    expect(plan.target).toBe(2);
    await h.tickAt(61);
    await h.tickAt(130);
    await h.tickAt(200);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.events(SKIPPED)).toHaveLength(0);
    expect(h.calls.filter((c) => c.startsWith("add "))).not.toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // The load falls to the limit (not above it): the parked seat receives.
    h.host = { cpuCount: 4, load1: 3.2, freeMemoryBytes: 1e10 };
    await h.tickAt(270);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, mode: "parked", parkedQitemId: expect.any(String) }]);
  });

  it("S2 (receives): with the load above the limit the demand command reads null for the parked seat and \"free\" for the free one", async () => {
    h.register({ ...BASE, max: 5, dispatch_to_parked: { parked_max_load_per_cpu_percent: 80 }, demand_cmd: { argv: ["demand"] } });
    h.demandDesired = 2;
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const f = h.poolSeat("dev.impl-s2", "idle");
    h.host = { cpuCount: 4, load1: 3.6, freeMemoryBytes: 1e10 };
    await h.tickAt(0);
    expect(h.demandSlots.at(-1)).toEqual([
      { session: p.session, phase: "live", receives: null },
      { session: f.session, phase: "live", receives: "free" },
    ]);
    h.host = { cpuCount: 4, load1: 3.2, freeMemoryBytes: 1e10 };
    await h.tickAt(30);
    expect(h.demandSlots.at(-1)!.find((s) => s.session === p.session)!.receives).toBe("parked");
  });

  it("S2 (recheck kept): the load rises above the limit between the plan and the move: parked_load, the row stays", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { parked_max_load_per_cpu_percent: 80 } });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    h.nowMs = T0 + 61_000;
    const b = await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const intent = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    expect(intent).toMatchObject({ slotId: p.slotId, mode: "parked" });
    h.host = { cpuCount: 4, load1: 3.6, freeMemoryBytes: 1e10 };
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, code: "parked_load", loadPerCpuPercent: 90, limit: 80 }]);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("N4 (a): the plan said \"parked\" and the seat became free before the move: it takes the row as a free seat", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    const a = await h.park(p);
    h.nowMs = T0 + 61_000;
    const b = await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const intent = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    expect(intent).toMatchObject({ slotId: p.slotId, mode: "parked" });
    // Its row closes and its background shell ends: free.
    h.queue.update({ qitemId: a, actorSession: p.session, state: "done", closureReason: "no-follow-on", transitionNote: "done" } as never);
    h.setMode(p, "idle");
    await h.submit([intent]);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, mode: "free" }]);
    expect(h.events(SKIPPED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: p.session, state: "pending" });
  });

  it("N4 (b): with the limit set and load1 null (no reading), the parked seat receives: an unknown load does not refuse", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { parked_max_load_per_cpu_percent: 80 } });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    h.host = { cpuCount: 4, load1: null, freeMemoryBytes: 1e10 } as unknown as HostSample;
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, mode: "parked", parkedQitemId: expect.any(String) }]);
    expect(h.events(SKIPPED)).toHaveLength(0);
  });
});

// c1c of T-005481 (T-005758): S3 of REVISAO-c1b. The usage read of a run is not reused across a lifecycle step: an
// account that runs out while a slow command of the run (here prepare_cmd) runs is seen by the demand command at the
// run's end. The harness's usage read returns the SAME mutable Set (so a mutation leaks into a cached read); the real
// read (usageLimitedSessions) returns a new Set each time, which these tests reproduce by assigning h.limited.
describe("c1c T-005481 (T-005758): the demand command reads the usage limit after the run's lifecycle steps (S3)", () => {
  let h: Harness;

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    h = new Harness();
  });
  afterEach(async () => {
    await h.executor.stop();
    h.db.close();
    vi.useRealTimers();
  });

  /** The executor's own effects (the object startup hands it), so a test can make one command slow. */
  const effectsOf = (x: Harness) => (x.executor as unknown as { deps: { effects: SeatPoolEffects } }).deps.effects;

  it("S3: a parked seat whose account runs out while prepare_cmd runs (after the run's dispatch read the usage) says null to demand_cmd", async () => {
    h.register({ ...BASE, max: 5, ...ON, demand_cmd: { argv: ["demand"] }, prepare_cmd: { argv: ["prepare"] } });
    h.demandDesired = 3;
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const w = h.poolSeat("dev.impl-s2", "background");
    await h.park(w);
    // A seat the pool added and has not started: the run's launch runs prepare_cmd for it.
    const s = h.addSeat("dev.impl-s3", "idle");
    h.endSession(s);
    const added = h.slots.insert({ poolJobId: h.jobId, logicalId: s.logicalId, nodeId: s.nodeId, phase: "added", intent: null, opId: `op-${s.logicalId}`, at: iso(T0) });
    h.nowMs = T0 + 61_000;
    const b = await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const dispatch = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    expect(dispatch).toMatchObject({ kind: "dispatch", qitemId: b, mode: "parked" });
    const witness = dispatch.kind === "dispatch" && dispatch.slotId === p.slotId ? w : p;
    // Control: before its account runs out, the witness is a parked seat that receives (so a null below is the limit).
    expect(seatPoolCanReceive(h.spec(), h.state().slots.find((x) => x.sessionName === witness.session)!, h.nowMs)).toBe("parked");

    const effects = effectsOf(h);
    const run = effects.runCommand;
    effects.runCommand = async (command, env, signal) => {
      if (command.argv[0] === "prepare") {
        await new Promise((r) => setTimeout(r, 20)); // a slow prepare: the account runs out while it runs
        h.limited = new Set([witness.session]);
      }
      return run(command, env, signal);
    };
    h.limitedReads = 0;
    await h.submit([dispatch, { kind: "launch", slotId: added.slotId }]);

    expect(h.calls).toContain("run prepare");
    expect(h.notes(DISPATCHED)).toHaveLength(1);
    expect(h.demandSlots.at(-1)!.find((x) => x.session === witness.session)!.receives).toBeNull();
    // Read at the dispatch and again after the launch: the launch's await invalidates the run's read.
    expect(h.limitedReads).toBe(2);
  });

  it("S3: a run with no lifecycle step between the dispatch and the demand command still reads the usage once", async () => {
    h.register({ ...BASE, max: 5, ...ON, demand_cmd: { argv: ["demand"] } });
    h.demandDesired = 2;
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const w = h.poolSeat("dev.impl-s2", "background");
    await h.park(w);
    h.nowMs = T0 + 61_000;
    await h.row(MODEL);
    h.nowMs = T0 + 200_000;
    const dispatch = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch")!;
    h.limitedReads = 0;
    await h.submit([dispatch]);
    expect(h.notes(DISPATCHED)).toHaveLength(1);
    expect(h.limitedReads).toBe(1);
  });
});
