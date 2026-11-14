// A seat parked on its own waits (dispatch_to_parked) that hands a dispatched row straight back to the model, without
// taking it, is out of the parked receivers for refused_cooldown_s or until its own open rows change, and the row (and
// its successor) never goes back to it. Without this memory the pool gives the same seat a new row every minute and the
// seat gives each one back (06:55-07:01Z of 14/11: one seat 4 times, another 3 times in 6 minutes).
//
// The harness is seat-pool-c1b's (the production schema, the real queue, slots, reader, planner, policy, executor and
// history; tmux, the seats' activity, the host's load and the demand command faked), with the row facts read up to
// max_open_rows, as startup.ts reads them.

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
import { archiveAgedTerminalTransitions } from "../src/domain/queue-retention.js";
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
  seatPoolRefusedCooldown,
  type ParkFacts,
  type SeatPoolIntent,
  type SeatPoolSlotView,
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
/** idle_grace_s high: no free seat drains during a case; cooldown 0. */
const BASE: Record<string, unknown> = { min: 0, cooldown_s: 0, idle_grace_s: 86400, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}" };
const DISPATCHED = "seat_pool_dispatched";
const SKIPPED = "seat_pool_dispatch_skipped";

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
      rowFacts: (sessionName, max) => this.queue.listPoolSeatRowFacts(sessionName, max),
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

/** Two parks of the seat's own, each with a live one-hour timer armed a minute ago: they stay its waits for the case. */
async function twoParks(h: Harness, seat: Seat): Promise<[string, string]> {
  const a = await h.park(seat, { gate: "external:x1", wakeAfter: 3600, claimAgoS: 120, parkAgoS: 60 });
  const b = await h.park(seat, { gate: "external:x2", wakeAfter: 3600, claimAgoS: 120, parkAgoS: 60 });
  return [a, b];
}

/** The seat hands its pending row (the one the pool just moved to it) back to the model, without taking it. */
async function giveBack(h: Harness, seat: Seat): Promise<string> {
  const row = h.queue.listOpenObligations(seat.session, 10).find((r) => r.state === "pending");
  expect(row, `${seat.session} holds a pending row to give back`).toBeDefined();
  return (await h.queue.handoff({ qitemId: row!.qitemId, fromSession: seat.session, toSession: MODEL, nudge: false })).created.qitemId;
}

const to = (h: Harness, seat: Seat) => h.notes(DISPATCHED).filter((n) => n["to"] === seat.session);

describe("seat pool: a parked seat that gives a dispatched row back untaken is not given rows again", () => {
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

  it("a seat with 2 parks that gives the row back 3 times in a row receives it once: the successor stays with the model", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    const parks = await twoParks(h, p);
    await h.row(MODEL);
    let at = 0;
    for (let round = 0; round < 3; round++) {
      at += 61;
      await h.tickAt(at);
      if (h.queue.listOpenObligations(p.session, 10).some((r) => r.state === "pending")) await giveBack(h, p);
    }
    at += 61;
    await h.tickAt(at);
    expect(to(h, p)).toHaveLength(1);
    expect((to(h, p)[0]!["parkedQitemIds"] as string[]).sort()).toEqual([...parks].sort());
    const open = h.queue.listOpenObligations(MODEL, 10);
    expect(open).toHaveLength(1);
    expect(open[0]!.state).toBe("pending");
    expect(h.queue.listOpenObligations(p.session, 10).map((r) => r.state)).toEqual(["blocked", "blocked"]);
  });

  it("with a 2nd parked seat, the row the 1st gave back goes to the 2nd, never back to the 1st", async () => {
    h.register({ ...BASE, max: 2, dispatch_to_parked: { max_open_rows: 2 } });
    const p1 = h.poolSeat("dev.impl-s1", "idle");
    const p2 = h.poolSeat("dev.impl-s2", "idle");
    await twoParks(h, p1);
    await twoParks(h, p2);
    await h.row(MODEL);
    await h.tickAt(61);
    expect(to(h, p1)).toHaveLength(1);
    const successor = await giveBack(h, p1);
    await h.tickAt(122);
    expect(to(h, p2).map((n) => n["qitemId"])).toEqual([successor]);
    expect(to(h, p1)).toHaveLength(1);
  });

  it("once refused_cooldown_s passes the seat receives a new row again, but never the row it gave back", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2, refused_cooldown_s: 300 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await twoParks(h, p);
    await h.row(MODEL);
    await h.tickAt(61);
    const successor = await giveBack(h, p);
    await h.tickAt(122);
    expect(to(h, p)).toHaveLength(1);
    h.nowMs = T0 + 400_000;
    const fresh = await h.row(MODEL);
    await h.tickAt(500);
    expect(to(h, p).map((n) => n["qitemId"]).slice(1)).toEqual([fresh]);
    expect(h.get(successor)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("when the seat's own open rows change (a park closes) it receives a new row before the cooldown ends", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    const [a] = await twoParks(h, p);
    await h.row(MODEL);
    await h.tickAt(61);
    const successor = await giveBack(h, p);
    await h.tickAt(122);
    expect(to(h, p)).toHaveLength(1);
    h.queue.update({ qitemId: a, actorSession: p.session, state: "done", closureReason: "no-follow-on", transitionNote: "done" } as never);
    h.nowMs = T0 + 130_000;
    const fresh = await h.row(MODEL);
    await h.tickAt(200);
    expect(to(h, p).map((n) => n["qitemId"]).slice(1)).toEqual([fresh]);
    expect(h.get(successor)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("a row the seat took (in progress) and then handed back is no refusal: the seat receives again", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await twoParks(h, p);
    const b = await h.row(MODEL);
    await h.tickAt(61);
    h.queue.claim({ qitemId: b, destinationSession: p.session });
    const successor = (await h.queue.handoff({ qitemId: b, fromSession: p.session, toSession: MODEL, nudge: false })).created.qitemId;
    await h.tickAt(122);
    await h.tickAt(183);
    expect(to(h, p).map((n) => n["qitemId"])).toEqual([b, successor]);
  });

  it("a row that reached the seat by another fallback (not the pool's parked move) and went back is no refusal", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await twoParks(h, p);
    const b = await h.row(MODEL);
    h.queue.routeToFallback(b, p.session, "explicit fallback while removing node x");
    const successor = await giveBack(h, p);
    await h.tickAt(61);
    expect(to(h, p).map((n) => n["qitemId"])).toEqual([successor]);
  });

  it("the row given back never returns to the seat, a day later (past the job's history) with the seat free, nor once its transitions are archived", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    const [a, b] = await twoParks(h, p);
    const first = await h.row(MODEL);
    await h.tickAt(61);
    const successor = await giveBack(h, p);
    // A day and an hour on: the parks are done, the seat is free, the dispatch has left the history's window.
    h.nowMs = T0 + 90_000_000;
    for (const park of [a, b]) {
      h.queue.update({ qitemId: park, actorSession: p.session, state: "done", closureReason: "no-follow-on", transitionNote: "done" } as never);
    }
    await h.tickAt(90_000);
    expect(to(h, p).map((n) => n["qitemId"])).not.toContain(successor);
    expect(h.get(successor)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // 31 days on, the retention sweep archives the row given back (closed, handed off): it still never returns.
    const later = 31 * 86_400;
    h.nowMs = T0 + later * 1000;
    const archived = archiveAgedTerminalTransitions(h.db, { nowIso: iso(h.nowMs), transitionsRetentionDays: 30 });
    expect(archived.archivedQitems).toBeGreaterThan(0);
    expect(h.db.prepare("SELECT COUNT(*) AS n FROM queue_transitions_archive WHERE qitem_id = ?").get(first) as { n: number }).not.toEqual({ n: 0 });
    await h.tickAt(later + 1);
    expect(to(h, p).map((n) => n["qitemId"])).not.toContain(successor);
    const fresh = await h.row(MODEL);
    await h.tickAt(later + 62);
    expect(to(h, p).map((n) => n["qitemId"]).slice(1)).toEqual([fresh]);
  });

  it("refused_cooldown_s: 0 turns the memory off: the seat is given the row each time, as before the field", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2, refused_cooldown_s: 0 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await twoParks(h, p);
    await h.row(MODEL);
    let at = 0;
    for (let round = 0; round < 3; round++) {
      at += 61;
      await h.tickAt(at);
      if (h.queue.listOpenObligations(p.session, 10).some((r) => r.state === "pending")) await giveBack(h, p);
    }
    expect(to(h, p)).toHaveLength(3);
  });

  it("the executor's recheck: a plan made before the seat gave a row back moves nothing to it after (parked_refused)", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { max_open_rows: 2 } });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await twoParks(h, p);
    await h.row(MODEL);
    await h.tickAt(61);
    h.nowMs = T0 + 70_000;
    const other = await h.row(MODEL);
    h.nowMs = T0 + 140_000;
    const intent = planSeatPool(h.spec(), h.state(), h.nowMs, 600_000).intents.find((i) => i.kind === "dispatch");
    expect(intent).toBeUndefined(); // the seat holds the dispatched row, pending: it is not parked now
    await giveBack(h, p);
    const planned: SeatPoolIntent = { kind: "dispatch", qitemId: other, from: MODEL, slotId: p.slotId, mode: "parked" };
    await h.submit([planned]);
    expect(h.notes(SKIPPED)).toEqual([{ qitemId: other, from: MODEL, to: p.session, code: "parked_refused" }]);
    expect(h.get(other)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });
});

describe("seat pool: refused_cooldown_s and the parked refusal (pure)", () => {
  const dtp = (extra: Record<string, unknown>) => parseSeatPoolSpec({ ...BASE, max: 2, dispatch_to_parked: { ...extra } });

  it("refused_cooldown_s: absent reads as 1800 (no property, so {} parses as before the field), takes 0 to 86400 and refuses anything else", () => {
    const off = dtp({}) as { spec: SeatPoolSpec };
    expect(off.spec.dispatchToParked).not.toHaveProperty("refusedCooldownSeconds");
    expect(seatPoolRefusedCooldown(off.spec)).toBe(1800);
    expect(seatPoolRefusedCooldown((parseSeatPoolSpec({ ...BASE, max: 2 }) as { spec: SeatPoolSpec }).spec)).toBe(0);
    expect(dtp({ refused_cooldown_s: 0 })).toMatchObject({ spec: { dispatchToParked: { refusedCooldownSeconds: 0 } } });
    expect(dtp({ refused_cooldown_s: 86_400 })).toMatchObject({ spec: { dispatchToParked: { refusedCooldownSeconds: 86_400 } } });
    for (const bad of [86_401, -1, 1.5, "60", null]) {
      expect(dtp({ refused_cooldown_s: bad })).toMatchObject({ problem: { field: "dispatch_to_parked.refused_cooldown_s" } });
    }
  });

  const NOW = Date.parse("2026-11-14T08:00:00.000Z");
  const park = (qitemId: string): ParkFacts => ({
    qitemId, blockedOn: "external:x", timerIntervalS: 3600, timerArmedLive: true, parkActor: "s@r",
    parkedAt: new Date(NOW - 60_000).toISOString(), takenAt: new Date(NOW - 120_000).toISOString(),
  });
  const view = (refusal?: SeatPoolSlotView["parkedRefusal"]): SeatPoolSlotView => ({
    slotId: 1, logicalId: "dev.impl-s1", sessionName: "s@r", nodeId: "n1", phase: "live", adopted: false, nodeExists: true,
    openRows: 2, heldMessages: 0, idleSince: null, cause: "idle", atPrompt: true, backgroundWork: false,
    parkedRows: [park("a"), park("b")], ...(refusal ? { parkedRefusal: refusal } : {}),
  });
  const spec = (cooldown: number) => (parseSeatPoolSpec({ ...BASE, max: 2, dispatch_to_parked: { max_open_rows: 2, refused_cooldown_s: cooldown } }) as { spec: SeatPoolSpec }).spec;
  const ago = (s: number) => new Date(NOW - s * 1000).toISOString();

  it("a refusal within the cooldown with the same open rows refuses; past the cooldown, with other rows or with the memory off it does not", () => {
    expect(seatPoolCanReceive(spec(1800), view(), NOW)).toBe("parked");
    expect(seatPoolCanReceive(spec(1800), view({ at: ago(60), openRowIds: ["b", "a"] }), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec(1800), view({ at: ago(60), openRowIds: null }), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec(1800), view({ at: ago(1801), openRowIds: ["a", "b"] }), NOW)).toBe("parked");
    expect(seatPoolCanReceive(spec(1800), view({ at: ago(60), openRowIds: ["a", "c"] }), NOW)).toBe("parked");
    expect(seatPoolCanReceive(spec(0), view({ at: ago(60), openRowIds: ["a", "b"] }), NOW)).toBe("parked");
  });
});
