// Frozen eval of T-005497 (e-c1 of T-005481): the executor gives a parked slot its 2nd row (C3, C4). Layer E: the
// daemon's own wiring with only the terminal side faked, in the mold of the F3 harness of T-004870
// (eval/f3/packages/daemon/test/qa-t4870-f3-drain-deadline.test.ts): the production schema (ALL_MIGRATIONS), the real
// queue (QueueRepository through seatPoolQueueEffects), the real pool rows (SeatPoolSlots), the real reader
// (readSeatPoolState, with rowFacts = QueueRepository.listPoolSeatRowFacts), the real planner and policy, the real
// executor and the real watchdog history. Faked: tmux (stop, clean, launch: none happens), the seat's activity (idle at
// its prompt, working, at its prompt with a background shell, needs input), the host's load (hostSample) and the demand
// command (it records OPENRIG_SEAT_POOL_SLOTS). The wall clock follows the harness clock (vi.setSystemTime).
// Contract: ARQUITETURA.md r1 C3, C4 and section 3, FATIAS.md r1 "e-c1", eval/c1.qa/INTERFACE.md sections 3 and 5.
//
// MUTANTE → it() (the full map is at the top of qa-t5481-c1-policy.test.ts); in this file:
//   M1  → "caso 4"
//   M8  → "caso 1", "caso 6", "caso 8"
//   M10 → "negativo: a chave ausente (executor)"
//   M12 → "caso 7"
//   M16 → "caso 8 (recheck): o slot passou a draining …"

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

  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState {
    return readSeatPoolState({
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

  state(): SeatPoolState { return this.readState({ jobId: this.jobId, targetSession: MODEL, spec: this.spec() }); }

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
      record: (jobId, entry) => {
        this.history.record({ jobId, evaluatedAt: iso(this.nowMs), outcome: entry.failed ? "failed" : "skipped", skipReason: entry.reason, evaluationNotes: entry.notes });
      },
      now: () => new Date(this.nowMs),
      effectTimeoutMs: 15_000,
    });
    this.executor = executor;
    this.policy = makeSeatPoolPolicy({
      readState: async (job) => this.readState(job),
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

describe("qa T-005481 e-c1: the executor gives a parked slot its 2nd row (C3, C4)", () => {
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

  it("caso 1 (o caso base): o slot estacionado recebe 1 tarefa-nova do modelo depois de dispatch_after_s, não antes", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    const a = await h.park(p);
    const view = h.state().slots[0]!;
    expect(view).toMatchObject({ openRows: 1, atPrompt: true, backgroundWork: true, cause: "working" });
    expect(view.parked).toMatchObject({ qitemId: a, blockedOn: "external:x", timerIntervalS: 2700, timerArmedLive: true, parkActor: p.session });
    expect(seatPoolCanReceive(h.spec(), view, h.nowMs)).toBe("parked");
    const b = await h.row(MODEL);
    await h.tickAt(30);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toHaveLength(1);
    expect(h.notes(DISPATCHED)[0]).toMatchObject({ qitemId: b, from: MODEL, to: p.session, mode: "parked" });
    expect(h.get(b)).toMatchObject({ destinationSession: p.session, state: "pending" });
    expect(h.get(a)).toMatchObject({ destinationSession: p.session, state: "blocked", blockedOn: "external:x" });
  });

  it("caso 4: depois de receber B, o slot não recebe C, nem no mesmo plano nem no seguinte", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const b = await h.row(MODEL);
    h.nowMs = T0 + 1;
    const c = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.events(DISPATCHED)).toHaveLength(1);
    const at = [h.get(b).destinationSession, h.get(c).destinationSession].sort();
    expect(at).toEqual([MODEL, p.session].sort());
    // The next plans: the slot holds A (blocked) and B (pending), the ceiling of 2.
    expect(h.state().slots[0]).toMatchObject({ openRows: 2 });
    expect(h.state().slots[0]).not.toHaveProperty("parked");
    await h.tickAt(200);
    h.nowMs = T0 + 200_001;
    const d = await h.row(MODEL);
    await h.tickAt(400);
    expect(h.events(DISPATCHED)).toHaveLength(1);
    expect(h.get(d).destinationSession).toBe(MODEL);
    expect([h.get(b), h.get(c)].filter((r) => r.destinationSession === MODEL)).toHaveLength(1);
  });

  it("caso 6: seat_pool_dispatched com mode \"parked\" e parkedQitemId; a nota e a resolution dizem parked on … (2nd row); A fica intocada", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    const a = await h.park(p);
    const wakeBefore = h.queue.getParkWakeStatus(a);
    const transitionsOfA = h.queue.listTransitions(a).length;
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, mode: "parked", parkedQitemId: a }]);
    const reason = `seat pool ${h.jobId}: ${MODEL} had not taken it; ${p.session} was parked on external:x (2nd row)`;
    expect(h.queue.listTransitions(b).map((t) => t.transitionNote)).toContain(`fallback-routed: ${MODEL} → ${p.session} (${reason})`);
    expect(h.get(b).resolution).toBe(`fallback: ${reason}`);
    // A: the same row, the same wake, nothing written on it.
    expect(h.queue.getParkWakeStatus(a)).toEqual(wakeBefore);
    expect(h.queue.listTransitions(a)).toHaveLength(transitionsOfA);
    expect(h.get(a)).toMatchObject({ destinationSession: p.session, state: "blocked" });
  });

  it("caso 6 (modo free com a chave ligada): o seat_pool_dispatched traz mode \"free\" e o texto de hoje", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const f = h.poolSeat("dev.impl-s1", "idle");
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: f.session, mode: "free" }]);
    expect(h.get(b).resolution).toBe(`fallback: seat pool ${h.jobId}: ${MODEL} had not taken it; ${f.session} was free`);
  });

  it("caso 7: o OPENRIG_SEAT_POOL_SLOTS do demand_cmd traz receives \"parked\" no slot estacionado, \"free\" no livre e null nos outros", async () => {
    h.register({ ...BASE, max: 5, ...ON, demand_cmd: { argv: ["demand"] } });
    h.demandDesired = 5;
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const f = h.poolSeat("dev.impl-s2", "idle");
    const w = h.poolSeat("dev.impl-s3", "working");
    const wRow = await h.row(w.session);
    h.queue.claim({ qitemId: wRow, destinationSession: w.session });
    const g = h.poolSeat("dev.impl-s4", "background");
    await h.park(g, { gate: "gate:x" });
    const l = h.poolSeat("dev.impl-s5", "background");
    await h.park(l, { gate: "external:y", wakeAfter: 4500 });
    await h.tickAt(0);
    const sent = h.demandSlots.at(-1)!;
    expect(sent).toEqual([
      { session: p.session, phase: "live", receives: "parked" },
      { session: f.session, phase: "live", receives: "free" },
      { session: w.session, phase: "live", receives: null },
      { session: g.session, phase: "live", receives: null },
      { session: l.session, phase: "live", receives: null },
    ]);
    // The same function as the planner's, over the same state.
    const st = h.state();
    for (const entry of sent) {
      const view = st.slots.find((s) => s.sessionName === entry.session)!;
      expect(entry.receives, entry.session).toBe(seatPoolCanReceive(h.spec(), view, h.nowMs));
    }
    // After the run's dispatches: a slot that just received has 2 rows (or 1 pending) and sends null.
    const b1 = await h.row(MODEL);
    h.nowMs = T0 + 1;
    const b2 = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.get(b1).destinationSession).toBe(f.session);
    expect(h.get(b2).destinationSession).toBe(p.session);
    const after = h.demandSlots.at(-1)!;
    expect(after.find((s) => s.session === p.session)!.receives).toBeNull();
    expect(after.find((s) => s.session === f.session)!.receives).toBeNull();
  });

  it("caso 8 (recheck): o plano escolheu o \"parked\" e antes do move o slot saiu do prompt ou ganhou outra linha: a linha fica no modelo e o executor grava parked_changed; o recheck do \"free\" segue sem registro", async () => {
    h.register({ ...BASE, max: 2, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const w = h.poolSeat("dev.impl-s2", "working");
    const b = await h.row(MODEL);
    h.nowMs = T0 + 61_000;
    const planned = planSeatPool(h.spec(), h.state(), h.nowMs, 60_000).intents.filter((i) => i.kind === "dispatch");
    const intent = { kind: "dispatch", qitemId: b, from: MODEL, slotId: p.slotId, mode: "parked" } as const;
    expect(planned).toEqual([intent]);
    // (a) the slot left its prompt.
    h.setMode(p, "working");
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toHaveLength(1);
    expect(h.notes(SKIPPED)[0]).toMatchObject({ qitemId: b, from: MODEL, to: p.session, code: "parked_changed" });
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // (b) back at its prompt, but another row arrived straight to it.
    h.setMode(p, "background");
    await h.row(p.session, [], "coordenacao-coord-4@fabrica");
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toHaveLength(2);
    expect(h.notes(SKIPPED)[1]).toMatchObject({ qitemId: b, to: p.session, code: "parked_changed" });
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // (c) a "free" plan whose seat is no longer free: nothing recorded, as today.
    await h.submit([{ kind: "dispatch", qitemId: b, from: MODEL, slotId: w.slotId }]);
    expect(h.events(SKIPPED)).toHaveLength(2);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.events(FAILED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("caso 8 (recheck): o slot passou a draining entre o plano e o move: a linha fica no modelo e o executor grava parked_changed", async () => {
    h.register({ ...BASE, max: 2, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const b = await h.row(MODEL);
    h.nowMs = T0 + 61_000;
    const intent = { kind: "dispatch", qitemId: b, from: MODEL, slotId: p.slotId, mode: "parked" } as const;
    expect(planSeatPool(h.spec(), h.state(), h.nowMs, 60_000).intents.filter((i) => i.kind === "dispatch")).toEqual([intent]);
    // Between the plan and the move the seat starts draining: it is still at its prompt, its park still valid.
    h.slots.update(p.slotId, { phase: "draining" });
    const view = h.state().slots.find((s) => s.slotId === p.slotId)!;
    expect(view).toMatchObject({ phase: "draining", atPrompt: true, backgroundWork: true, openRows: 1 });
    expect(view.parked).toMatchObject({ blockedOn: "external:x", timerArmedLive: true, parkActor: p.session });
    expect(seatPoolCanReceive(h.spec(), view, h.nowMs)).toBeNull();
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toHaveLength(1);
    expect(h.notes(SKIPPED)[0]).toMatchObject({ qitemId: b, from: MODEL, to: p.session, code: "parked_changed" });
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("negativo: parked_max_load_per_cpu_percent: 80 e load em 90%: não recebe e grava parked_load; um slot \"free\" no mesmo estado recebe", async () => {
    h.register({ ...BASE, max: 2, dispatch_to_parked: { parked_max_load_per_cpu_percent: 80 } });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    h.host = { cpuCount: 4, load1: 3.6, freeMemoryBytes: 1e10 };
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.notes(SKIPPED).at(-1)).toMatchObject({ qitemId: b, from: MODEL, to: p.session, code: "parked_load" });
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // A free seat under the same load receives: the load is checked only for a parked slot.
    const skippedBefore = h.events(SKIPPED).length;
    const f = h.poolSeat("dev.impl-s2", "idle");
    await h.tickAt(120);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: f.session, mode: "free" }]);
    expect(h.events(SKIPPED)).toHaveLength(skippedBefore);
    // At 80% exactly (not above the limit) the parked slot receives.
    h.host = { cpuCount: 4, load1: 3.2, freeMemoryBytes: 1e10 };
    h.nowMs = T0 + 120_001;
    const c = await h.row(MODEL);
    await h.tickAt(200);
    expect(h.notes(DISPATCHED).at(-1)).toMatchObject({ qitemId: c, to: p.session, mode: "parked" });
  });

  it("negativo: a chave ausente (executor): o slot estacionado não recebe e o seat_pool_dispatched é o de hoje", async () => {
    // max 1: no room for an add while the row waits (the second seat comes in by hand below).
    h.register({ ...BASE, max: 1 });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p);
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.events(SKIPPED)).toHaveLength(0);
    expect(h.get(b).destinationSession).toBe(MODEL);
    const view = h.state().slots[0]!;
    expect(view).not.toHaveProperty("atPrompt");
    expect(view).not.toHaveProperty("backgroundWork");
    expect(view).not.toHaveProperty("parked");
    const f = h.poolSeat("dev.impl-s2", "idle");
    await h.tickAt(120);
    expect(h.notes(DISPATCHED)).toEqual([{ qitemId: b, from: MODEL, to: f.session }]);
  });

  it("invariante: o reap de um slot com 2 linhas move as duas: a in-progress vai ao modelo com a tag, a blocked passa pelo rerouteParked", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    const a = await h.park(p);
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.get(b).destinationSession).toBe(p.session);
    h.queue.claim({ qitemId: b, destinationSession: p.session });
    h.endSession(p);
    await h.tickAt(120);
    const reaped = h.notes("seat_pool_reaped");
    expect(reaped).toHaveLength(1);
    const moved = reaped[0]!["rows"] as Array<Record<string, unknown>>;
    expect(moved).toHaveLength(2);
    expect(moved).toContainEqual({ qitemId: b, to: MODEL });
    expect(moved).toContainEqual({ qitemId: a, to: MODEL, park: "rearmed" });
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.get(b).tags).toContain("tarefa-nova");
    expect(h.get(a)).toMatchObject({ destinationSession: MODEL, state: "blocked", blockedOn: "external:x" });
    expect(h.queue.getParkWakeStatus(a)).toMatchObject({ kind: "timer", phase: "armed", live: true });
  });

  it("invariante: o holdsWork do dreno não muda (um slot com 1 blocked e idleSince antigo não é drenado)", async () => {
    h.register({ ...BASE, max: 1, ...ON, idle_grace_s: 0 });
    const p = h.poolSeat("dev.impl-s1", "idle");
    const a = await h.park(p);
    expect(h.state().slots[0]).toMatchObject({ cause: "idle", idleSince: IDLE_SINCE, openRows: 1 });
    await h.tickAt(10);
    expect(h.events("seat_pool_drain_started")).toHaveLength(0);
    await h.submit([{ kind: "drain", slotId: p.slotId, idleSince: IDLE_SINCE }]);
    expect(h.slots.get(p.slotId)!.phase).toBe("live");
    expect(h.calls.some((c) => c.startsWith(`stop ${p.session}`))).toBe(false);
    expect(h.get(a)).toMatchObject({ destinationSession: p.session, state: "blocked" });
  });
});
