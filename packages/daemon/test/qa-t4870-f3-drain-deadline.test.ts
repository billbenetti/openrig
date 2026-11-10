// Frozen eval of T-004870 (F3 of T-004860): a seat pool's drain ends at a deadline, and its rows go back to the model.
// Contract: ARQUITETURA-F3.md, section 3 (cases 1-10), plus the held-messages case of section 2.2 (today's behaviour).
//
// The harness is the daemon's own wiring with only the terminal side faked: the production schema (ALL_MIGRATIONS),
// the real queue (QueueRepository through seatPoolQueueEffects), the real pool rows (SeatPoolSlots), the real state
// reader (readSeatPoolState), the real planner and policy (makeSeatPoolPolicy), the real executor (SeatPoolExecutor), the
// real watchdog history (WatchdogHistoryLog) and the real watchdog routes (stop and show). Faked: tmux (stop, clean,
// launch), the node removal (refused while work is still addressed to the seat, as removeNode is without a fallback),
// the pool's commands (exit codes chosen per test) and the clock. A "restart" drops the executor and the policy and
// builds new ones on the same database, as a daemon restart does.
//
// The golden case reads the real capture copied next to this file (fixtures/qa-t4870-f3/, from eval/f3.qa/real/): the
// build-dev pool spec that the operator registers (drain off, then the drain spec), and the real columns of
// seat_pool_slots and watchdog_history (depois-troca snapshot, the production daemon at 0bde2cffb).
//
// The wall clock follows the harness clock (vi.setSystemTime on every move of `nowMs`), so the queue's own timestamps
// (ts_created, which the pool reads as a row's waiting time) are the test's, whatever the date the eval runs on.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { readFileSync } from "node:fs";
import type Database from "better-sqlite3";
import { Hono } from "hono";
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
import { updateWatchdogJob } from "../src/domain/watchdog-job-update.js";
import { parseWatchdogSpec, watchdogJobTarget } from "../src/domain/watchdog-policy-engine.js";
import { watchdogRoutes } from "../src/routes/watchdog.js";
import {
  readSeatPoolState,
  SeatPoolExecutor,
  seatPoolQueueEffects,
  SeatPoolSlots,
  seatSessionName,
  type SeatPoolEffects,
} from "../src/domain/seat-pool-executor.js";
import { makeSeatPoolPolicy, parseSeatPoolSpec, type SeatPoolSpec } from "../src/domain/policies/seat-pool.js";
import type { Policy, PolicyJob } from "../src/domain/policies/types.js";

// ---------------------------------------------------------------------------------------------------------------
// The real capture (T-003440): read, never written.
// ---------------------------------------------------------------------------------------------------------------
const fixture = (name: string): unknown =>
  JSON.parse(readFileSync(new URL(`./fixtures/qa-t4870-f3/${name}`, import.meta.url), "utf8"));
type RealSpec = { policy: string; context: Record<string, unknown> };
const REAL_SPEC = fixture("spec-seat-pool-build-dev.json") as RealSpec;
const REAL_DRAIN_SPEC = fixture("spec-seat-pool-build-dev-drain.json") as RealSpec;
type RealColumn = { name: string };
const REAL_SHAPE = fixture("forma-depois-troca.json") as {
  tables: Record<"seat_pool_slots" | "watchdog_history" | "watchdog_jobs", { columns: RealColumn[] }>;
  example_history_row_shape: { evaluated_at: string; evaluation_notes: { type: string } };
};

// ---------------------------------------------------------------------------------------------------------------
// Names fixed by the contract (NOTAS.md, "API mínima").
// ---------------------------------------------------------------------------------------------------------------
const DRAIN_STARTED = "seat_pool_drain_started";
const DRAIN_ENDED = "seat_pool_drain_ended";
const DRAIN_EXPIRED = "seat_pool_drain_expired";
const SLOT_LEFT = "seat_pool_left";
const DEFAULT_DEADLINE_S = 900;

const RIG = "r";
const MODEL = "dev-impl@r";
const MODEL_LOGICAL = "dev.impl";
const LEAD = "lead@r";
const T0 = Date.parse("2026-11-10T02:00:00.000Z");
const IDLE_SINCE = new Date(T0 - 3_600_000).toISOString();
const iso = (ms: number) => new Date(ms).toISOString();
/** The quiet skip reasons the engine never writes to the history (watchdog-policy-engine.ts QUIET_SKIP_REASONS). */
const QUIET = new Set(["seat_pool_steady", "seat_pool_busy", "not_due"]);

const BASE_CONTEXT: Record<string, unknown> = {
  min: 0, max: 3, cooldown_s: 0, idle_grace_s: 300, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}",
};

type Activity = "idle" | "working";
interface Seat { logicalId: string; session: string; nodeId: string }
interface PoolSeat extends Seat { slotId: number }

const escape = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
/** A seat may be named by its logical id or its session name; both are accepted wherever the contract says `<slot>`. */
const refsOf = (seat: Seat) => [seat.logicalId, seat.session];
const drainNote = (seat: Seat) => new RegExp(`^fallback-routed: .* \\(drain:(${escape(seat.session)}|${escape(seat.logicalId)})\\)$`);

class Harness {
  readonly db: Database.Database;
  readonly rigRepo: RigRepository;
  readonly queue: QueueRepository;
  readonly outbox: OutboxHandler;
  readonly jobs: WatchdogJobsRepository;
  readonly history: WatchdogHistoryLog;
  readonly slots: SeatPoolSlots;
  readonly bus: EventBus;
  readonly app: Hono;
  readonly rigId: string;
  readonly podId: string;
  readonly model: Seat;
  private clockMs = T0;
  /** The test's clock: the executor's, the policy's and the history's, and the wall clock the queue stamps rows with. */
  get nowMs(): number { return this.clockMs; }
  set nowMs(ms: number) { this.clockMs = ms; vi.setSystemTime(ms); }
  jobId = "";
  calls: string[] = [];
  readonly activity = new Map<string, Activity>();
  /** Returns true when the command must exit 1. */
  refuse: (argv: string[], env: Record<string, string>) => boolean = () => false;
  /** Session names whose stop fails. */
  readonly stopFails = new Set<string>();
  /** The session id each stop was given as `expected`, by session name. */
  readonly stopExpected = new Map<string, string | null>();
  /** Runs inside a pool command, before it exits (a release that relaunches the seat, for one). */
  duringCommand: ((argv: string[], env: Record<string, string>) => void) | null = null;
  /** Runs once, right after the first row the pool moves has moved. */
  afterFirstMove: (() => void) | null = null;
  executor!: SeatPoolExecutor;
  policy!: Policy;

  constructor() {
    this.nowMs = T0;
    this.db = createDb();
    migrate(this.db, ALL_MIGRATIONS);
    this.bus = new EventBus(this.db);
    this.rigRepo = new RigRepository(this.db);
    const rig = this.rigRepo.createRig(RIG);
    this.rigId = rig.id;
    this.podId = new PodRepository(this.db).createPod(rig.id, "dev", "Dev").id;
    this.queue = new QueueRepository(this.db, this.bus);
    this.outbox = new OutboxHandler(this.db);
    this.queue.attachOutbox(this.outbox);
    this.jobs = new WatchdogJobsRepository(this.db);
    this.history = new WatchdogHistoryLog(this.db);
    this.slots = new SeatPoolSlots(this.db);
    this.model = this.addSeat(MODEL_LOGICAL, "working");
    this.app = new Hono();
    this.app.use("*", async (c, next) => {
      c.set("eventBus" as never, this.bus as never);
      c.set("watchdogJobsRepo" as never, this.jobs as never);
      c.set("watchdogHistoryLog" as never, this.history as never);
      c.set("queueRepo" as never, this.queue as never);
      c.set("rigRepo" as never, this.rigRepo as never);
      c.set("outboxHandler" as never, this.outbox as never);
      await next();
    });
    this.app.route("/api/watchdog", watchdogRoutes());
    this.boot();
  }

  // ---- the rig ---------------------------------------------------------------------------------------------------
  addSeat(logicalId: string, activity: Activity = "idle"): Seat {
    const node = this.rigRepo.addNode(this.rigId, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: this.podId });
    const session = seatSessionName(logicalId, RIG);
    this.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${logicalId}`, node.id, session);
    this.activity.set(node.id, activity);
    return { logicalId, session, nodeId: node.id };
  }

  /** A seat the pool added (it has an Operation-Id), live and admitted. */
  poolSeat(logicalId: string, activity: Activity = "idle"): PoolSeat {
    const seat = this.addSeat(logicalId, activity);
    const slot = this.slots.insert({
      poolJobId: this.jobId, logicalId, nodeId: seat.nodeId, phase: "live", intent: null, opId: `op-${logicalId}`, at: iso(T0 - 7_200_000),
    });
    return { ...seat, slotId: slot.slotId };
  }

  /** A seat that existed before the pool, linked as adopted (no Operation-Id). */
  adopt(seat: Seat): number {
    return this.slots.adopt({ poolJobId: this.jobId, logicalId: seat.logicalId, nodeId: seat.nodeId, at: iso(this.nowMs) }).slotId;
  }

  latestSession(nodeId: string): { id: string; status: string } | undefined {
    return this.db.prepare("SELECT id, status FROM sessions WHERE node_id = ? ORDER BY rowid DESC LIMIT 1").get(nodeId) as
      | { id: string; status: string } | undefined;
  }

  nodeExists(nodeId: string): boolean {
    return !!this.db.prepare("SELECT 1 FROM nodes WHERE id = ?").get(nodeId);
  }

  sessionRunning(nodeId: string): boolean {
    return this.latestSession(nodeId)?.status === "running";
  }

  // ---- the queue -------------------------------------------------------------------------------------------------
  async row(destination: string): Promise<string> {
    return (await this.queue.create({ sourceSession: LEAD, destinationSession: destination, body: "work", nudge: false } as never)).qitemId;
  }

  claim(qitemId: string, seat: Seat): void {
    this.queue.update({ qitemId, actorSession: seat.session, state: "in-progress" } as never);
  }

  block(qitemId: string, seat: Seat, on: string): void {
    this.queue.update({ qitemId, actorSession: seat.session, state: "blocked", blockedOn: on } as never);
  }

  get(qitemId: string): { destinationSession: string; state: string } {
    return this.queue.getById(qitemId)! as unknown as { destinationSession: string; state: string };
  }

  fallbackNotes(qitemId: string): string[] {
    return this.queue.listTransitions(qitemId).map((t) => t.transitionNote ?? "").filter((note) => note.startsWith("fallback-routed:"));
  }

  fallbackWakes(qitemId: string): string[] {
    return (this.db.prepare("SELECT destination_session FROM outbox_entries WHERE audit_pointer = ? AND outbox_id LIKE 'wake-intent-fallback-%'")
      .all(qitemId) as Array<{ destination_session: string }>).map((r) => r.destination_session);
  }

  reapCount(qitemId: string): number {
    return seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "eval" }) }).reapCount(qitemId);
  }

  // ---- the job ---------------------------------------------------------------------------------------------------
  specYaml(context: Record<string, unknown>): string {
    return JSON.stringify({ policy: "seat-pool", target: { session: MODEL }, context });
  }

  register(context: Record<string, unknown>): void {
    this.jobId = this.jobs.register({
      policy: "seat-pool", specYaml: this.specYaml(context), targetSession: MODEL, intervalSeconds: 30, registeredBySession: LEAD,
    } as never).jobId;
  }

  /** As `rig watchdog update <job> --spec`: the same check and the same writer. */
  setContext(context: Record<string, unknown>): void {
    updateWatchdogJob(this.jobs, this.jobId, { specYaml: this.specYaml(context) }, () => new Date(this.nowMs));
  }

  context(): Record<string, unknown> {
    return parseWatchdogSpec(this.jobs.getByIdOrThrow(this.jobId).specYaml).context;
  }

  setDrain(drain: boolean, extra: Record<string, unknown> = {}): void {
    this.setContext({ ...this.context(), ...extra, drain });
  }

  // ---- the daemon ------------------------------------------------------------------------------------------------
  effects(): SeatPoolEffects {
    const queueEffects = seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "eval" }) });
    const logicalOf = (nodeId: string) =>
      (this.db.prepare("SELECT logical_id FROM nodes WHERE id = ?").get(nodeId) as { logical_id: string } | undefined)?.logical_id ?? null;
    let inLease: string | null = null;
    return {
      ...queueEffects,
      moveRow: (input) => {
        this.calls.push(`move ${input.qitemId} ${input.from} -> ${input.to}${inLease ? ` [lease ${inLease}]` : ""}`);
        const moved = queueEffects.moveRow(input);
        const hook = this.afterFirstMove;
        if (hook) { this.afterFirstMove = null; hook(); }
        return moved;
      },
      unclaimRow: (input) => {
        this.calls.push(`unclaim ${input.qitemId}`);
        return queueEffects.unclaimRow(input);
      },
      addSlot: async ({ memberId }) => {
        this.calls.push(`add ${memberId}`);
        return { ok: false, code: "eval_no_add", error: "the eval adds no seat" };
      },
      operationNodeId: () => null,
      launchSlot: async ({ nodeId }) => {
        this.calls.push(`launch ${logicalOf(nodeId)}`);
        return { ok: false, code: "eval_no_launch", error: "the eval launches no seat" };
      },
      sessionRunning: (nodeId) => this.sessionRunning(nodeId),
      lifecycle: async (nodeId, fn) => {
        const logical = logicalOf(nodeId) ?? nodeId;
        this.calls.push(`lease ${logical}`);
        const outer = inLease;
        inLease = logical;
        try { return await fn(); } finally { inLease = outer; this.calls.push(`released ${logical}`); }
      },
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: this.latestSession(nodeId)?.id ?? null }),
      stopSlot: async ({ sessionName, expected }) => {
        this.calls.push(`stop ${sessionName}${inLease ? ` [lease ${inLease}]` : ""}`);
        this.stopExpected.set(sessionName, expected.sessionId);
        if (this.stopFails.has(sessionName)) return { ok: false, code: "tmux_probe_failed", error: "the eval refuses this stop" };
        this.db.prepare("UPDATE sessions SET status = 'exited' WHERE node_id = ?").run(expected.nodeId);
        return { ok: true };
      },
      removeSlot: async ({ nodeId }) => {
        const logical = logicalOf(nodeId);
        this.calls.push(`remove ${logical ?? nodeId}`);
        if (!logical) return { ok: false, code: "node_not_found", error: "the node is gone" };
        // As rigLifecycleService.removeNode without a fallback destination: work addressed to the seat keeps the node.
        if (this.queue.listOpenObligations(seatSessionName(logical, RIG), 10).length > 0) {
          return { ok: false, code: "active_qitems", error: `${logical} still has open rows` };
        }
        this.db.prepare("DELETE FROM sessions WHERE node_id = ?").run(nodeId);
        this.rigRepo.deleteNode(nodeId);
        return { ok: true };
      },
      runCommand: async (command, env, signal) => {
        this.calls.push(`run ${command.argv.join(" ")}${env["OPENRIG_SEAT_POOL_SLOT"] ? ` @${env["OPENRIG_SEAT_POOL_SLOT"]}` : ""}`);
        if (signal.aborted) return { exitCode: null, stdout: "", error: "aborted" };
        this.duringCommand?.(command.argv, env);
        return { exitCode: this.refuse(command.argv, env) ? 1 : 0, stdout: "{\"desired\": 0}" };
      },
      cleanSlot: async ({ sessionName }) => {
        this.calls.push(`clean ${sessionName}`);
        return { ok: true };
      },
      relaunchSeat: async ({ logicalId }) => {
        this.calls.push(`relaunch ${logicalId}`);
        return { ok: false, code: "eval_no_relaunch", error: "the eval relaunches no seat" };
      },
    };
  }

  readJob(jobId: string): { active: boolean; targetSession: string; spec: SeatPoolSpec | null } | null {
    const job = this.jobs.getById(jobId);
    if (!job) return null;
    try {
      const parsed = parseWatchdogSpec(job.specYaml);
      const pool = parseSeatPoolSpec(parsed.context);
      return { active: job.state === "active", targetSession: watchdogJobTarget(job, parsed.target).session, spec: "spec" in pool ? pool.spec : null };
    } catch {
      return { active: job.state === "active", targetSession: job.targetSession, spec: null };
    }
  }

  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }) {
    return readSeatPoolState({
      db: this.db,
      slots: this.slots,
      openRows: (sessionName) => this.queue.listOpenObligations(sessionName, 500),
      sessionAddresses: (sessionName) => [sessionName],
      heldMessages: (nodeId) => this.outbox.heldCount(nodeId),
      seatStopped: (nodeId) => !["running", "idle", "unknown"].includes(this.latestSession(nodeId)?.status ?? ""),
      seatState: (nodeId) => {
        if (!this.sessionRunning(nodeId)) return null;
        const idle = this.activity.get(nodeId) === "idle";
        return { activity: idle ? "idle-at-prompt" : "running", needsInput: { count: 0 }, backgroundWork: null, changedAt: IDLE_SINCE } as never;
      },
      restoring: () => false,
      demand: (jobId) => this.executor.demandSample(jobId),
    }, job);
  }

  /** A daemon start: a new executor and a new policy over the same database. */
  boot(): void {
    const executor: SeatPoolExecutor = new SeatPoolExecutor({
      slots: this.slots,
      readJob: (jobId) => this.readJob(jobId),
      readState: (job) => this.readState(job),
      effects: this.effects(),
      record: (jobId, entry) => {
        this.history.record({
          jobId, evaluatedAt: iso(this.nowMs), outcome: entry.failed ? "failed" : "skipped", skipReason: entry.reason, evaluationNotes: entry.notes,
        });
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

  /** A daemon restart: the running executor stops, and a new daemon boots on the same database. */
  async restart(): Promise<void> {
    await this.executor.stop();
    this.boot();
  }

  /** One scheduler pass for the job, as the watchdog engine runs it: evaluate, record a loud skip, wait for the run. */
  async tick(): Promise<void> {
    const job = this.jobs.getByIdOrThrow(this.jobId);
    if (job.state !== "active") return;
    const parsed = parseWatchdogSpec(job.specYaml);
    const policyJob = {
      jobId: job.jobId, policy: job.policy, target: watchdogJobTarget(job, parsed.target), message: parsed.message ?? undefined,
      intervalSeconds: job.intervalSeconds, activeWakeIntervalSeconds: job.activeWakeIntervalSeconds, scanIntervalSeconds: job.scanIntervalSeconds,
      context: parsed.context, lastEvaluationAt: job.lastEvaluationAt, lastFireAt: job.lastFireAt, registeredBySession: job.registeredBySession,
      registeredAt: job.registeredAt, watchedFilePath: null, thresholdBytes: null, thresholdTokens: null, thresholdPercent: null,
    } as unknown as PolicyJob;
    const outcome = await this.policy.evaluate(policyJob);
    if (outcome.action === "skip" && !QUIET.has(outcome.reason)) {
      this.history.record({
        jobId: job.jobId, evaluatedAt: iso(this.nowMs), outcome: "skipped", skipReason: outcome.reason, evaluationNotes: outcome.notes ?? null,
      });
    }
    await this.executor.settled(job.jobId);
  }

  async tickAt(offsetSeconds: number): Promise<void> {
    this.nowMs = T0 + offsetSeconds * 1000;
    await this.tick();
  }

  // ---- what the operator sees ------------------------------------------------------------------------------------
  events(reason: string): WatchdogHistoryEntry[] {
    return this.history.listForJob(this.jobId, 100_000).filter((e) => e.skipReason === reason).reverse();
  }

  async stop(): Promise<{ status: number; body: Record<string, unknown> }> {
    const res = await this.app.request(`/api/watchdog/${this.jobId}/stop`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ reason: "eval: drain ended" }),
    });
    return { status: res.status, body: await res.json() as Record<string, unknown> };
  }

  async show(): Promise<Record<string, unknown>> {
    const res = await this.app.request(`/api/watchdog/${this.jobId}`);
    expect(res.status).toBe(200);
    return await res.json() as Record<string, unknown>;
  }

  slotOpen(slotId: number): boolean {
    return this.slots.get(slotId)!.retiredAt === null;
  }

  poolActions(seat: Seat): string[] {
    return this.calls.filter((c) => c === `stop ${seat.session}` || c.startsWith(`stop ${seat.session} `) || c === `remove ${seat.logicalId}`
      || c.endsWith(`@${seat.session}`) || c === `clean ${seat.session}` || c === `relaunch ${seat.logicalId}`);
  }
}

/** The single expired event of the current drain, with its lists. */
function expiredOf(h: Harness): { left: Array<{ slot: string; why: string }>; moved: Array<{ qitemId: string; to: string }> } {
  const expired = h.events(DRAIN_EXPIRED);
  expect(expired).toHaveLength(1);
  const notes = expired[0]!.evaluationNotes as { left?: unknown; moved?: unknown };
  expect(Array.isArray(notes.left)).toBe(true);
  expect(Array.isArray(notes.moved)).toBe(true);
  return { left: notes.left as Array<{ slot: string; why: string }>, moved: notes.moved as Array<{ qitemId: string; to: string }> };
}

function expectLeft(left: Array<{ slot: string; why: string }>, expected: Array<{ seat: Seat; why: string }>): void {
  expect(left).toHaveLength(expected.length);
  for (const { seat, why } of expected) {
    const entry = left.find((l) => refsOf(seat).includes(l.slot));
    expect(entry, `left has ${seat.logicalId}`).toBeDefined();
    expect(entry!.why).toBe(why);
  }
}

function deadlineOf(entry: WatchdogHistoryEntry): number {
  return Date.parse(String((entry.evaluationNotes as { deadlineAt?: unknown } | null)?.deadlineAt));
}

describe("qa T-004870 F3: the seat pool's drain ends at a deadline, with its rows back at the model", () => {
  let h: Harness;

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    h = new Harness();
    h.register({ ...BASE_CONTEXT, drain: false });
  });
  afterEach(async () => {
    await h.executor.stop();
    h.db.close();
    vi.useRealTimers();
  });

  it("1 (golden, real build-dev spec): an idle seat holding a pending row stays until the deadline and leaves at it; the row reaches the model whole", async () => {
    // The real spec (capture of 2026-11-10): the operator registers it with drain off, then turns the drain spec on.
    expect(REAL_SPEC.policy).toBe("seat-pool");
    expect(REAL_DRAIN_SPEC.context["drain"]).toBe(true);
    expect({ ...REAL_DRAIN_SPEC.context, drain: false }).toEqual(REAL_SPEC.context);
    const deadlineS = (REAL_DRAIN_SPEC.context["drain_deadline_s"] as number | undefined) ?? DEFAULT_DEADLINE_S;
    const releaseScript = (REAL_DRAIN_SPEC.context["release_cmd"] as { argv: string[] }).argv.join(" ");
    const removeScript = (REAL_DRAIN_SPEC.context["remove_cmd"] as { argv: string[] }).argv.join(" ");
    h.setContext(REAL_SPEC.context);
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const row = await h.row(seat.session);

    h.setContext(REAL_DRAIN_SPEC.context);
    await h.tickAt(0);
    const started = h.events(DRAIN_STARTED);
    expect(started).toHaveLength(1);
    expect(deadlineOf(started[0]!)).toBe(T0 + deadlineS * 1000);

    // Before the deadline the drain is today's: an idle seat holding a row is not free, so nothing happens to it.
    await h.tickAt(deadlineS - 1);
    expect(h.slotOpen(seat.slotId)).toBe(true);
    expect(h.sessionRunning(seat.nodeId)).toBe(true);
    expect(h.get(row).destinationSession).toBe(seat.session);
    expect(h.poolActions(seat).filter((c) => !c.includes("demanda.py"))).toEqual([]);
    expect(h.events(DRAIN_EXPIRED)).toEqual([]);

    // At the deadline: release, stop and the row move under the seat's lease, then the node removal and remove_cmd.
    h.calls = [];
    await h.tickAt(deadlineS);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    expect(h.nodeExists(seat.nodeId)).toBe(false);
    const order = (prefix: string) => h.calls.findIndex((c) => c.startsWith(prefix));
    const iRelease = order(`run ${releaseScript} @${seat.session}`);
    const iStop = order(`stop ${seat.session}`);
    const iMove = order(`move ${row} `);
    const iRemove = order(`remove ${seat.logicalId}`);
    const iRemoveCmd = order(`run ${removeScript} @${seat.session}`);
    expect([iRelease, iStop, iMove, iRemove, iRemoveCmd].every((i) => i >= 0)).toBe(true);
    expect(iRelease).toBeLessThan(iStop);
    expect(iStop).toBeLessThan(iMove);
    expect(iMove).toBeLessThan(iRemove);
    expect(iRemove).toBeLessThan(iRemoveCmd);
    expect(h.calls[iStop]).toContain(`[lease ${seat.logicalId}]`);
    expect(h.calls[iMove]).toContain(`[lease ${seat.logicalId}]`);

    // The row: same id, at the model, pending, one drain reroute, a wake for the model, and no reap counted.
    expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    const notes = h.fallbackNotes(row);
    expect(notes).toHaveLength(1);
    expect(notes[0]).toMatch(drainNote(seat));
    expect(h.fallbackWakes(row)).toEqual([MODEL]);
    expect(h.reapCount(row)).toBe(0);
    const expired = expiredOf(h);
    expect(expired.left).toEqual([]);
    expect(expired.moved).toHaveLength(1);
    expect(expired.moved[0]).toMatchObject({ qitemId: row, to: MODEL });

    // Once per drain: a later tick writes neither event again.
    await h.tickAt(deadlineS + 30);
    expect(h.events(DRAIN_STARTED)).toHaveLength(1);
    expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);

    // The real shape: every production column is still there (no column dropped or renamed; whether the drain's start
    // may also live in a new column is the architect's open question, PERGUNTAS.md), and the drain events are ordinary
    // history rows.
    const columns = (table: string) => (h.db.prepare(`PRAGMA table_info(${table})`).all() as RealColumn[]).map((c) => c.name);
    for (const table of ["seat_pool_slots", "watchdog_history", "watchdog_jobs"] as const) {
      expect(columns(table), table).toEqual(expect.arrayContaining(REAL_SHAPE.tables[table].columns.map((c) => c.name)));
    }
    const isoMs = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/;
    expect(REAL_SHAPE.example_history_row_shape.evaluated_at).toMatch(isoMs);
    const rows = h.db.prepare(
      `SELECT evaluated_at, outcome, evaluation_notes FROM watchdog_history WHERE job_id = ? AND skip_reason IN (?, ?)`,
    ).all(h.jobId, DRAIN_STARTED, DRAIN_EXPIRED) as Array<{ evaluated_at: string; outcome: string; evaluation_notes: string }>;
    expect(rows).toHaveLength(2);
    for (const r of rows) {
      expect(r.evaluated_at).toMatch(isoMs);
      expect(["skipped", "failed"]).toContain(r.outcome);
      expect(REAL_SHAPE.example_history_row_shape.evaluation_notes.type).toBe("json");
      expect(typeof JSON.parse(r.evaluation_notes)).toBe("object");
    }
  });

  it("2: an in-progress row is released and moved, a blocked row arrives blocked, even from a working seat", async () => {
    const seat = h.poolSeat("dev.impl-s1", "working");
    const anchor = await h.row(LEAD);
    const claimed = await h.row(seat.session);
    h.claim(claimed, seat);
    const blocked = await h.row(seat.session);
    h.block(blocked, seat, anchor);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S - 1);
    expect(h.get(claimed)).toMatchObject({ destinationSession: seat.session, state: "in-progress" });
    expect(h.get(blocked)).toMatchObject({ destinationSession: seat.session, state: "blocked" });

    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.calls).toContain(`unclaim ${claimed}`);
    expect(h.calls.some((c) => c.startsWith(`stop ${seat.session}`))).toBe(true);
    expect(h.get(claimed)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.get(blocked)).toMatchObject({ destinationSession: MODEL, state: "blocked" });
    for (const id of [claimed, blocked]) {
      const notes = h.fallbackNotes(id);
      expect(notes).toHaveLength(1);
      expect(notes[0]).toMatch(drainNote(seat));
      expect(h.reapCount(id)).toBe(0);
    }
    expect(h.fallbackWakes(blocked)).toEqual([]);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    const expired = expiredOf(h);
    expect(expired.left).toEqual([]);
    expect(expired.moved.map((m) => m.qitemId).sort()).toEqual([claimed, blocked].sort());
    expect(expired.moved.every((m) => m.to === MODEL)).toBe(true);
  });

  it("3: a row that changed destination between the read and the move is not moved again and is no error", async () => {
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const first = await h.row(seat.session);
    const second = await h.row(seat.session);
    // Someone hands the other row elsewhere while the pool moves the first.
    h.afterFirstMove = () => {
      for (const id of [first, second]) {
        if (h.get(id).destinationSession === seat.session) {
          h.queue.routeToFallback(id, "other-seat@r", "handed by hand", { expectState: "pending", expectDestination: seat.session, wake: false });
        }
      }
    };

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    const movedByPool = [first, second].filter((id) => h.get(id).destinationSession === MODEL);
    const handed = [first, second].filter((id) => h.get(id).destinationSession === "other-seat@r");
    expect(movedByPool).toHaveLength(1);
    expect(handed).toHaveLength(1);
    expect(h.fallbackNotes(handed[0]!).filter((n) => /\(drain:/.test(n))).toEqual([]);
    expect(h.get(handed[0]!).state).toBe("pending");
    // No failure recorded for the pass, and the seat still left.
    const failures = h.history.listForJob(h.jobId, 100_000).filter((e) => e.outcome === "failed");
    expect(failures).toEqual([]);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    const expired = expiredOf(h);
    expect(expired.left).toEqual([]);
    expect(expired.moved.map((m) => m.qitemId)).toEqual(movedByPool);
  });

  it("4: a release command that refuses keeps the seat alive with its rows; a stop that fails moves nothing; both are left", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: { argv: ["release"] } });
    const refusing = h.poolSeat("dev.impl-s1", "idle");
    const unstoppable = h.poolSeat("dev.impl-s2", "working");
    const kept = await h.row(refusing.session);
    const stuck = await h.row(unstoppable.session);
    h.refuse = (argv, env) => argv[0] === "release" && env["OPENRIG_SEAT_POOL_SLOT"] === refusing.session;
    h.stopFails.add(unstoppable.session);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    // The refused seat: never stopped nor removed, its row where it was.
    expect(h.calls.some((c) => c.startsWith(`stop ${refusing.session}`))).toBe(false);
    expect(h.calls).not.toContain(`remove ${refusing.logicalId}`);
    expect(h.sessionRunning(refusing.nodeId)).toBe(true);
    expect(h.nodeExists(refusing.nodeId)).toBe(true);
    expect(h.get(kept)).toMatchObject({ destinationSession: refusing.session, state: "pending" });
    expect(h.fallbackNotes(kept)).toEqual([]);
    expect(h.slotOpen(refusing.slotId)).toBe(true);
    // The seat whose stop failed: still there, its row not moved.
    expect(h.calls.some((c) => c.startsWith(`stop ${unstoppable.session}`))).toBe(true);
    expect(h.calls).not.toContain(`remove ${unstoppable.logicalId}`);
    expect(h.get(stuck)).toMatchObject({ destinationSession: unstoppable.session, state: "pending" });
    expect(h.fallbackNotes(stuck)).toEqual([]);
    expect(h.slotOpen(unstoppable.slotId)).toBe(true);

    const expired = expiredOf(h);
    expectLeft(expired.left, [{ seat: refusing, why: "release_refused" }, { seat: unstoppable, why: "stop_failed" }]);
    expect(expired.moved).toEqual([]);
  });

  describe("5: stop", () => {
    it("refuses without drain and before the deadline, saying when it falls due", async () => {
      const seat = h.poolSeat("dev.impl-s1", "idle");
      await h.row(seat.session);
      const without = await h.stop();
      expect(without.status).toBe(409);
      expect(without.body["error"]).toBe("pool_has_slots");

      h.setDrain(true);
      await h.tickAt(0);
      await h.tickAt(DEFAULT_DEADLINE_S - 1);
      const early = await h.stop();
      expect(early.status).toBe(409);
      expect(early.body["error"]).toBe("pool_has_slots");
      // "The message says when the deadline falls" (section 2.3): its time of day in the message, or the deadline itself
      // as a deadlineAt in the body.
      const due = T0 + DEFAULT_DEADLINE_S * 1000;
      const deadlines = JSON.stringify(early.body).match(/"deadlineAt":"[^"]+"/g)?.map((m) => Date.parse(m.slice(14, -1))) ?? [];
      expect(String(early.body["message"]).includes(iso(due).slice(11, 16)) || deadlines.includes(due),
        `409 says when the deadline falls: ${JSON.stringify(early.body)}`).toBe(true);
      expect(h.jobs.getByIdOrThrow(h.jobId).state).toBe("active");
      expect(h.slotOpen(seat.slotId)).toBe(true);
    });

    it("refuses with drain_deadline_s: 0, which never forces the seats out", async () => {
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setDrain(true, { drain_deadline_s: 0 });
      await h.tickAt(0);
      await h.tickAt(30 * 86_400);
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
      expect(h.slotOpen(seat.slotId)).toBe(true);
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.get(row).destinationSession).toBe(seat.session);
      expect(h.calls.some((c) => c.startsWith(`stop ${seat.session}`))).toBe(false);
      const refused = await h.stop();
      expect(refused.status).toBe(409);
      expect(refused.body["error"]).toBe("pool_has_slots");
      expect(h.jobs.getByIdOrThrow(h.jobId).state).toBe("active");
    });

    it("accepts once the drain expired, retiring each seat left and listing it as still running", async () => {
      h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: { argv: ["release"] } });
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.refuse = (argv) => argv[0] === "release";
      h.setDrain(true);
      await h.tickAt(0);
      await h.tickAt(DEFAULT_DEADLINE_S);
      expectLeft(expiredOf(h).left, [{ seat, why: "release_refused" }]);

      const stopped = await h.stop();
      expect(stopped.status).toBe(200);
      expect(stopped.body["state"]).toBe("stopped");
      const leftRunning = stopped.body["leftRunning"] as Array<{ slot: string; sessionName: string; why: string }>;
      expect(leftRunning).toHaveLength(1);
      expect(refsOf(seat)).toContain(leftRunning[0]!.slot);
      expect(leftRunning[0]).toMatchObject({ sessionName: seat.session, why: "release_refused" });
      // Retired in the stop, marked as left; its session and its row are the operator's now, untouched.
      expect(h.slotOpen(seat.slotId)).toBe(false);
      expect(h.slots.listOpen(h.jobId)).toEqual([]);
      const left = h.events(SLOT_LEFT);
      expect(left).toHaveLength(1);
      expect(refsOf(seat)).toContain((left[0]!.evaluationNotes as { slot: string }).slot);
      expect(left[0]!.evaluationNotes).toMatchObject({ why: "release_refused" });
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.get(row)).toMatchObject({ destinationSession: seat.session, state: "pending" });
    });

    it("refuses when a seat appeared after the drain expired", async () => {
      const seat = h.poolSeat("dev.impl-s1", "idle");
      await h.row(seat.session);
      h.setDrain(true);
      await h.tickAt(0);
      await h.tickAt(DEFAULT_DEADLINE_S);
      expect(expiredOf(h).left).toEqual([]);
      expect(h.slots.listOpen(h.jobId)).toEqual([]);
      const late = h.poolSeat("dev.impl-s2", "idle");
      const refused = await h.stop();
      expect(refused.status).toBe(409);
      expect(refused.body["error"]).toBe("pool_has_slots");
      expect(h.slotOpen(late.slotId)).toBe(true);
    });

    it("accepts a pool with no seats, as today", async () => {
      const stopped = await h.stop();
      expect(stopped.status).toBe(200);
      expect(stopped.body["state"]).toBe("stopped");
      expect(stopped.body["leftRunning"] ?? []).toEqual([]);
    });
  });

  it("6: an adopted seat with an open row is never drained, stopped, removed or moved, and does not hold the stop", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: { argv: ["release"] } });
    const fixed = h.addSeat("dev.impl2", "idle");
    h.adopt(fixed);
    const fixedRow = await h.row(fixed.session);
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const row = await h.row(seat.session);

    h.setDrain(true);
    await h.tickAt(0);
    // Today, under drain, the adopted seat is only given back.
    expect(h.events("seat_pool_unlinked")).toHaveLength(1);
    // A link that is still there when the deadline falls (a race): the forced pass leaves it alone all the same.
    const relinked = h.adopt(fixed);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.poolActions(fixed)).toEqual([]);
    expect(h.sessionRunning(fixed.nodeId)).toBe(true);
    expect(h.nodeExists(fixed.nodeId)).toBe(true);
    expect(h.get(fixedRow)).toMatchObject({ destinationSession: fixed.session, state: "pending" });
    expect(h.fallbackNotes(fixedRow)).toEqual([]);
    const expired = expiredOf(h);
    expect(expired.left.filter((l) => refsOf(fixed).includes(l.slot))).toEqual([]);
    expect(expired.moved.map((m) => m.qitemId)).toEqual([row]);

    await h.tickAt(DEFAULT_DEADLINE_S + 30);
    expect(h.slotOpen(relinked)).toBe(false);
    const stopped = await h.stop();
    expect(stopped.status).toBe(200);
    expect(((stopped.body["leftRunning"] ?? []) as Array<{ sessionName: string }>).map((l) => l.sessionName)).not.toContain(fixed.session);
    expect(h.sessionRunning(fixed.nodeId)).toBe(true);
    expect(h.get(fixedRow).destinationSession).toBe(fixed.session);
  });

  it("7: a daemon restart in the middle of the drain does not reset the deadline", async () => {
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const row = await h.row(seat.session);
    h.setDrain(true);
    await h.tickAt(0);

    h.nowMs = T0 + 600_000;
    await h.restart();
    await h.tick();
    expect(h.slotOpen(seat.slotId)).toBe(true);

    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    const started = h.events(DRAIN_STARTED);
    expect(started).toHaveLength(1);
    expect(deadlineOf(started[0]!)).toBe(T0 + DEFAULT_DEADLINE_S * 1000);
    expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
  });

  describe("8: drain off", () => {
    it("before the deadline cancels it: the seat stays admitted, and a new drain counts from zero", async () => {
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setDrain(true);
      await h.tickAt(0);

      h.nowMs = T0 + 300_000;
      h.setDrain(false);
      await h.tick();
      expect(h.events(DRAIN_ENDED)).toHaveLength(1);

      // Past the first deadline, with drain off: nothing is forced.
      await h.tickAt(DEFAULT_DEADLINE_S + 1);
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
      expect(h.slots.get(seat.slotId)).toMatchObject({ phase: "live", retiredAt: null });
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.get(row).destinationSession).toBe(seat.session);
      expect(h.calls.some((c) => c.startsWith(`stop ${seat.session}`))).toBe(false);

      // A new drain at T0+1000 s has its own deadline at T0+1900 s.
      h.nowMs = T0 + 1_000_000;
      h.setDrain(true);
      await h.tick();
      const started = h.events(DRAIN_STARTED);
      expect(started).toHaveLength(2);
      expect(deadlineOf(started[1]!)).toBe(T0 + (1000 + DEFAULT_DEADLINE_S) * 1000);
      await h.tickAt(1000 + DEFAULT_DEADLINE_S - 1);
      expect(h.slotOpen(seat.slotId)).toBe(true);
      await h.tickAt(1000 + DEFAULT_DEADLINE_S);
      expect(h.slotOpen(seat.slotId)).toBe(false);
      expect(h.get(row).destinationSession).toBe(MODEL);
      expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
    });

    it("a pool never drained forces nothing out, however long a seat holds a row", async () => {
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      for (const offset of [0, DEFAULT_DEADLINE_S, 2 * 3600, 30 * 86_400]) await h.tickAt(offset);
      expect(h.events(DRAIN_STARTED)).toEqual([]);
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
      expect(h.slotOpen(seat.slotId)).toBe(true);
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.get(row).destinationSession).toBe(seat.session);
      expect(h.calls.some((c) => c.startsWith(`stop ${seat.session}`) || c === `remove ${seat.logicalId}`)).toBe(false);
    });
  });

  it("9: show --json lists the pool's seats from its table: a removed node whose remove_cmd refused still shows", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, remove_cmd: { argv: ["remove"] } });
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const row = await h.row(seat.session);
    h.refuse = (argv) => argv[0] === "remove";
    h.setDrain(true);
    await h.tickAt(0);

    const before = (await h.show())["seatPool"] as {
      drain: { startedAt: string; deadlineAt: string; expired: boolean };
      slots: Array<{ logicalId: string; phase: string; adopted: boolean; nodeExists: boolean; openRows: number }>;
    };
    expect(Date.parse(before.drain.startedAt)).toBe(T0);
    expect(Date.parse(before.drain.deadlineAt)).toBe(T0 + DEFAULT_DEADLINE_S * 1000);
    expect(before.drain.expired).toBe(false);
    expect(before.slots).toHaveLength(1);
    expect(before.slots[0]).toMatchObject({ logicalId: seat.logicalId, phase: "live", adopted: false, nodeExists: true, openRows: 1 });

    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.nodeExists(seat.nodeId)).toBe(false);
    expect(h.slotOpen(seat.slotId)).toBe(true);
    expect(h.get(row).destinationSession).toBe(MODEL);
    const expired = expiredOf(h);
    expectLeft(expired.left, [{ seat, why: "remove_cmd_failed" }]);
    const after = (await h.show())["seatPool"] as typeof before;
    expect(after.drain.expired).toBe(true);
    expect(after.slots).toHaveLength(1);
    expect(after.slots[0]).toMatchObject({ logicalId: seat.logicalId, adopted: false, nodeExists: false, openRows: 0 });

    const stopped = await h.stop();
    expect(stopped.status).toBe(200);
    expect(stopped.body["leftRunning"]).toEqual([expect.objectContaining({ sessionName: seat.session, why: "remove_cmd_failed" })]);
    expect(((await h.show())["seatPool"] as typeof before | undefined)?.slots ?? []).toEqual([]);
  });

  describe("10: the spec", () => {
    const parse = (extra: Record<string, unknown>) => parseSeatPoolSpec({ ...BASE_CONTEXT, ...extra });

    it("refuses a negative or non-whole drain_deadline_s, on parse and on update", () => {
      for (const bad of [-1, 1.5, "900", true]) {
        const parsed = parse({ drain_deadline_s: bad });
        expect("problem" in parsed, `drain_deadline_s ${JSON.stringify(bad)}`).toBe(true);
        if ("problem" in parsed) expect(parsed.problem.field).toBe("drain_deadline_s");
      }
      for (const good of [0, 1, 60, 900, 86_400]) expect("spec" in parse({ drain_deadline_s: good })).toBe(true);
      expect("spec" in parse({})).toBe(true);
      expect(() => h.setContext({ ...BASE_CONTEXT, drain: true, drain_deadline_s: -1 })).toThrow(/drain_deadline_s/);
      expect(h.context()["drain_deadline_s"]).toBeUndefined();
      expect("spec" in parseSeatPoolSpec(REAL_DRAIN_SPEC.context)).toBe(true);
    });

    it("absent means 900 s; a value given is the deadline", async () => {
      const seat = h.poolSeat("dev.impl-s1", "idle");
      await h.row(seat.session);
      h.setDrain(true, { drain_deadline_s: 60 });
      await h.tickAt(0);
      expect(deadlineOf(h.events(DRAIN_STARTED)[0]!)).toBe(T0 + 60_000);
      await h.tickAt(59);
      expect(h.slotOpen(seat.slotId)).toBe(true);
      await h.tickAt(60);
      expect(h.slotOpen(seat.slotId)).toBe(false);

      const other = new Harness();
      try {
        other.register({ ...BASE_CONTEXT, drain: false });
        const s = other.poolSeat("dev.impl-s1", "idle");
        await other.row(s.session);
        other.setDrain(true);
        await other.tickAt(0);
        expect(deadlineOf(other.events(DRAIN_STARTED)[0]!)).toBe(T0 + DEFAULT_DEADLINE_S * 1000);
        await other.tickAt(DEFAULT_DEADLINE_S - 1);
        expect(other.slotOpen(s.slotId)).toBe(true);
        await other.tickAt(DEFAULT_DEADLINE_S);
        expect(other.slotOpen(s.slotId)).toBe(false);
      } finally {
        await other.executor.stop();
        other.db.close();
      }
    });
  });

  it("4b: the forced pass runs once per drain: a seat left at the deadline is not released again on the next ticks", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: { argv: ["release"] } });
    const refusing = h.poolSeat("dev.impl-s1", "idle");
    const kept = await h.row(refusing.session);
    h.refuse = (argv) => argv[0] === "release";

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expectLeft(expiredOf(h).left, [{ seat: refusing, why: "release_refused" }]);
    await h.tickAt(DEFAULT_DEADLINE_S + 30);
    await h.tickAt(DEFAULT_DEADLINE_S + 60);
    // Section 2.2: once per seat, and seat_pool_drain_expired once per drain.
    expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
    expect(h.calls.filter((c) => c === `run release @${refusing.session}`)).toHaveLength(1);
    expect(h.sessionRunning(refusing.nodeId)).toBe(true);
    expect(h.get(kept)).toMatchObject({ destinationSession: refusing.session, state: "pending" });
  });

  it("6b: an adopted seat still linked when the stop comes does not hold it", async () => {
    const fixed = h.addSeat("dev.impl2", "idle");
    const fixedRow = await h.row(fixed.session);
    const seat = h.poolSeat("dev.impl-s1", "idle");
    await h.row(seat.session);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(expiredOf(h).left).toEqual([]);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    // Linked right before the stop (a race with the next tick, which would give it back): the stop is accepted all the same.
    const linked = h.adopt(fixed);
    expect(h.slotOpen(linked)).toBe(true);
    const stopped = await h.stop();
    expect(stopped.status).toBe(200);
    expect(stopped.body["state"]).toBe("stopped");
    expect(((stopped.body["leftRunning"] ?? []) as Array<{ sessionName: string }>).map((l) => l.sessionName)).not.toContain(fixed.session);
    expect(h.poolActions(fixed)).toEqual([]);
    expect(h.sessionRunning(fixed.nodeId)).toBe(true);
    expect(h.nodeExists(fixed.nodeId)).toBe(true);
    expect(h.get(fixedRow)).toMatchObject({ destinationSession: fixed.session, state: "pending" });
    expect(h.fallbackNotes(fixedRow)).toEqual([]);
  });

  it("7b: a drain whose start is older than the history's 50 latest entries still falls due at its deadline, across a restart", async () => {
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const row = await h.row(seat.session);
    h.setDrain(true);
    await h.tickAt(0);
    // A busy job: more than 50 louder entries after the start (a demand_cmd failing on every tick, for one).
    for (let i = 1; i <= 60; i++) {
      h.history.record({
        jobId: h.jobId, evaluatedAt: iso(T0 + i * 10_000), outcome: "failed", skipReason: "seat_pool_demand_failed",
        evaluationNotes: { error: "eval: demand_cmd failed" },
      });
    }
    h.nowMs = T0 + 610_000;
    await h.restart();
    await h.tick();
    expect(h.events(DRAIN_STARTED)).toHaveLength(1);
    expect(h.slotOpen(seat.slotId)).toBe(true);

    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.events(DRAIN_STARTED)).toHaveLength(1);
    expect(deadlineOf(h.events(DRAIN_STARTED)[0]!)).toBe(T0 + DEFAULT_DEADLINE_S * 1000);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
  });

  it("8c: a new drain after one that expired with seats left has its own deadline and its own pass", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: { argv: ["release"] } });
    const seat = h.poolSeat("dev.impl-s1", "idle");
    await h.row(seat.session);
    h.refuse = (argv) => argv[0] === "release";

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expectLeft(expiredOf(h).left, [{ seat, why: "release_refused" }]);

    h.nowMs = T0 + 1_000_000;
    h.setDrain(false);
    await h.tick();
    expect(h.events(DRAIN_ENDED)).toHaveLength(1);
    h.nowMs = T0 + 1_100_000;
    h.setDrain(true);
    await h.tick();
    const started = h.events(DRAIN_STARTED);
    expect(started).toHaveLength(2);
    expect(deadlineOf(started[1]!)).toBe(T0 + (1100 + DEFAULT_DEADLINE_S) * 1000);

    // The expired pass belongs to the drain that ended: the new drain's stop waits for its own deadline.
    const early = await h.stop();
    expect(early.status).toBe(409);
    expect(early.body["error"]).toBe("pool_has_slots");
    expect(h.slotOpen(seat.slotId)).toBe(true);

    await h.tickAt(1100 + DEFAULT_DEADLINE_S - 1);
    expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
    await h.tickAt(1100 + DEFAULT_DEADLINE_S);
    const expired = h.events(DRAIN_EXPIRED);
    expect(expired).toHaveLength(2);
    expect(h.calls.filter((c) => c === `run release @${seat.session}`)).toHaveLength(2);
    const notes = expired[1]!.evaluationNotes as { left: Array<{ slot: string; why: string }> };
    expectLeft(notes.left, [{ seat, why: "release_refused" }]);
    const stopped = await h.stop();
    expect(stopped.status).toBe(200);
    expect(stopped.body["leftRunning"]).toEqual([expect.objectContaining({ sessionName: seat.session, why: "release_refused" })]);
  });

  it("2.2: the stop is given the expected session read at the start of the pass, before the release command ran", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: { argv: ["release"] } });
    const seat = h.poolSeat("dev.impl-s1", "idle");
    await h.row(seat.session);
    const before = h.latestSession(seat.nodeId)!.id;
    // The release command brings up a new session on the node while it runs.
    h.duringCommand = (argv, env) => {
      if (argv[0] !== "release" || env["OPENRIG_SEAT_POOL_SLOT"] !== seat.session) return;
      h.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${seat.logicalId}-2`, seat.nodeId, seat.session);
    };

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.calls).toContain(`run release @${seat.session}`);
    expect(h.stopExpected.has(seat.session)).toBe(true);
    expect(h.stopExpected.get(seat.session)).toBe(before);
  });

  it("held messages (section 2.2, today's behaviour): they hold the seat until the deadline, then stay retained, bound to the removed node", async () => {
    const seat = h.poolSeat("dev.impl-s1", "idle");
    h.db.prepare(
      `INSERT INTO outbox_entries (outbox_id, sender_session, destination_session, body, ts_dispatched, delivery_state, guard_binding)
       VALUES ('held-1', ?, ?, 'a held message', ?, 'retained', ?)`,
    ).run(LEAD, seat.session, iso(T0 - 60_000), JSON.stringify({ nodeId: seat.nodeId }));
    expect(h.outbox.heldCount(seat.nodeId)).toBe(1);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S - 1);
    // Today's drain: a seat holding a message is not free.
    expect(h.slotOpen(seat.slotId)).toBe(true);
    expect(h.sessionRunning(seat.nodeId)).toBe(true);

    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.slotOpen(seat.slotId)).toBe(false);
    expect(h.nodeExists(seat.nodeId)).toBe(false);
    const held = h.db.prepare("SELECT destination_session, delivery_state, retired_at, guard_binding FROM outbox_entries WHERE outbox_id = 'held-1'").get() as
      { destination_session: string; delivery_state: string; retired_at: string | null; guard_binding: string };
    expect(held).toMatchObject({ destination_session: seat.session, delivery_state: "retained", retired_at: null });
    expect(JSON.parse(held.guard_binding)).toMatchObject({ nodeId: seat.nodeId });
    expect(expiredOf(h).left).toEqual([]);
    expect((await h.stop()).status).toBe(200);
  });
});
