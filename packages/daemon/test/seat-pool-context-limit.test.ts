// T-012383: a seat the pool added whose context is at `context_limit_percent` or above (default 40), as the status-line
// sidecar of its current occupant reads it (used_percentage, sampled_at under 10 min: the rule the Billverso's
// orchestration gate, portao._contexto, applies to the same file), takes no row, free or parked.
//
// The case (17/11, the build-dev pool at max): the core kept handing rows to slots past 40%, which by their own rule
// take no new work, so each gave the row straight back to the model and the next plan handed it to the next full slot.
// T-011798 went s28 -> s24 -> model and T-012070 went s31 -> s24 -> model: two dispatches each, no work done, and the row
// back where it started. With the limit the plan never hands those rows to a full slot: it counts the full slot as
// occupied, so the pool grows for the rows instead, and a slot under the limit still takes them.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
import { WatchdogHistoryLog } from "../src/domain/watchdog-history-log.js";
import { parseWatchdogSpec, watchdogJobTarget } from "../src/domain/watchdog-policy-engine.js";
import { ContextUsageStore } from "../src/domain/context-usage-store.js";
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
  SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT,
  seatPoolCanReceive,
  seatPoolContextOver,
  seatPoolHeldByContext,
  type ParkFacts,
  type SeatPoolIntent,
  type SeatPoolPendingRow,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import type { Policy, PolicyJob } from "../src/domain/policies/types.js";

// ---------------------------------------------------------------------------------------------------------------
// The pure layer: the spec, the predicate and the planner
// ---------------------------------------------------------------------------------------------------------------
const NOW = Date.parse("2026-11-17T19:40:00.000Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const MODEL_SESSION = "build-dev-1@fabrica";
const TAG = "tarefa-nova";

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 6, cwd_template: "/w/{rig}/{slot}", cooldown_s: 0, idle_grace_s: 300, dispatch_tag: TAG, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

/** A slot of the pool, live and free (idle at its prompt for 10 min, no row), named as the real ones. */
function slot(n: number, overrides: Partial<SeatPoolSlotView> = {}): SeatPoolSlotView {
  return {
    slotId: n, logicalId: `build.dev-1-s${n}`, sessionName: `build-dev-1-s${n}@fabrica`, nodeId: `node-s${n}`, phase: "live",
    adopted: false, nodeExists: true, openRows: 0, heldMessages: 0, idleSince: ago(600), cause: "idle", ...overrides,
  };
}

/** The context reading of a full slot, sampled `ageS` ago. */
const full = (usedPercentage: number, ageS = 30) => ({ context: { usedPercentage, sampledAt: ago(ageS) } });

function row(qitemId: string, ageS = 120): SeatPoolPendingRow {
  return { qitemId, destination: MODEL_SESSION, waitingSince: ago(ageS), tags: [TAG, "pool-devolvida"] };
}

function state(slots: SeatPoolSlotView[], pending: SeatPoolPendingRow[]): SeatPoolState {
  return {
    model: { rigId: "rig", rigName: "fabrica", podNamespace: "build", memberId: "dev-1", logicalId: "build.dev-1", nodeId: "node-model", sessionName: MODEL_SESSION },
    modelCause: "working", restoring: false, slots, adoptable: [], pending, demand: null, lastScaleAt: null,
  };
}

const dispatches = (intents: SeatPoolIntent[]) => intents.filter((i): i is Extract<SeatPoolIntent, { kind: "dispatch" }> => i.kind === "dispatch");

describe("T-012383: context_limit_percent (spec)", () => {
  it("absent reads as 40 and adds no property; 0 to 100 are kept", () => {
    expect(spec()).not.toHaveProperty("contextLimitPercent");
    expect(SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT).toBe(40);
    for (const value of [0, 1, 40, 100]) expect(spec({ context_limit_percent: value }).contextLimitPercent).toBe(value);
  });

  it.each([[-1], [101], [40.5], ["40"], [null], [true]])("refuses %j", (value) => {
    const parsed = parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", context_limit_percent: value });
    expect(parsed).toEqual({ problem: { field: "context_limit_percent", message: expect.stringContaining("context_limit_percent must be a whole number from 0") } });
  });

  it("an unknown field names it among the known", () => {
    const parsed = parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", context_limit: 40 });
    expect("problem" in parsed && parsed.problem.message).toContain("context_limit_percent");
  });
});

describe("T-012383: the ping-pong of T-011798 and T-012070 (planner)", () => {
  // s24, s28 and s31 past 40% (the build-dev pool at 19:4xZ of 17/11), both rows back at the model with the pool's tag.
  const fullSlots = () => [slot(24, full(52)), slot(28, full(47)), slot(31, full(61))];
  const pending = () => [row("qitem-T-011798"), row("qitem-T-012070")];

  it("control, the behavior before the limit (context_limit_percent: 0): both rows go to full slots, which give them back", () => {
    const plan = planSeatPool(spec({ context_limit_percent: 0 }), state(fullSlots(), pending()), NOW, 120_000);
    expect(dispatches(plan.intents).map((i) => [i.qitemId, i.slotId])).toEqual([["qitem-T-011798", 24], ["qitem-T-012070", 28]]);
  });

  it("with the default limit no row goes to a full slot; the full slots count as occupied and the pool grows for the rows", () => {
    const plan = planSeatPool(spec(), state(fullSlots(), pending()), NOW, 120_000);
    expect(dispatches(plan.intents)).toEqual([]);
    // 3 full slots occupied + 2 rows no slot takes: 5 seats wanted, 3 there, 2 added.
    expect(plan).toMatchObject({ target: 5, current: 3, reason: "scale_up" });
    expect(plan.intents).toContainEqual({ kind: "add", count: 2 });
    // A full slot idle past idle_grace_s is not drained as free.
    expect(plan.intents.filter((i) => i.kind === "drain")).toEqual([]);
  });

  it("a slot under the limit takes the row; the full one beside it does not", () => {
    const plan = planSeatPool(spec(), state([slot(24, full(52)), slot(30, full(12))], [row("qitem-T-011798")]), NOW, 120_000);
    expect(dispatches(plan.intents)).toEqual([{ kind: "dispatch", qitemId: "qitem-T-011798", from: MODEL_SESSION, slotId: 30 }]);
  });

  it("with no row waiting, a full slot is still occupied: the pool does not drain it below what it holds", () => {
    const plan = planSeatPool(spec({ min: 0 }), state([slot(24, full(52))], []), NOW, 120_000);
    expect(plan).toMatchObject({ target: 1, current: 1 });
    expect(plan.intents).toEqual([]);
  });
});

describe("T-012383: seatPoolContextOver and seatPoolCanReceive (the reading)", () => {
  const s = spec();
  it.each([
    ["40% exactly", true, full(40)],
    ["39.9%", false, full(39.9)],
    ["sampled 599 s ago", true, full(80, 599)],
    ["sampled 600 s ago (stale)", false, full(80, 600)],
    ["sampled ahead of the clock (as just taken)", true, { context: { usedPercentage: 80, sampledAt: new Date(NOW + 5_000).toISOString() } }],
    ["an unreadable time", false, { context: { usedPercentage: 80, sampledAt: "not a time" } }],
    ["a percentage that is NaN", false, { context: { usedPercentage: Number.NaN, sampledAt: ago(10) } }],
    ["no reading (sidecar absent, unreadable or of a previous occupant)", false, {}],
    ["an adopted seat at 90% (a fixed seat the pool never replaces)", false, { ...full(90), adopted: true }],
  ])("%s: over=%s", (_name, over, overrides) => {
    const view = slot(5, overrides as Partial<SeatPoolSlotView>);
    expect(seatPoolContextOver(s, view, NOW)).toBe(over);
    expect(seatPoolCanReceive(s, view, NOW)).toBe(over ? null : "free");
  });

  it("the limit follows the spec: 50 lets a 45% slot take a row, 0 turns the rule off", () => {
    expect(seatPoolCanReceive(spec({ context_limit_percent: 50 }), slot(5, full(45)), NOW)).toBe("free");
    expect(seatPoolCanReceive(spec({ context_limit_percent: 50 }), slot(5, full(50)), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec({ context_limit_percent: 0 }), slot(5, full(99)), NOW)).toBe("free");
  });

  it("a slot parked on its own wait gets no 2nd row past the limit, and gets it under", () => {
    const parked = (pct: number) => slot(7, {
      ...full(pct), openRows: 1, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
      parked: {
        qitemId: "q-a", blockedOn: "external:ci-1", timerIntervalS: 2700, timerArmedLive: true, parkActor: "build-dev-1-s7@fabrica",
        parkedAt: ago(1800), takenAt: ago(3600),
      } satisfies ParkFacts,
    });
    const s2 = spec({ dispatch_to_parked: {} });
    expect(seatPoolCanReceive(s2, parked(10), NOW)).toBe("parked");
    expect(seatPoolCanReceive(s2, parked(45), NOW)).toBeNull();
  });

  it("seatPoolHeldByContext: only a quiet slot in usage past the limit", () => {
    expect(seatPoolHeldByContext(s, slot(5, full(45)), NOW)).toBe(true);
    expect(seatPoolHeldByContext(s, slot(5, full(30)), NOW)).toBe(false);
    expect(seatPoolHeldByContext(s, slot(5, { ...full(45), openRows: 1, idleSince: null, cause: "working" }), NOW)).toBe(false);
    expect(seatPoolHeldByContext(s, slot(5, { ...full(45), heldMessages: 1 }), NOW)).toBe(false);
    expect(seatPoolHeldByContext(s, slot(5, full(45)), NOW, { usageLimited: true })).toBe(false);
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The executor: the real queue, the reader and the history
// ---------------------------------------------------------------------------------------------------------------
const RIG = "fabrica";
const MODEL = "build-dev-1@fabrica";
const LEAD = "coordenacao-orquestrador-tech@fabrica";
const T0 = Date.parse("2026-11-17T19:40:00.000Z");
const IDLE_SINCE = new Date(T0 - 7_200_000).toISOString();
const iso = (ms: number) => new Date(ms).toISOString();
const QUIET = new Set(["seat_pool_steady", "seat_pool_busy", "not_due"]);
const BASE: Record<string, unknown> = { min: 0, cooldown_s: 0, idle_grace_s: 86400, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}", dispatch_tag: TAG };

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
  private clockMs = T0;
  get nowMs(): number { return this.clockMs; }
  set nowMs(ms: number) { this.clockMs = ms; vi.setSystemTime(ms); }
  jobId = "";
  /** Each seat's context reading, as the sidecar reader gives it (absent: null, no reading). */
  readonly context = new Map<string, { usedPercentage: number | null; sampledAt: string | null }>();
  readonly contextReads: string[] = [];
  demandSlots: Array<Array<{ session: string; receives?: string | null; held?: string }>> = [];
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
    this.podId = new PodRepository(this.db).createPod(rig.id, "build", "Build").id;
    this.queue = new QueueRepository(this.db, bus);
    this.outbox = new OutboxHandler(this.db);
    this.queue.attachOutbox(this.outbox);
    this.jobs = new WatchdogJobsRepository(this.db);
    this.history = new WatchdogHistoryLog(this.db);
    this.slots = new SeatPoolSlots(this.db);
    this.addSeat("build.dev-1");
    this.boot();
  }

  addSeat(logicalId: string): { logicalId: string; session: string; nodeId: string } {
    const node = this.rigRepo.addNode(this.rigId, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: this.podId });
    const session = seatSessionName(logicalId, RIG);
    this.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${logicalId}`, node.id, session);
    return { logicalId, session, nodeId: node.id };
  }

  /** A slot the pool added (with an Operation-Id), live and idle at its prompt; `adopted`: a fixed seat it adopted. */
  poolSeat(logicalId: string, adopted = false): { session: string; slotId: number; nodeId: string } {
    const seat = this.addSeat(logicalId);
    const slot = this.slots.insert({
      poolJobId: this.jobId, logicalId, nodeId: seat.nodeId, phase: "live", intent: null, opId: adopted ? null : `op-${logicalId}`, at: iso(T0 - 7_200_000),
    });
    return { session: seat.session, slotId: slot.slotId, nodeId: seat.nodeId };
  }

  async row(qitemId: string): Promise<string> {
    return (await this.queue.create({ qitemId, sourceSession: LEAD, destinationSession: MODEL, body: `${qitemId}: work`, tags: [TAG], nudge: false } as never)).qitemId;
  }

  get(qitemId: string): { destinationSession: string; state: string } {
    return this.queue.getById(qitemId)! as never;
  }

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

  /** The reader as startup.ts builds it: the plan's and the executor's own read the context the same way. */
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState {
    return readSeatPoolState({
      db: this.db,
      slots: this.slots,
      openRows: (sessionName) => this.queue.listOpenObligations(sessionName, 500),
      rowFacts: (sessionName) => this.queue.listPoolSeatRowFacts(sessionName),
      sessionAddresses: (sessionName) => [sessionName],
      heldMessages: (nodeId) => this.outbox.heldCount(nodeId),
      seatStopped: () => false,
      seatState: () => ({ activity: "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: null, changedAt: IDLE_SINCE }) as never,
      restoring: () => false,
      demand: (jobId) => this.executor.demandSample(jobId),
      contextUsage: (_nodeId, sessionName) => {
        this.contextReads.push(sessionName);
        return this.context.get(sessionName) ?? null;
      },
    }, job);
  }

  state(): SeatPoolState { return this.readState({ jobId: this.jobId, targetSession: MODEL, spec: this.spec() }); }

  effects(): SeatPoolEffects {
    const queueEffects = seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "eval" }) });
    return {
      ...queueEffects,
      addSlot: async () => ({ ok: false, code: "eval_no_add", error: "the test adds no seat" }),
      operationNodeId: () => null,
      launchSlot: async () => ({ ok: false, code: "eval_no_launch", error: "the test launches no seat" }),
      sessionRunning: () => true,
      lifecycle: async (_nodeId, fn) => fn(),
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: "x" }) as never,
      stopSlot: async () => ({ ok: false, code: "eval_no_stop", error: "the test stops no seat" }),
      removeSlot: async () => ({ ok: false, code: "eval_no_remove", error: "the test removes no seat" }),
      runCommand: async (_command, env) => {
        if (env["OPENRIG_SEAT_POOL_SLOTS"] !== undefined) this.demandSlots.push(JSON.parse(env["OPENRIG_SEAT_POOL_SLOTS"]));
        return { exitCode: 0, stdout: JSON.stringify({ desired: 0 }) };
      },
      cleanSlot: async () => ({ ok: true }),
      relaunchSeat: async () => ({ ok: false, code: "eval_no_relaunch", error: "the test relaunches no seat" }),
    };
  }

  boot(): void {
    const executor = new SeatPoolExecutor({
      slots: this.slots,
      readJob: (jobId) => this.readJob(jobId),
      readState: (job) => this.readState(job),
      effects: this.effects(),
      hostSample: () => ({ cpuCount: 4, load1: 0.4, freeMemoryBytes: 1e10 }),
      usageLimited: async () => new Set(),
      busySessions: async () => new Map(),
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

  async tickAt(offsetSeconds: number): Promise<void> {
    this.nowMs = T0 + offsetSeconds * 1000;
    const job = this.jobs.getByIdOrThrow(this.jobId);
    const parsed = parseWatchdogSpec(job.specYaml);
    const policyJob = {
      jobId: job.jobId, policy: job.policy, target: watchdogJobTarget(job, parsed.target), intervalSeconds: job.intervalSeconds,
      context: parsed.context, lastEvaluationAt: job.lastEvaluationAt, lastFireAt: job.lastFireAt, registeredBySession: job.registeredBySession,
      registeredAt: job.registeredAt,
    } as unknown as PolicyJob;
    const outcome = await this.policy.evaluate(policyJob);
    if (outcome.action === "skip" && !QUIET.has(outcome.reason)) {
      this.history.record({ jobId: job.jobId, evaluatedAt: iso(this.nowMs), outcome: "skipped", skipReason: outcome.reason, evaluationNotes: outcome.notes ?? null });
    }
    await this.executor.settled(job.jobId);
  }

  async submit(intents: SeatPoolIntent[]): Promise<void> {
    expect(this.executor.submit(this.jobId, intents)).toBe(true);
    await this.executor.settled(this.jobId);
  }

  notes(reason: string): Array<Record<string, unknown>> {
    return this.history.listForJob(this.jobId, 100_000).filter((e) => e.skipReason === reason).reverse().map((e) => e.evaluationNotes as Record<string, unknown>);
  }
}

describe("T-012383: the executor (real queue, reader and history)", () => {
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

  it("the ping-pong: the rows of T-011798 and T-012070 stay at the model while s24 and s28 are past 40%, and go once s24 is relaunched", async () => {
    h.register({ ...BASE, max: 2 });
    const s24 = h.poolSeat("build.dev-1-s24");
    const s28 = h.poolSeat("build.dev-1-s28");
    h.context.set(s24.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(s28.session, { usedPercentage: 47, sampledAt: iso(T0 - 30_000) });
    h.nowMs = T0 - 2_000;
    const a = await h.row("qitem-T-011798");
    h.nowMs = T0 - 1_000;
    const b = await h.row("qitem-T-012070");
    h.nowMs = T0;
    await h.tickAt(61);
    await h.tickAt(200);
    expect(h.notes("seat_pool_dispatched")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // s24 relaunched clean: its new occupant's sample reads 8%; the next plan hands it the oldest row.
    h.context.set(s24.session, { usedPercentage: 8, sampledAt: iso(T0 + 250_000) });
    await h.tickAt(260);
    expect(h.notes("seat_pool_dispatched")).toEqual([expect.objectContaining({ qitemId: a, to: s24.session })]);
    expect(h.get(b).destinationSession).toBe(MODEL);
  });

  it("control: with context_limit_percent 0 the same full slots get both rows (the dispatches that came back to the model)", async () => {
    h.register({ ...BASE, max: 2, context_limit_percent: 0 });
    const s24 = h.poolSeat("build.dev-1-s24");
    const s28 = h.poolSeat("build.dev-1-s28");
    h.context.set(s24.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(s28.session, { usedPercentage: 47, sampledAt: iso(T0 - 30_000) });
    await h.row("qitem-T-011798");
    await h.row("qitem-T-012070");
    await h.tickAt(61);
    expect(h.notes("seat_pool_dispatched").map((n) => n["to"]).sort()).toEqual([s24.session, s28.session].sort());
    // With the rule off nothing is read.
    expect(h.contextReads).toEqual([]);
  });

  it("the reader: a slot the pool added carries its reading; an adopted seat, no reading or one with no percentage carries none", async () => {
    h.register({ ...BASE, max: 4 });
    const added = h.poolSeat("build.dev-1-s24");
    const adopted = h.poolSeat("build.dev-2", true);
    const none = h.poolSeat("build.dev-1-s25");
    const noPct = h.poolSeat("build.dev-1-s26");
    h.context.set(added.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(adopted.session, { usedPercentage: 90, sampledAt: iso(T0 - 30_000) });
    h.context.set(noPct.session, { usedPercentage: null, sampledAt: iso(T0 - 30_000) });
    const bySession = new Map(h.state().slots.map((s) => [s.sessionName, s]));
    expect(bySession.get(added.session)!.context).toEqual({ usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    expect(bySession.get(adopted.session)).not.toHaveProperty("context");
    expect(bySession.get(none.session)).not.toHaveProperty("context");
    expect(bySession.get(noPct.session)).not.toHaveProperty("context");
    // An adopted seat is never asked for.
    expect(h.contextReads).not.toContain(adopted.session);
  });

  it("recheck: a slot that passed the limit between the plan and the move skips the row with free_context; the row stays at the model", async () => {
    h.register({ ...BASE, max: 1 });
    const s = h.poolSeat("build.dev-1-s24");
    h.context.set(s.session, { usedPercentage: 35, sampledAt: iso(T0 - 30_000) });
    h.nowMs = T0 - 120_000;
    const a = await h.row("qitem-T-011798");
    h.nowMs = T0;
    const intent = dispatches(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    expect(intent).toMatchObject({ qitemId: a, slotId: s.slotId });
    h.context.set(s.session, { usedPercentage: 41, sampledAt: iso(T0 - 5_000) });
    await h.submit([intent]);
    expect(h.notes("seat_pool_dispatch_skipped")).toEqual([{
      qitemId: a, from: MODEL, to: s.session, code: "free_context", usedPercentage: 41, sampledAt: iso(T0 - 5_000), limit: 40,
    }]);
    expect(h.notes("seat_pool_dispatched")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("the demand command is told a full quiet slot is held by its context (receives null, held context); a slot under it receives", async () => {
    h.register({ ...BASE, max: 2, demand_cmd: { argv: ["demand"] } });
    const fullSlot = h.poolSeat("build.dev-1-s24");
    const fresh = h.poolSeat("build.dev-1-s25");
    h.context.set(fullSlot.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(fresh.session, { usedPercentage: 3, sampledAt: iso(T0 - 30_000) });
    await h.tickAt(1);
    const last = h.demandSlots.at(-1)!;
    expect(last.find((x) => x.session === fullSlot.session)).toMatchObject({ receives: null, held: "context" });
    const freshEntry = last.find((x) => x.session === fresh.session)!;
    expect(freshEntry.receives).toBe("free");
    expect(freshEntry).not.toHaveProperty("held");
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The sidecar the daemon reads (ContextUsageStore.readSidecarForNode)
// ---------------------------------------------------------------------------------------------------------------
describe("T-012383: readSidecarForNode, the live occupant's sidecar only", () => {
  let home: string;
  let db: Database.Database;
  const SESSION = "build-dev-1-s24@fabrica";
  const write = (sample: Record<string, unknown>) => {
    mkdirSync(join(home, "state", "context-usage"), { recursive: true });
    writeFileSync(join(home, "state", "context-usage", `${SESSION}.json`), JSON.stringify(sample));
  };
  const sample = (pct: number | null, sampledAt: string, generation?: string) => ({
    context_window: { context_window_size: 1_000_000, used_percentage: pct, remaining_percentage: pct === null ? null : 100 - pct },
    session_name: SESSION, sampled_at: sampledAt, ...(generation ? { occupant_generation: generation } : {}),
  });

  beforeEach(() => {
    home = mkdtempSync(join(tmpdir(), "ctx-pool-"));
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
  });
  afterEach(() => {
    db.close();
    rmSync(home, { recursive: true, force: true });
  });

  it("reads the percentage and the time of the sample", () => {
    write(sample(52, "2026-11-17T19:39:30.000Z"));
    const store = new ContextUsageStore(db, { stateDir: home });
    expect(store.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "known", usedPercentage: 52, sampledAt: "2026-11-17T19:39:30.000Z" });
  });

  it("a sample taken before the live occupant booted (a previous occupant's, under the same name) reads as unknown", () => {
    write(sample(52, "2026-11-17T19:30:00.000Z"));
    const store = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00" });
    expect(store.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "unknown", reason: "stale_generation" });
  });

  it("a sample recording the live occupant's generation is its own, whatever its time; one after the boot is too", () => {
    write(sample(52, "2026-11-17T19:30:00.000Z", "gen-2"));
    const own = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00", resolveOccupantGeneration: () => "gen-2" });
    expect(own.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "known", usedPercentage: 52 });
    write(sample(52, "2026-11-17T19:36:00.000Z"));
    const after = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00" });
    expect(after.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "known", usedPercentage: 52 });
  });

  it("a sample recording another occupant's generation reads as unknown, even sampled after the live one booted", () => {
    write(sample(52, "2026-11-17T19:36:00.000Z", "gen-1"));
    const store = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00", resolveOccupantGeneration: () => "gen-2" });
    expect(store.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "unknown", reason: "stale_generation" });
  });

  it("no sidecar, an unreadable one or one with no percentage reads as unknown", () => {
    const store = new ContextUsageStore(db, { stateDir: home });
    expect(store.readSidecarForNode("node-s24", SESSION).availability).toBe("unknown");
    mkdirSync(join(home, "state", "context-usage"), { recursive: true });
    writeFileSync(join(home, "state", "context-usage", `${SESSION}.json`), "{not json");
    expect(store.readSidecarForNode("node-s24", SESSION).availability).toBe("unknown");
    write(sample(null, "2026-11-17T19:39:30.000Z"));
    expect(store.readSidecarForNode("node-s24", SESSION).availability).toBe("unknown");
  });
});
