// P04-v2 PR-A (T-018090; eval da qa, T-018103): the head-of-line block of the seat pool's release command.
//
// The case (spec specs/P04-v2-fork-openrig.md, 1.1): a free seat whose release_cmd refuses goes back to admission with no
// memory of the refusal, and the next plan (60 s later) picks the same highest slotId again: 2.350+ refusals in 7 days,
// 680 on one slot, and every free seat behind it held. With the backoff the refused seat waits 300 s, doubled per
// refusal in a row, at most 1800 s; the plan skips it in the drain order (still slotId descending) and drains the next
// one, while every count (removeStarting, freeOut's share of the surplus, fullOut) still takes all the free seats the
// pool added. Work on the seat (an open row), another occupant or the seat's retirement forget the backoff; a full seat
// (the context drain of #42, T-017384) waits a minute at most.
//
// The contract this eval fixes (the names of the spec, 1.2.3):
// - SeatPoolSlotView.releaseRefusedUntilMs?: number (epoch ms): the plan skips the seat while it is later than now.
// - SeatPoolExecutor.releaseRefusedUntilMs(jobId, { slotId, nodeId, openRows }): number | null, the executor's
//   backoff (releaseRefusalBackoff), and SeatPoolStateReaderDeps.releaseRefusedUntilMs with the same signature, which
//   readSeatPoolState turns into the view's field. The reader passes the seat's openRows: with an open row the backoff
//   is forgotten.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { readFileSync } from "node:fs";
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
  type SeatPoolIntent,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/fork-seat-pool.js";
import type { Policy, PolicyJob } from "../src/domain/policies/types.js";

// ---------------------------------------------------------------------------------------------------------------
// The planner (pure)
const NOW = Date.parse("2026-11-22T03:00:00.000Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const MODEL_SESSION = "build-dev-1@fabrica";
const TAG = "tarefa-nova";

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 6, cwd_template: "/w/{rig}/{slot}", cooldown_s: 0, idle_grace_s: 300, dispatch_tag: TAG, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

/** A slot the pool added, live and free (idle at its prompt for 10 min, no row). */
function slot(n: number, overrides: Partial<SeatPoolSlotView> = {}): SeatPoolSlotView {
  return {
    slotId: n, logicalId: `build.dev-1-s${n}`, sessionName: `build-dev-1-s${n}@fabrica`, nodeId: `node-s${n}`, phase: "live",
    adopted: false, nodeExists: true, openRows: 0, heldMessages: 0, idleSince: ago(600), cause: "idle", ...overrides,
  };
}
/** A slot added and not yet live (adding/added): a seat still starting, which holds no work. */
const starting = (n: number) => slot(n, { phase: "added", idleSince: null, nodeExists: true });
/** Refused by its release command, held out until `seconds` from now (negative: the backoff is over). */
const refused = (seconds: number) => ({ releaseRefusedUntilMs: NOW + seconds * 1000 });
/** Past `context_limit_percent` (52%, sampled 30 s ago): a full slot, which the context drain of #42 takes out. */
const fullCtx = () => ({ context: { usedPercentage: 52, sampledAt: ago(30) } });

function state(slots: SeatPoolSlotView[]): SeatPoolState {
  return {
    model: { rigId: "rig", rigName: "fabrica", podNamespace: "build", memberId: "dev-1", logicalId: "build.dev-1", nodeId: "node-model", sessionName: MODEL_SESSION },
    modelCause: "working", restoring: false, slots, adoptable: [], pending: [], demand: null, lastScaleAt: null,
  };
}
const plan = (s: SeatPoolSpec, slots: SeatPoolSlotView[]) => planSeatPool(s, state(slots), NOW, 120_000);
const kinds = (intents: SeatPoolIntent[], kind: SeatPoolIntent["kind"]) =>
  intents.filter((i) => i.kind === kind).map((i) => (i as { slotId: number }).slotId);

describe("P04-v2 PR-A: the plan skips a seat whose release was refused, and counts it (planner)", () => {
  it("(a) the highest slot refused: the drain goes to the next slotId down; with its backoff over, to it again", () => {
    // min 2, three free seats: a surplus of one.
    const held = plan(spec({ min: 2 }), [slot(1), slot(2), slot(3, refused(300))]);
    expect(held).toMatchObject({ target: 2, current: 3, reason: "scale_down" });
    expect(kinds(held.intents, "drain")).toEqual([2]);
    const over = plan(spec({ min: 2 }), [slot(1), slot(2), slot(3, refused(-1))]);
    expect(kinds(over.intents, "drain")).toEqual([3]);
  });

  it("(a) the order stays slotId descending among the seats not held: two refused at the top, the 3rd one goes", () => {
    // min 4, five free seats: a surplus of one.
    const result = plan(spec({ min: 4 }), [slot(1), slot(2), slot(5, refused(60)), slot(7, refused(1200)), slot(4)]);
    expect(kinds(result.intents, "drain")).toEqual([4]);
  });

  it("(b) every free seat held: no drain, and no starting seat is removed for them (removeStarting counts all the free)", () => {
    // Two free seats held + two starting: the target keeps the starting ones (2), the surplus (2) is the free seats'.
    const result = plan(spec({ min: 0 }), [slot(1, refused(300)), slot(2, refused(600)), starting(3), starting(4)]);
    expect(result).toMatchObject({ target: 2, current: 4 });
    expect(kinds(result.intents, "drain")).toEqual([]);
    expect(kinds(result.intents, "remove")).toEqual([]);
    expect(kinds(result.intents, "launch").sort()).toEqual([3, 4]);
  });

  it("(f) a free seat held does not send the drain to a full seat instead (fullOut counts all the free)", () => {
    // min 2: one seat over. The free seat would cover it; held, nothing goes this tick (never the full ones for it).
    const result = plan(spec({ min: 2 }), [slot(1, refused(300)), slot(4, fullCtx()), slot(5, fullCtx())]);
    expect(result).toMatchObject({ target: 2, current: 3 });
    expect(kinds(result.intents, "drain")).toEqual([]);
    // Control: not held, the free seat goes, and still no full one.
    expect(kinds(plan(spec({ min: 2 }), [slot(1), slot(4, fullCtx()), slot(5, fullCtx())]).intents, "drain")).toEqual([1]);
  });

  it("(f) with #42: a free seat held, a starting seat and a full seat: the full one goes, the starting one stays", () => {
    const result = plan(spec({ min: 0 }), [slot(1, refused(300)), starting(3), slot(4, fullCtx())]);
    expect(kinds(result.intents, "drain")).toEqual([4]);
    expect(kinds(result.intents, "remove")).toEqual([]);
    expect(kinds(result.intents, "launch")).toEqual([3]);
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The executor (real queue, reader, policy and history; the effects are fakes that kill or remove nothing real)
const RIG = "fabrica";
const MODEL = "build-dev-1@fabrica";
const LEAD = "coordenacao-orquestrador-tech@fabrica";
const T0 = Date.parse("2026-11-22T03:00:00.000Z");
const IDLE_SINCE = new Date(T0 - 7_200_000).toISOString();
const iso = (ms: number) => new Date(ms).toISOString();
const QUIET = new Set(["seat_pool_steady", "seat_pool_busy", "not_due"]);
const BASE: Record<string, unknown> = {
  min: 0, max: 6, cooldown_s: 0, idle_grace_s: 300, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}", dispatch_tag: TAG,
  release_cmd: { argv: ["release"] },
};

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
  /** Each release the executor asked: [seat session, the reason env, ms since T0]. */
  readonly releases: Array<[string, string, number]> = [];
  /** Sessions whose release command refuses (exit 1). */
  readonly refuse = new Set<string>();
  /** Nodes with no live session (cause "stopped": the plan reaps them). */
  readonly stopped = new Set<string>();
  /** Sessions working (not idle at their prompt). */
  readonly working = new Set<string>();
  readonly context = new Map<string, { usedPercentage: number | null; sampledAt: string | null }>();
  outOfUsage = false;
  relaunchOk = true;
  private nextSession = 1;
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

  /** A new occupant of the node: a new row in `sessions`, the latest one (the stop's precondition reads it). Also a
   *  seat launched again by hand (rig seat launch --fresh), outside the pool. */
  newSession(nodeId: string, session: string): void {
    this.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${this.nextSession++}`, nodeId, session);
  }

  addSeat(logicalId: string): { logicalId: string; session: string; nodeId: string } {
    const node = this.rigRepo.addNode(this.rigId, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: this.podId });
    const session = seatSessionName(logicalId, RIG);
    this.newSession(node.id, session);
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

  register(context: Record<string, unknown>): void {
    this.jobId = this.jobs.register({
      policy: "fork-seat-pool", specYaml: JSON.stringify({ policy: "fork-seat-pool", target: { session: MODEL }, context: { ...BASE, ...context } }), targetSession: MODEL,
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

  latestSessionId(nodeId: string): string | null {
    const row = this.db.prepare("SELECT id FROM sessions WHERE node_id = ? ORDER BY rowid DESC LIMIT 1").get(nodeId) as { id: string } | undefined;
    return row?.id ?? null;
  }

  /** The reader as startup.ts builds it, with the executor's backoff (the contract above). */
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState {
    return readSeatPoolState({
      db: this.db,
      slots: this.slots,
      openRows: (sessionName) => this.queue.listOpenObligations(sessionName, 500),
      sessionAddresses: (sessionName) => [sessionName],
      heldMessages: (nodeId) => this.outbox.heldCount(nodeId),
      seatStopped: (nodeId) => this.stopped.has(nodeId),
      seatState: (nodeId) => {
        const session = (this.db.prepare("SELECT session_name FROM sessions WHERE node_id = ? ORDER BY rowid DESC LIMIT 1").get(nodeId) as { session_name: string }).session_name;
        const working = this.working.has(session);
        return { activity: working ? "working" : "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: null, changedAt: IDLE_SINCE } as never;
      },
      restoring: () => false,
      demand: (jobId) => this.executor.demandSample(jobId),
      contextUsage: (_nodeId, sessionName) => this.context.get(sessionName) ?? null,
      releaseRefusedUntilMs: (jobId, slot) => this.executor.releaseRefusedUntilMs(jobId, slot),
    } as Parameters<typeof readSeatPoolState>[0], job);
  }

  state(): SeatPoolState { return this.readState({ jobId: this.jobId, targetSession: MODEL, spec: this.spec() }); }

  effects(): SeatPoolEffects {
    const queueEffects = seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "eval" }) });
    return {
      ...queueEffects,
      addSlot: async () => ({ ok: false, code: "eval_no_add", error: "the test adds no seat" }),
      operationNodeId: () => null,
      launchSlot: async () => ({ ok: false, code: "eval_no_launch", error: "the test launches no seat" }),
      sessionRunning: (nodeId) => !this.stopped.has(nodeId),
      lifecycle: async (_nodeId, fn) => fn(),
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: this.latestSessionId(nodeId) }) as never,
      // A fake: nothing real is stopped or removed (the seat only leaves the fake's view).
      stopSlot: async () => ({ ok: true }) as never,
      removeSlot: async () => ({ ok: true }) as never,
      runCommand: async (command, env) => {
        if (command.argv[0] !== "release") return { exitCode: 0, stdout: JSON.stringify({ desired: 0 }) };
        const seat = env["OPENRIG_SEAT_POOL_SLOT"] ?? "";
        this.releases.push([seat, env["OPENRIG_SEAT_POOL_RELEASE_REASON"] ?? "", (this.nowMs - T0) / 1000]);
        return { exitCode: this.refuse.has(seat) ? 1 : 0, stdout: "", ...(this.refuse.has(seat) ? { error: "release refused (eval)" } : {}) } as never;
      },
      cleanSlot: async () => ({ ok: true }),
      relaunchSeat: async ({ logicalId }) => {
        if (!this.relaunchOk) return { ok: false, code: "eval_no_relaunch", error: "the test relaunches no seat" };
        const node = this.db.prepare("SELECT id FROM nodes WHERE rig_id = ? AND logical_id = ?").get(this.rigId, logicalId) as { id: string };
        this.stopped.delete(node.id);
        this.newSession(node.id, seatSessionName(logicalId, RIG));
        return { ok: true } as never;
      },
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
      outOfUsage: async () => this.outOfUsage,
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

  /** One executor run with these intents at `offsetSeconds` (the drain clock runs in every run). */
  async submitAt(offsetSeconds: number, intents: SeatPoolIntent[]): Promise<void> {
    this.nowMs = T0 + offsetSeconds * 1000;
    expect(this.executor.submit(this.jobId, intents)).toBe(true);
    await this.executor.settled(this.jobId);
  }

  /** The executor's backoff for a seat, read as the reader reads it for a seat with no open row. */
  until(seat: { slotId: number; nodeId: string }): number | null {
    return this.executor.releaseRefusedUntilMs(this.jobId, { slotId: seat.slotId, nodeId: seat.nodeId, openRows: 0 });
  }

  /** Seconds from `atS` to the backoff's end (null without one). */
  waitFrom(seat: { slotId: number; nodeId: string }, atS: number): number | null {
    const until = this.until(seat);
    return until === null ? null : (until - (T0 + atS * 1000)) / 1000;
  }

  releasedSeats(): string[] { return this.releases.map(([seat]) => seat); }

  notes(reason: string): Array<Record<string, unknown>> {
    return this.history.listForJob(this.jobId, 100_000).filter((e) => e.skipReason === reason).reverse().map((e) => e.evaluationNotes as Record<string, unknown>);
  }
}

/** The real capture (eval p04-pra.qa/real, 2026-11-22, read only from the VM daemon's sqlite): the refusals of 7 days. */
const REAL = JSON.parse(readFileSync(new URL("./fixtures/seat-pool-release-refused/recusas-2026-11-22.json", import.meta.url), "utf8")) as {
  total_recusas: number;
  slots_top10: Array<{ slot: string; recusas: number }>;
  exemplo_slot_topo: { slot: string; primeiras_12: string[] };
};

describe("P04-v2 PR-A: the refused release's backoff (executor, real queue, reader and history)", () => {
  let h: Harness;
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    h = new Harness();
  });
  afterEach(() => {
    vi.useRealTimers();
    h.db.close();
  });

  it("(golden, real) the cadence of the most refused slot (680 in 7 days, one per tick): its 12 ticks ask it twice, not 12 times", async () => {
    expect(REAL.total_recusas).toBe(2550);
    expect(REAL.slots_top10[0]).toEqual({ slot: "qualidade.qa-s24", recusas: 680 });
    const times = REAL.exemplo_slot_topo.primeiras_12.map((t) => Date.parse(t));
    const offsets = times.map((t) => Math.round((t - times[0]!) / 1000));
    expect(offsets).toEqual([0, 63, 122, 182, 245, 304, 365, 427, 487, 547, 608, 668]);
    h.register({ min: 0 });
    const seat = h.poolSeat("build.dev-1-s24");
    h.refuse.add(seat.session);
    for (const offset of offsets) await h.tickAt(offset);
    // Before: one ask per tick (the 12 refusals of the capture). With the backoff: at 0, and at 304 (the first tick past
    // 300 s); the next would be 600 s after that, past the last tick.
    expect(h.releases.map(([, , t]) => t)).toEqual([0, 304]);
  });

  it("(a) the highest free seat refuses: the next tick drains the next slotId down, not the same seat again", async () => {
    h.register({ min: 2 });
    h.poolSeat("build.dev-1-s1");
    h.poolSeat("build.dev-1-s2");
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    await h.tickAt(0);
    expect(h.releasedSeats()).toEqual([s3.session]);
    expect(h.notes("seat_pool_returned").at(-1)).toMatchObject({ slot: "build.dev-1-s3", why: "release_refused" });
    await h.tickAt(60);
    expect(h.releasedSeats()).toEqual([s3.session, seatSessionName("build.dev-1-s2", RIG)]);
    expect(h.waitFrom(s3, 0)).toBe(300);
    // Read as the plan reads it: the seat refused still carries its backoff.
    expect(h.state().slots.find((s) => s.slotId === s3.slotId)?.releaseRefusedUntilMs).toBe(T0 + 300_000);
  });

  it("(c) the idle drain (release told 'idle'): 300, 600, 1200, 1800 s and 1800 s again, and no ask while held", async () => {
    h.register({ min: 0 });
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    const waits: Array<number | null> = [];
    let at = 0;
    for (let refusal = 1; refusal <= 5; refusal++) {
      await h.tickAt(at);
      expect(h.releases).toHaveLength(refusal);
      expect(h.releases.at(-1)).toEqual([s3.session, "idle", at]);
      const wait = h.waitFrom(s3, at)!;
      waits.push(wait);
      // Inside the backoff the plan asks nothing (one second before its end).
      await h.tickAt(at + wait - 1);
      expect(h.releases).toHaveLength(refusal);
      at += wait + 1;
    }
    expect(waits).toEqual([300, 600, 1200, 1800, 1800]);
  });

  it("(c) the forced pass at the drain's deadline (release told 'drain-deadline'): the refusal holds the seat 300 s", async () => {
    h.register({ drain: true, drain_deadline_s: 900 });
    const s3 = h.poolSeat("build.dev-1-s3");
    // Working, so the drain's plan does not take it before the deadline: only the forced pass asks it.
    h.working.add(s3.session);
    h.refuse.add(s3.session);
    await h.submitAt(0, []);
    expect(h.notes("seat_pool_drain_started")).toHaveLength(1);
    expect(h.releases).toEqual([]);
    await h.submitAt(901, []);
    expect(h.releases).toEqual([[s3.session, "drain-deadline", 901]]);
    expect(h.notes("seat_pool_drain_expired").at(-1)).toMatchObject({ left: [expect.objectContaining({ slot: "build.dev-1-s3", why: "release_refused" })] });
    expect(h.waitFrom(s3, 901)).toBe(300);
  });

  it("(c) the reap of a stopped seat (release told 'reap'): the refusal is held 300 s while the seat keeps its occupant", async () => {
    h.register({ min: 0 });
    const s3 = h.poolSeat("build.dev-1-s3");
    h.stopped.add(s3.nodeId);
    h.refuse.add(s3.session);
    // Out of usage: the relaunch the refusal asks for is held, so the occupant does not change.
    h.outOfUsage = true;
    await h.tickAt(0);
    expect(h.releases).toEqual([[s3.session, "reap", 0]]);
    expect(h.notes("seat_pool_held").at(-1)).toMatchObject({ reason: "usage_limit", why: "release_refused" });
    expect(h.waitFrom(s3, 0)).toBe(300);
    // Past the backoff, the same occupant refused again: 600 s.
    await h.tickAt(301);
    expect(h.releases.map(([, why, t]) => [why, t])).toEqual([["reap", 0], ["reap", 301]]);
    expect(h.waitFrom(s3, 301)).toBe(600);
  });

  it("(d) an open row on the seat forgets the backoff: once the row is done the seat drains at once, and a refusal starts at 300 s", async () => {
    h.register({ min: 0 });
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    await h.tickAt(0);
    expect(h.waitFrom(s3, 0)).toBe(300);
    const row = await h.queue.create({ sourceSession: LEAD, destinationSession: s3.session, body: "work", nudge: false } as never);
    await h.tickAt(30);
    expect(h.state().slots.find((s) => s.slotId === s3.slotId)?.releaseRefusedUntilMs).toBeUndefined();
    h.queue.claim({ qitemId: row.qitemId, destinationSession: s3.session } as never);
    await h.queue.update({ qitemId: row.qitemId, actorSession: s3.session, state: "done", closureReason: "no-follow-on" } as never);
    await h.tickAt(60);
    expect(h.releases.map(([, , t]) => t)).toEqual([0, 60]);
    expect(h.waitFrom(s3, 60)).toBe(300);
  });

  it("(d) another occupant (the reap's relaunch) forgets the backoff: the new session drains at once, from 300 s", async () => {
    h.register({ min: 0 });
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    await h.tickAt(0);
    expect(h.waitFrom(s3, 0)).toBe(300);
    // The session dies; the reap's release refuses too, and the seat is launched again: a new occupant.
    h.stopped.add(s3.nodeId);
    await h.tickAt(30);
    expect(h.releases.map(([, why]) => why)).toEqual(["idle", "reap"]);
    expect(h.notes("seat_pool_relaunched").at(-1)).toMatchObject({ seat: s3.session, why: "release_refused" });
    expect(h.stopped.has(s3.nodeId)).toBe(false);
    await h.tickAt(60);
    expect(h.releases.map(([, why, t]) => [why, t])).toEqual([["idle", 0], ["reap", 30], ["idle", 60]]);
    expect(h.waitFrom(s3, 60)).toBe(300);
  });

  it("(d) a seat launched again outside the pool (a new session) forgets the backoff: it drains at once", async () => {
    h.register({ min: 0 });
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    await h.tickAt(0);
    expect(h.waitFrom(s3, 0)).toBe(300);
    h.newSession(s3.nodeId, s3.session);
    expect(h.until(s3)).toBeNull();
    await h.tickAt(60);
    expect(h.releases.map(([, , t]) => t)).toEqual([0, 60]);
    expect(h.waitFrom(s3, 60)).toBe(300);
  });

  it("(d) a seat retired leaves no backoff behind", async () => {
    h.register({ min: 1 });
    h.poolSeat("build.dev-1-s1");
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    await h.tickAt(0);
    expect(h.waitFrom(s3, 0)).toBe(300);
    h.slots.retire(s3.slotId, iso(T0 + 10_000));
    expect(h.until(s3)).toBeNull();
  });

  it("(e) a restart of the daemon leaves no orphan: the backoff is gone or the same, and min, max, adopted and the drain hold", async () => {
    // min 3 with the adopted seat: one seat over.
    h.register({ min: 3, max: 5 });
    const adopted = h.poolSeat("build.dev-1-s9", true);
    h.poolSeat("build.dev-1-s1");
    const s2 = h.poolSeat("build.dev-1-s2");
    const s3 = h.poolSeat("build.dev-1-s3");
    h.refuse.add(s3.session);
    await h.tickAt(0);
    expect(h.waitFrom(s3, 0)).toBe(300);
    h.boot();
    const after = h.until(s3);
    expect(after === null || after === T0 + 300_000).toBe(true);
    const planned = planSeatPool(h.spec(), h.state(), T0 + 60_000, 120_000);
    expect(planned).toMatchObject({ target: 3, current: 4 });
    await h.tickAt(60);
    // One seat over min: exactly one more release, never the adopted seat's, and the one the restarted executor's
    // backoff says: forgotten, the s3 again; kept, the s2 (no backoff the plan does not honour, none it invents).
    expect(h.releases).toHaveLength(2);
    expect(h.releases[1]![0]).toBe(after === null ? s3.session : s2.session);
    expect(h.releasedSeats()).not.toContain(adopted.session);
    expect(h.spec()).toMatchObject({ min: 3, max: 5, drain: false });
    expect(h.slots.get(adopted.slotId)).toMatchObject({ opId: null, retiredAt: null });
  });

  it("(f) a full seat (the context drain of #42) whose release refuses is asked again after a minute, not after 300 s", async () => {
    h.register({ min: 0, max: 1, context_limit_percent: 40 });
    const s24 = h.poolSeat("build.dev-1-s24");
    h.context.set(s24.session, { usedPercentage: 52, sampledAt: iso(T0) });
    h.refuse.add(s24.session);
    // A row waiting at the model past dispatch_after_s that no seat takes: the full seat goes to make room (renew).
    await h.queue.create({ sourceSession: LEAD, destinationSession: MODEL, body: "work", tags: [TAG], nudge: false } as never);
    await h.tickAt(61);
    expect(h.releases).toEqual([[s24.session, "idle", 61]]);
    const wait = h.waitFrom(s24, 61);
    expect(wait === null || wait <= 60).toBe(true);
    await h.tickAt(122);
    expect(h.releases.map(([, , t]) => t)).toEqual([61, 122]);
  });
});
