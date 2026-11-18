// T-012673: a seat the pool added whose open rows are all parked on a long wait of `return_long_gates` (a PR waiting for
// a person's review) hands them to the fixed seat of the spec, each parked again there on the same gate with the same
// wake, and is free for the next row.
//
// The case (17/11 23:06Z): 6 of the 10 seats of the reviewer pool each held one row on external:github-review, so they
// counted as occupied and the pool dispatched nothing for half an hour, until the supervisor moved the rows by hand
// (handoff --no-nudge to qualidade-revisor-2@fabrica, then block again on the same gate).

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { PodRepository } from "../src/domain/pod-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { defaultParkWakeMessage, QueueRepository } from "../src/domain/queue-repository.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { WatchdogHistoryLog } from "../src/domain/watchdog-history-log.js";
import { parseWatchdogSpec, watchdogJobTarget } from "../src/domain/watchdog-policy-engine.js";
import {
  readSeatPoolBusyCandidates,
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
  seatPoolLongGateOf,
  seatPoolLongGateRows,
  seatPoolReceives,
  type ParkFacts,
  type SeatPoolIntent,
  type SeatPoolPendingRow,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import type { Policy, PolicyJob } from "../src/domain/policies/types.js";

const TAG = "tarefa-nova";
const GATE = "external:github-review";
const FIXED = "qualidade-revisor-2@fabrica";
const RULE = { gates: [GATE], to: FIXED };

// ---------------------------------------------------------------------------------------------------------------
// The spec
// ---------------------------------------------------------------------------------------------------------------
const SPEC_BASE = { max: 6, cwd_template: "/w/{rig}/{slot}", cooldown_s: 0, idle_grace_s: 300, dispatch_tag: TAG };

function parse(extra: Record<string, unknown>) {
  return parseSeatPoolSpec({ ...SPEC_BASE, ...extra });
}

function spec(extra: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parse(extra);
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

describe("T-012673: the spec's return_long_gates", () => {
  it("without the key the spec is the one from before it: no returnLongGates property", () => {
    const s = spec();
    expect(s).not.toHaveProperty("returnLongGates");
    expect(Object.keys(spec())).toEqual(Object.keys(s));
  });

  it("with gates and to it parses with the defaults (min_blocked_s 1800, max_rows 4)", () => {
    expect(spec({ return_long_gates: RULE }).returnLongGates).toEqual({ gates: [GATE], minBlockedSeconds: 1800, to: FIXED, maxRows: 4 });
    expect(spec({ return_long_gates: { ...RULE, min_blocked_s: 0, max_rows: 32 } }).returnLongGates)
      .toEqual({ gates: [GATE], minBlockedSeconds: 0, to: FIXED, maxRows: 32 });
  });

  it.each([
    ["not an object", ["external:github-review"], "return_long_gates"],
    ["a gate without external:", { ...RULE, gates: ["github-review"] }, "return_long_gates.gates"],
    ["a bare external:", { ...RULE, gates: ["external:"] }, "return_long_gates.gates"],
    ["a gate with whitespace", { ...RULE, gates: ["external:github review"] }, "return_long_gates.gates"],
    ["no gate", { ...RULE, gates: [] }, "return_long_gates.gates"],
    ["gates missing", { to: FIXED }, "return_long_gates.gates"],
    ["to without @", { ...RULE, to: "qualidade-revisor-2" }, "return_long_gates.to"],
    ["to missing", { gates: [GATE] }, "return_long_gates.to"],
    ["an unknown field", { ...RULE, wake_s: 60 }, "return_long_gates"],
    ["min_blocked_s below 0", { ...RULE, min_blocked_s: -1 }, "return_long_gates.min_blocked_s"],
    ["min_blocked_s above 7 days", { ...RULE, min_blocked_s: 604_801 }, "return_long_gates.min_blocked_s"],
    ["max_rows 0", { ...RULE, max_rows: 0 }, "return_long_gates.max_rows"],
    ["max_rows above 32", { ...RULE, max_rows: 33 }, "return_long_gates.max_rows"],
    ["max_rows not whole", { ...RULE, max_rows: 1.5 }, "return_long_gates.max_rows"],
  ])("refuses %s", (_what, value, field) => {
    const parsed = parse({ return_long_gates: value });
    expect("problem" in parsed && parsed.problem.field).toBe(field);
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The predicate and the planner
// ---------------------------------------------------------------------------------------------------------------
const NOW = Date.parse("2026-11-17T23:06:00.000Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const MODEL_SESSION = "qualidade-revisor@fabrica";

function park(qitemId: string, overrides: Partial<ParkFacts> = {}): ParkFacts {
  return {
    qitemId, blockedOn: GATE, timerIntervalS: null, timerArmedLive: false, parkActor: null, parkedAt: null, takenAt: ago(7200),
    blockedSince: NOW - 3_600_000, wakeFiredAt: null, returnRefusal: null, ...overrides,
  };
}

/** A slot of the pool, live and idle at its prompt, holding one row parked on github-review for 1 h. */
function slot(n: number, overrides: Partial<SeatPoolSlotView> = {}): SeatPoolSlotView {
  return {
    slotId: n, logicalId: `qualidade.revisor-s${n}`, sessionName: `qualidade-revisor-s${n}@fabrica`, nodeId: `node-s${n}`, phase: "live",
    adopted: false, nodeExists: true, openRows: 1, heldMessages: 0, idleSince: ago(600), cause: "idle", atPrompt: true, backgroundWork: false,
    parked: park(`qitem-s${n}`), ...overrides,
  };
}

/** A free slot: no row, idle. */
const free = (n: number) => slot(n, { openRows: 0, parked: undefined });

function row(qitemId: string, ageS = 120): SeatPoolPendingRow {
  return { qitemId, destination: MODEL_SESSION, waitingSince: ago(ageS), tags: [TAG] };
}

function state(slots: SeatPoolSlotView[], pending: SeatPoolPendingRow[] = []): SeatPoolState {
  return {
    model: { rigId: "rig", rigName: "fabrica", podNamespace: "qualidade", memberId: "revisor", logicalId: "qualidade.revisor", nodeId: "node-model", sessionName: MODEL_SESSION },
    modelCause: "working", restoring: false, slots, adoptable: [], pending, demand: null, lastScaleAt: null,
  };
}

const plan = (s: SeatPoolSpec, st: SeatPoolState) => planSeatPool(s, st, NOW, 120_000).intents;
const returns = (intents: SeatPoolIntent[]) => intents.filter((i) => i.kind === "return_long_gate");
const dispatches = (intents: SeatPoolIntent[]) => intents.filter((i): i is Extract<SeatPoolIntent, { kind: "dispatch" }> => i.kind === "dispatch");

describe("T-012673: the predicate and the planner", () => {
  const on = spec({ return_long_gates: RULE });

  it("(1) a slot with 1 row on github-review for 1 h is planned back to the fixed seat, and gets no row in the same plan", () => {
    const intents = plan(on, state([slot(1), free(2)], [row("qitem-b"), row("qitem-c", 60)]));
    expect(returns(intents)).toEqual([{ kind: "return_long_gate", slotId: 1, qitemIds: ["qitem-s1"], to: FIXED }]);
    expect(dispatches(intents).map((d) => d.slotId)).toEqual([2]);
  });

  it("(2) a slot whose row is in progress (no park) is not touched", () => {
    expect(seatPoolLongGateRows(on, slot(1, { parked: undefined, cause: "working", idleSince: null }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: undefined }), NOW)).toBeNull();
    expect(returns(plan(on, state([slot(1, { parked: undefined })])))).toEqual([]);
  });

  it("(3) a gate out of the list is not touched; a gate named per task matches, a longer name does not", () => {
    expect(returns(plan(on, state([slot(1, { parked: park("qitem-s1", { blockedOn: "external:outro" }) })])))).toEqual([]);
    expect(seatPoolLongGateOf(on.returnLongGates!, "external:github-review-T-012673")).toBe(GATE);
    expect(seatPoolLongGateOf(on.returnLongGates!, "external:github-review:pr-42")).toBe(GATE);
    expect(seatPoolLongGateOf(on.returnLongGates!, "external:github-review/pr/42")).toBe(GATE);
    expect(seatPoolLongGateOf(on.returnLongGates!, "external:github-reviewer")).toBeNull();
    expect(seatPoolLongGateOf(on.returnLongGates!, "external:github-review-")).toBeNull();
    expect(seatPoolLongGateOf(on.returnLongGates!, "external:outro-github-review")).toBeNull();
  });

  it("a row blocked for less than min_blocked_s, or since an unknown time, is not touched", () => {
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { blockedSince: NOW - 1_799_000 }) }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { blockedSince: NOW - 1_800_000 }) }), NOW)).not.toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { blockedSince: null }) }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { blockedSince: undefined }) }), NOW)).toBeNull();
  });

  it("an adopted seat, a draining pool, a seat with a held message, not at its prompt, with a background shell or a busy process is not touched", () => {
    expect(seatPoolLongGateRows(on, slot(1, { adopted: true }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(spec({ return_long_gates: RULE, drain: true }), slot(1), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { heldMessages: 1 }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { cause: "working" }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { atPrompt: false }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { backgroundWork: true }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { busyProcess: "suite-flock" }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { phase: "draining" }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { cause: "stopped" }), NOW)).toBeNull();
    // The rule off.
    expect(seatPoolLongGateRows(spec(), slot(1), NOW)).toBeNull();
  });

  it("several rows: all on long gates (up to max_rows) go together; one on another gate or past max_rows keeps them all", () => {
    const two = { openRows: 2, parked: undefined, parkedRows: [park("qitem-a"), park("qitem-b", { blockedOn: "external:github-review-T-1" })] };
    expect(returns(plan(on, state([slot(1, two)])))).toEqual([{ kind: "return_long_gate", slotId: 1, qitemIds: ["qitem-a", "qitem-b"], to: FIXED }]);
    const mixed = { ...two, parkedRows: [park("qitem-a"), park("qitem-b", { blockedOn: "external:suite-flock-T-1" })] };
    expect(seatPoolLongGateRows(on, slot(1, mixed), NOW)).toBeNull();
    expect(seatPoolLongGateRows(spec({ return_long_gates: { ...RULE, max_rows: 1 } }), slot(1, two), NOW)).toBeNull();
    // 2 open rows, 1 park read (the other is pending or in progress): no.
    expect(seatPoolLongGateRows(on, slot(1, { ...two, parkedRows: [park("qitem-a")] }), NOW)).toBeNull();
  });

  it("a row the queue would refuse to move, or with unknown refusal, is never planned, and the seat keeps its turn as a parked receiver", () => {
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { returnRefusal: "wake_kind_unsupported" }) }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { returnRefusal: "dependent_at_target" }) }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { returnRefusal: undefined }) }), NOW)).toBeNull();
    // Two rows, the 2nd held by a watchdog: neither is planned.
    const two = { openRows: 2, parked: undefined, parkedRows: [park("qitem-a"), park("qitem-b", { returnRefusal: "wake_kind_unsupported" })] };
    expect(seatPoolLongGateRows(on, slot(1, two), NOW)).toBeNull();
    // With dispatch_to_parked, such a seat on a short timer of its own still receives as parked.
    const both = spec({ return_long_gates: RULE, dispatch_to_parked: { max_wake_s: 7200 } });
    const own = slot(1, {
      parked: park("qitem-s1", {
        timerIntervalS: 3600, timerArmedLive: true, parkActor: "qualidade-revisor-s1@fabrica", parkedAt: ago(3600), takenAt: ago(7200),
        returnRefusal: "dependent_at_target",
      }),
    });
    const intents = plan(both, state([own], [row("qitem-b")]));
    expect(returns(intents)).toEqual([]);
    expect(dispatches(intents)).toEqual([expect.objectContaining({ qitemId: "qitem-b", slotId: 1, mode: "parked" })]);
  });

  it("a seat idle for less than 2 minutes, or a row whose wake fired less than 2 minutes ago, is not touched (the observed state lags)", () => {
    expect(seatPoolLongGateRows(on, slot(1, { idleSince: ago(119) }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { idleSince: ago(120) }), NOW)).not.toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { idleSince: null }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { wakeFiredAt: NOW - 119_000 }) }), NOW)).toBeNull();
    expect(seatPoolLongGateRows(on, slot(1, { parked: park("qitem-s1", { wakeFiredAt: NOW - 120_000 }) }), NOW)).not.toBeNull();
  });

  it("a fixed seat the queue does not take plans no move for any seat", () => {
    expect(returns(plan(on, { ...state([slot(1), slot(2)]), returnLongGatesRefused: true }))).toEqual([]);
  });

  it("with dispatch_to_parked a github-review park with a short timer of its own is no parked receiver while it is returned", () => {
    const both = spec({ return_long_gates: RULE, dispatch_to_parked: { max_wake_s: 7200 } });
    const own = slot(1, {
      parked: park("qitem-s1", { timerIntervalS: 3600, timerArmedLive: true, parkActor: "qualidade-revisor-s1@fabrica", parkedAt: ago(3600), takenAt: ago(7200) }),
    });
    // Without the rule it would get a 2nd row as parked.
    expect(seatPoolReceives(spec({ dispatch_to_parked: { max_wake_s: 7200 } }), own, NOW)).toBe("parked");
    const intents = plan(both, state([own], [row("qitem-b")]));
    expect(returns(intents)).toHaveLength(1);
    expect(dispatches(intents)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The executor: the real queue, the reader and the history
// ---------------------------------------------------------------------------------------------------------------
const RIG = "fabrica";
const MODEL = "qualidade-revisor@fabrica";
const LEAD = "coordenacao-orquestrador-tech@fabrica";
const T0 = Date.parse("2026-11-17T23:06:00.000Z");
const IDLE_SINCE = new Date(T0 - 7_200_000).toISOString();
const iso = (ms: number) => new Date(ms).toISOString();
const QUIET = new Set(["seat_pool_steady", "seat_pool_busy", "not_due"]);
const BASE: Record<string, unknown> = {
  min: 0, max: 1, cooldown_s: 0, idle_grace_s: 86400, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}", dispatch_tag: TAG,
  return_long_gates: { gates: [GATE], to: FIXED, min_blocked_s: 1800 },
};
type Mode = "idle" | "working";

class Harness {
  readonly db: Database.Database;
  readonly rigRepo: RigRepository;
  readonly bus: EventBus;
  readonly queue: QueueRepository;
  readonly outbox: OutboxHandler;
  readonly jobs: WatchdogJobsRepository;
  readonly history: WatchdogHistoryLog;
  readonly slots: SeatPoolSlots;
  readonly rigId: string;
  readonly podId: string;
  readonly mode = new Map<string, Mode>();
  private clockMs = T0;
  get nowMs(): number { return this.clockMs; }
  set nowMs(ms: number) { this.clockMs = ms; vi.setSystemTime(ms); }
  jobId = "";
  executor!: SeatPoolExecutor;
  policy!: Policy;
  /** Runs inside the handoff effect, before the queue moves anything: a change "between" the recheck and the move. */
  beforeHandoff: (() => void) | null = null;
  /** What the executor's busy read (busySessions, busyNow) returns: a seat's busy program, or null. */
  busy = new Map<string, string | null>();

  constructor(opts: { validateRig?: (session: string) => boolean } = {}) {
    this.nowMs = T0;
    this.db = createDb();
    migrate(this.db, ALL_MIGRATIONS);
    this.bus = new EventBus(this.db);
    this.rigRepo = new RigRepository(this.db);
    const rig = this.rigRepo.createRig(RIG);
    this.rigId = rig.id;
    this.podId = new PodRepository(this.db).createPod(rig.id, "qualidade", "Qualidade").id;
    this.queue = new QueueRepository(this.db, this.bus, opts.validateRig ? { validateRig: opts.validateRig } : undefined);
    this.outbox = new OutboxHandler(this.db);
    this.queue.attachOutbox(this.outbox);
    this.jobs = new WatchdogJobsRepository(this.db);
    this.history = new WatchdogHistoryLog(this.db);
    this.slots = new SeatPoolSlots(this.db);
    this.addSeat("qualidade.revisor");
    this.addSeat("qualidade.revisor-2");
    this.boot();
  }

  addSeat(logicalId: string): { session: string; nodeId: string } {
    const node = this.rigRepo.addNode(this.rigId, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: this.podId });
    const session = seatSessionName(logicalId, RIG);
    this.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${logicalId}`, node.id, session);
    this.mode.set(node.id, "idle");
    return { session, nodeId: node.id };
  }

  /** A slot the pool added (with an Operation-Id), live and idle at its prompt. */
  poolSeat(logicalId: string): { session: string; slotId: number; nodeId: string } {
    const seat = this.addSeat(logicalId);
    const slot = this.slots.insert({ poolJobId: this.jobId, logicalId, nodeId: seat.nodeId, phase: "live", intent: null, opId: `op-${logicalId}`, at: iso(T0 - 7_200_000) });
    return { ...seat, slotId: slot.slotId };
  }

  async row(destination: string, tags: string[] = [TAG]): Promise<string> {
    return (await this.queue.create({ sourceSession: LEAD, destinationSession: destination, body: "review the PR", tags, nudge: false } as never)).qitemId;
  }

  /** A row sent to the seat, claimed by it `claimAgoS` ago and parked by it on `gate` `parkAgoS` ago (with a timer of
   *  `wakeAfter` seconds, or none with null). */
  async park(seat: { session: string }, opts: { gate?: string; wakeAfter?: number | null; claimAgoS?: number; parkAgoS?: number; wakeMessage?: string; tags?: string[] } = {}): Promise<string> {
    const { gate = GATE, wakeAfter = 7200, claimAgoS = 7200, parkAgoS = 3600 } = opts;
    const back = this.nowMs;
    this.nowMs = back - claimAgoS * 1000;
    const a = await this.row(seat.session, opts.tags);
    this.queue.claim({ qitemId: a, destinationSession: seat.session });
    this.nowMs = back - parkAgoS * 1000;
    this.queue.update({
      qitemId: a, actorSession: seat.session, state: "blocked", blockedOn: gate, transitionNote: `parked on ${gate}`,
      ...(wakeAfter !== null ? { wakeAfterSeconds: wakeAfter } : {}), ...(opts.wakeMessage ? { wakeMessage: opts.wakeMessage } : {}),
    } as never);
    this.nowMs = back;
    return a;
  }

  get(qitemId: string): { destinationSession: string; state: string; tags: string[] | null; blockedOn: string | null; chainOfRecord: string[] | null; handedOffFrom: string | null } {
    return this.queue.getById(qitemId)! as never;
  }

  /** The successor a handoff of `qitemId` created. */
  successorOf(qitemId: string): string {
    return (this.db.prepare("SELECT qitem_id FROM queue_items WHERE handed_off_from = ?").get(qitemId) as { qitem_id: string }).qitem_id;
  }

  outboxCount(): number {
    return (this.db.prepare("SELECT COUNT(*) AS n FROM outbox_entries").get() as { n: number }).n;
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

  /** The reader as startup.ts builds it, the row facts with every argument. */
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState {
    return readSeatPoolState({
      db: this.db,
      slots: this.slots,
      openRows: (sessionName) => this.queue.listOpenObligations(sessionName, 500),
      rowFacts: (sessionName, maxBlocked, opts) => this.queue.listPoolSeatRowFacts(sessionName, maxBlocked, opts),
      acceptsDestination: (sessionName) => this.queue.acceptsDestination(sessionName),
      sessionAddresses: (sessionName) => [sessionName],
      heldMessages: (nodeId) => this.outbox.heldCount(nodeId),
      seatStopped: () => false,
      seatState: (nodeId) => (this.mode.get(nodeId) === "idle"
        ? { activity: "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: null, changedAt: IDLE_SINCE }
        : { activity: "active", needsInput: { count: 0 }, backgroundWork: null, changedAt: IDLE_SINCE }) as never,
      restoring: () => false,
      demand: (jobId) => this.executor.demandSample(jobId),
    }, job);
  }

  state(): SeatPoolState { return this.readState({ jobId: this.jobId, targetSession: MODEL, spec: this.spec() }); }

  effects(): SeatPoolEffects {
    const queueEffects = seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "test" }) });
    return {
      ...queueEffects,
      handoffParked: (input) => {
        this.beforeHandoff?.();
        return queueEffects.handoffParked(input);
      },
      addSlot: async () => ({ ok: false, code: "test_no_add", error: "the test adds no seat" }),
      operationNodeId: () => null,
      launchSlot: async () => ({ ok: false, code: "test_no_launch", error: "the test launches no seat" }),
      sessionRunning: () => true,
      lifecycle: async (_nodeId, fn) => fn(),
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: "x" }) as never,
      stopSlot: async () => ({ ok: false, code: "test_no_stop", error: "the test stops no seat" }),
      removeSlot: async () => ({ ok: false, code: "test_no_remove", error: "the test removes no seat" }),
      runCommand: async () => ({ exitCode: 0, stdout: JSON.stringify({ desired: 0 }) }),
      cleanSlot: async () => ({ ok: true }),
      relaunchSeat: async () => ({ ok: false, code: "test_no_relaunch", error: "the test relaunches no seat" }),
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
      busySessions: async () => this.busy,
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

describe("T-012673: the executor (real queue, reader and history)", () => {
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

  it("(1) a slot with 1 row on github-review: the row goes to the fixed seat, blocked on the same gate with the same wake, unwoken; the slot then takes a row as free", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1, { wakeAfter: 7200, tags: [TAG, "revisao-fixa", "mission:x"] });
    const oldWake = h.queue.getParkWakeStatus(a)!;
    expect(h.jobs.getById(oldWake.ref)!.intervalSeconds).toBe(7200);
    const b = await h.row(MODEL);
    const outboxBefore = h.outboxCount();

    await h.tickAt(61);

    const returned = h.notes("seat_pool_long_gate_returned");
    expect(returned).toHaveLength(1);
    const n = h.successorOf(a);
    expect(returned[0]).toEqual({
      slot: s1.session, qitemId: a, to: FIXED, gate: GATE, blockedOn: GATE, blockedSince: iso(T0 - 3_600_000), newQitemId: n, wakeAfterSeconds: 7200,
    });
    // The old row: handed off, the passage on its last transition.
    expect(h.get(a).state).toBe("handed-off");
    const closing = h.queue.listTransitions(a).at(-1)!;
    expect(closing.state).toBe("handed-off");
    expect(closing.transitionNote).toContain(`returned from ${s1.session}`);
    expect(closing.transitionNote).toContain(GATE);
    // The successor: at the fixed seat, blocked on the same gate, the chain ending in the old row, without the pool's tag.
    expect(h.get(n)).toMatchObject({ destinationSession: FIXED, state: "blocked", blockedOn: GATE, handedOffFrom: a });
    expect(h.get(n).chainOfRecord!.at(-1)).toBe(a);
    // Its other tags kept, in their order (the fixed seat's revisao-fixa and mission:<x>, K6h).
    expect(h.get(n).tags).toEqual(["revisao-fixa", "mission:x"]);
    // The same wake: a live timer of the same interval, its default message naming the successor; the old timer retired.
    const newWake = h.queue.getParkWakeStatus(n)!;
    expect(newWake).toMatchObject({ kind: "timer", phase: "armed", live: true });
    const job = h.jobs.getById(newWake.ref)!;
    expect(job.intervalSeconds).toBe(7200);
    expect(job.targetSession).toBe(FIXED);
    expect(job.specYaml).toContain(JSON.stringify(defaultParkWakeMessage(n)));
    expect(h.jobs.getById(oldWake.ref)!.state).not.toBe("active");
    // No wake went to the fixed seat (nor to anyone).
    expect(h.outboxCount()).toBe(outboxBefore);
    // Row b waited at the model: nothing was dispatched to the slot in the same plan.
    expect(h.get(b).destinationSession).toBe(MODEL);
    // The next plan reads the slot free, and hands it the row.
    const view = h.state().slots.find((s) => s.slotId === s1.slotId)!;
    expect(view.openRows).toBe(0);
    expect(seatPoolReceives(h.spec(), view, h.nowMs)).toBe("free");
    await h.tickAt(120);
    expect(h.notes("seat_pool_dispatched")).toEqual([expect.objectContaining({ qitemId: b, to: s1.session })]);
    // The successor stays where it is.
    expect(h.get(n)).toMatchObject({ destinationSession: FIXED, state: "blocked", blockedOn: GATE });
  });

  it("a seat running a busy program keeps its rows (skipped, code busy); once it stops, they go (S1 of the cross review)", async () => {
    h.register({ ...BASE, dispatch_to_parked: { busy_processes: ["suite-flock"] } });
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    h.busy.set(s1.session, "suite-flock (pid 4242)");

    await h.tickAt(61);

    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([
      expect.objectContaining({ slot: s1.session, qitemIds: [a], code: "busy", busyProcess: "suite-flock (pid 4242)" }),
    ]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked", blockedOn: GATE });

    h.busy.set(s1.session, null);
    await h.tickAt(200);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([expect.objectContaining({ slot: s1.session, qitemId: a, to: FIXED })]);
    expect(h.get(a).state).toBe("handed-off");
  });

  it("the daemon's busy read (readSeatPoolBusyCandidates) asks the seat the plan may empty by return_long_gates", async () => {
    h.register({ ...BASE, dispatch_to_parked: { busy_processes: ["suite-flock"] } });
    const s1 = h.poolSeat("qualidade.revisor-s1");
    await h.park(s1);
    h.nowMs = T0 + 61_000;
    const view = h.state().slots.find((s) => s.sessionName === s1.session)!;
    expect(seatPoolLongGateRows(h.spec(), view, h.nowMs)).not.toBeNull();
    const asked: string[] = [];
    await readSeatPoolBusyCandidates(h.spec(), [view], h.nowMs, {
      processes: async () => [], panes: async () => [], processPane: async () => null,
      paneOf: (session) => { asked.push(session); return { paneId: "%1", tmuxSession: session }; },
    });
    expect(asked).toEqual([s1.session]);
  });

  it("(1b) a row parked with no wake goes to the fixed seat blocked with no wake; a custom wake message is carried", async () => {
    h.register({ ...BASE, max: 2 });
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const s2 = h.poolSeat("qualidade.revisor-s2");
    const a = await h.park(s1, { wakeAfter: null, gate: "external:github-review-T-012673" });
    const c = await h.park(s2, { wakeAfter: 3600, wakeMessage: "check PR #42 for the review" });
    await h.tickAt(61);
    const na = h.successorOf(a);
    const nc = h.successorOf(c);
    expect(h.get(na)).toMatchObject({ destinationSession: FIXED, state: "blocked", blockedOn: "external:github-review-T-012673" });
    expect(h.queue.getParkWakeStatus(na)).toBeNull();
    const wake = h.queue.getParkWakeStatus(nc)!;
    expect(h.jobs.getById(wake.ref)!.specYaml).toContain(JSON.stringify("check PR #42 for the review"));
    expect(h.notes("seat_pool_long_gate_returned").map((r) => [r["qitemId"], r["gate"], r["wakeAfterSeconds"]]).sort())
      .toEqual([[a, GATE, null], [c, GATE, 3600]].sort());
  });

  it("(1c) a row waiting on the old row waits on the successor", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    const lead = h.addSeat("coordenacao.orquestrador-tech");
    const d = await h.row(lead.session, []);
    h.queue.update({ qitemId: d, actorSession: lead.session, state: "blocked", blockedOn: a, transitionNote: "waits for the review" } as never);
    await h.tickAt(61);
    const n = h.successorOf(a);
    expect(h.get(d)).toMatchObject({ state: "blocked", blockedOn: n });
  });

  it("(2) a slot with a row in progress does not change", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.row(s1.session);
    h.queue.claim({ qitemId: a, destinationSession: s1.session });
    await h.tickAt(4000);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "in-progress" });
    // Nor one with a long-gate row and an in-progress row.
    const b = await h.park(s1);
    await h.tickAt(4100);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(b)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
  });

  it("(3) a gate out of the list does not change", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1, { gate: "external:suite-flock-T-012673" });
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked", blockedOn: "external:suite-flock-T-012673" });
  });

  it("a row parked for less than min_blocked_s stays, and goes once it is old enough (a re-park on the same gate keeps the count)", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1, { parkAgoS: 600 });
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    // The seat parks it again (blocked -> blocked, a new timer): the age still counts from T0 - 600.
    h.nowMs = T0 + 100_000;
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "blocked", blockedOn: GATE, wakeAfterSeconds: 1800, transitionNote: "still waiting" } as never);
    await h.tickAt(1201);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([expect.objectContaining({ qitemId: a, blockedSince: iso(T0 - 600_000), wakeAfterSeconds: 1800 })]);
  });

  it("a slot not at its prompt keeps its rows", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    h.mode.set(s1.nodeId, "working");
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a).destinationSession).toBe(s1.session);
  });

  it("recheck: a row the seat took back between the plan and the move stays (seat_pool_long_gate_skipped, seat_changed)", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    h.nowMs = T0 + 61_000;
    const intent = returns(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    expect(intent).toEqual({ kind: "return_long_gate", slotId: s1.slotId, qitemIds: [a], to: FIXED });
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "in-progress", transitionNote: "the review came" } as never);
    await h.submit([intent]);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([{ slot: s1.session, to: FIXED, qitemIds: [a], code: "seat_changed" }]);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "in-progress" });
  });

  it("recheck: a row parked again on another gate between the plan and the move stays", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    h.nowMs = T0 + 61_000;
    const intent = returns(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "blocked", blockedOn: "external:suite-flock-T-1", wakeAfterSeconds: 600, transitionNote: "running the suite" } as never);
    await h.submit([intent]);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([expect.objectContaining({ code: "seat_changed" })]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked", blockedOn: "external:suite-flock-T-1" });
  });

  it("the queue refuses, writing nothing: another gate, another seat, other open rows, a watchdog wake, a destination it does not accept", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    const call = (row: Partial<{ blockedOn: string }> = {}, extra: Partial<{ expectDestination: string; rows: string[] }> = {}) =>
      h.queue.handoffParkedRows((extra.rows ?? [a]).map((qitemId) => ({ qitemId, blockedOn: GATE, note: "test", ...row })), FIXED,
        { expectDestination: extra.expectDestination ?? s1.session, actorSession: "daemon@kernel" });
    expect(call({ blockedOn: "external:outro" })).toMatchObject({ ok: false, code: "blocked_on_mismatch", qitemId: a });
    expect(call({}, { expectDestination: "qualidade-revisor-s9@fabrica" })).toMatchObject({ ok: false, code: "open_rows_mismatch" });
    expect(call({}, { rows: [a, "qitem-x"] })).toMatchObject({ ok: false, code: "open_rows_mismatch" });
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    // A watchdog attached to the row (not a park timer of its own) can't be carried over.
    const watch = h.jobs.register({ policy: "periodic-reminder", targetSession: s1.session, intervalSeconds: 600, registeredBySession: LEAD,
      specYaml: `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(s1.session)}\nmessage: "check"\n` } as never);
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "blocked", blockedOn: GATE, wakeWatchdogId: watch.jobId, transitionNote: "on the operator's reminder" } as never);
    expect(call()).toMatchObject({ ok: false, code: "wake_kind_unsupported", qitemId: a });
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    // The effect refuses a destination the queue does not accept before reading anything.
    const strict = new QueueRepository(h.db, h.bus, { validateRig: (s) => s !== FIXED });
    const effects = seatPoolQueueEffects({ queueRepo: strict, escalationOwner: () => ({ owner: LEAD, reason: "test" }) });
    expect(effects.handoffParked({ rows: [{ qitemId: a, blockedOn: GATE, note: "test" }], from: s1.session, to: FIXED, dropTags: [TAG] }))
      .toMatchObject({ ok: false, code: "destination_refused" });
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
  });

  it("2 rows of one slot on long gates go together: both successors at the fixed seat, blocked, and the slot is free next", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    const b = await h.park(s1, { gate: "external:github-review-T-1", wakeAfter: 3600 });
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_failed")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_returned").map((r) => r["qitemId"]).sort()).toEqual([a, b].sort());
    for (const [old, gate] of [[a, GATE], [b, "external:github-review-T-1"]] as const) {
      expect(h.get(old).state).toBe("handed-off");
      expect(h.get(h.successorOf(old))).toMatchObject({ destinationSession: FIXED, state: "blocked", blockedOn: gate });
    }
    const view = h.state().slots.find((s) => s.slotId === s1.slotId)!;
    expect(view.openRows).toBe(0);
    expect(seatPoolReceives(h.spec(), view, h.nowMs)).toBe("free");
  });

  it("2 rows, the 2nd with a watchdog attached: neither moves, and nothing is recorded (the plan reads it)", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    const b = await h.park(s1);
    const watch = h.jobs.register({ policy: "periodic-reminder", targetSession: s1.session, intervalSeconds: 600, registeredBySession: LEAD,
      specYaml: `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(s1.session)}\nmessage: "check"\n` } as never);
    h.nowMs = T0 - 3_000_000;
    h.queue.update({ qitemId: b, actorSession: s1.session, state: "blocked", blockedOn: GATE, wakeWatchdogId: watch.jobId, transitionNote: "on the operator's reminder" } as never);
    await h.tickAt(61);
    await h.tickAt(91);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_failed")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    expect(h.get(b)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    // The queue, asked for both, moves neither: the 1st row is not handed on when the 2nd is refused.
    expect(h.queue.handoffParkedRows([{ qitemId: a, blockedOn: GATE, note: "t" }, { qitemId: b, blockedOn: GATE, note: "t" }], FIXED,
      { expectDestination: s1.session, actorSession: "daemon@kernel" })).toMatchObject({ ok: false, code: "wake_kind_unsupported", qitemId: b });
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    expect(h.db.prepare("SELECT COUNT(*) AS n FROM queue_items WHERE handed_off_from IN (?, ?)").get(a, b)).toEqual({ n: 0 });
  });

  it("a refusal of the queue after the recheck moves no row, and is recorded as skipped with its code, not failed", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    const b = await h.park(s1);
    const watch = h.jobs.register({ policy: "periodic-reminder", targetSession: s1.session, intervalSeconds: 600, registeredBySession: LEAD,
      specYaml: `policy: periodic-reminder\ntarget:\n  session: ${JSON.stringify(s1.session)}\nmessage: "check"\n` } as never);
    h.beforeHandoff = () => {
      h.beforeHandoff = null;
      h.queue.update({ qitemId: b, actorSession: s1.session, state: "blocked", blockedOn: GATE, wakeWatchdogId: watch.jobId, transitionNote: "attached" } as never);
    };
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([{ slot: s1.session, to: FIXED, qitemIds: expect.arrayContaining([a, b]), qitemId: b, code: "wake_kind_unsupported" }]);
    expect(h.notes("seat_pool_long_gate_failed")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    expect(h.get(b)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
  });

  it("a row of the fixed seat waiting on the slot's row keeps it where it is (the move would release that row)", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    const d = await h.row(FIXED, []);
    h.queue.update({ qitemId: d, actorSession: FIXED, state: "blocked", blockedOn: a, transitionNote: "waits for the review" } as never);
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    expect(h.get(d)).toMatchObject({ state: "blocked", blockedOn: a });
    expect(h.queue.handoffParkedRows([{ qitemId: a, blockedOn: GATE, note: "t" }], FIXED, { expectDestination: s1.session, actorSession: "daemon@kernel" }))
      .toMatchObject({ ok: false, code: "dependent_at_target" });
    expect(h.get(d)).toMatchObject({ state: "blocked", blockedOn: a });
  });

  it("a row whose wake just fired stays until 2 minutes after the fire", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1, { wakeAfter: 3600, parkAgoS: 3600 });
    h.queue.recordWatchdogWakeAttempt(h.queue.getParkWakeStatus(a)!.ref, "delivered");
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a).destinationSession).toBe(s1.session);
    await h.tickAt(121);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([expect.objectContaining({ qitemId: a, blockedSince: iso(T0 - 3_600_000) })]);
  });

  it("the age counts the unbroken block on any gate: a row parked again on github-review after hours on another gate is as old as its first park", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1, { gate: "external:suite-flock-T-1", parkAgoS: 7200 });
    // The change of gate (suite-flock -> github-review, blocked -> blocked) does not reset the count.
    h.nowMs = T0 - 60_000;
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "blocked", blockedOn: GATE, wakeAfterSeconds: 7200, transitionNote: "waiting for the review" } as never);
    // A note on the row (blocked -> blocked, no gate) keeps it too.
    h.nowMs = T0 - 30_000;
    h.queue.update({ qitemId: a, actorSession: s1.session, transitionNote: "still waiting" } as never);
    expect(h.queue.listPoolSeatRowFacts(s1.session, 1, { returnTo: FIXED }).parked!.blockedSince).toBe(T0 - 7_200_000);
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([expect.objectContaining({ qitemId: a, gate: GATE, blockedOn: GATE, blockedSince: iso(T0 - 7_200_000) })]);
    expect(h.get(h.successorOf(a))).toMatchObject({ destinationSession: FIXED, state: "blocked", blockedOn: GATE });
  });

  it("the age restarts when the row leaves blocked: taken back and parked again, it waits min_blocked_s from the new park", async () => {
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1, { parkAgoS: 7200 });
    h.nowMs = T0 - 300_000;
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "in-progress", transitionNote: "the review came" } as never);
    h.nowMs = T0 - 60_000;
    h.queue.update({ qitemId: a, actorSession: s1.session, state: "blocked", blockedOn: GATE, wakeAfterSeconds: 7200, transitionNote: "a 2nd round" } as never);
    expect(h.queue.listPoolSeatRowFacts(s1.session, 1, { returnTo: FIXED }).parked!.blockedSince).toBe(T0 - 60_000);
    await h.tickAt(61);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    await h.tickAt(1741);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([expect.objectContaining({ qitemId: a, blockedSince: iso(T0 - 60_000) })]);
  });

  it("the facts read as before without the key: no blockedSince, no atPrompt", async () => {
    h.register({ ...BASE, return_long_gates: undefined });
    const s1 = h.poolSeat("qualidade.revisor-s1");
    await h.park(s1);
    const view = h.state().slots[0]!;
    expect(view).not.toHaveProperty("atPrompt");
    expect(view).not.toHaveProperty("parked");
    expect(h.queue.listPoolSeatRowFacts(s1.session).parked).not.toHaveProperty("blockedSince");
    expect(h.queue.listPoolSeatRowFacts(s1.session, 1, { returnTo: FIXED }).parked).toMatchObject({
      blockedSince: T0 - 3_600_000, wakeFiredAt: null, returnRefusal: null,
    });
  });

  it("a fixed seat the queue does not take: no move is planned, and the failure is recorded once while it lasts", async () => {
    h.db.close();
    h = new Harness({ validateRig: (session) => session !== FIXED });
    h.register(BASE);
    const s1 = h.poolSeat("qualidade.revisor-s1");
    const a = await h.park(s1);
    await h.tickAt(61);
    await h.tickAt(91);
    await h.tickAt(121);
    expect(h.notes("seat_pool_long_gate_failed")).toEqual([{ to: FIXED, code: "destination_refused", error: expect.stringContaining(FIXED) }]);
    expect(h.notes("seat_pool_long_gate_skipped")).toEqual([]);
    expect(h.notes("seat_pool_long_gate_returned")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: s1.session, state: "blocked" });
    expect(h.state().returnLongGatesRefused).toBe(true);
  });
});
