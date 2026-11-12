// T-006090: a seat parked on its own wait takes no 2nd row while its wait is heavy local work: a gate the spec refuses
// (dispatch_to_parked.refuse_gates: an eval being frozen, a suite queued for the host's lock, a long run) or a live
// process under its pane the spec names (dispatch_to_parked.busy_processes: suite-flock, congelar-eval). The case of
// T-006052 (12/11): qualidade-qa-s10, idle at its prompt with its only row parked on external:kro3-rodada-longa and then
// on external:kro1v6-congelar-eval, got a 2nd row three times and gave it back to the model each time. A seat whose only
// wait is CI, a review or a push keeps taking it. Without the two fields the rule is the one from before (the frozen
// eval of T-005497 parks on external:qa-s6-congelar-e1-v4 with `dispatch_to_parked: {}`).
// Real captures (T-003440): test/fixtures/t6090/real/ (COMANDO.txt says how).

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
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
import { readSeatPoolBusySessions, seatPoolBusyCommand, seatPoolBusyProcess, type BusyProcessRow } from "../src/domain/seat-pool-busy.js";
import {
  seatPoolReceives,
  seatPoolSpecProblem,
  type ParkFacts,
  type SeatPoolPendingRow,
  type SeatPoolSlotView,
} from "../src/domain/policies/seat-pool.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const REAL = join(HERE, "fixtures", "t6090", "real");
const realTree = (file: string) => JSON.parse(readFileSync(join(REAL, file), "utf8")) as { session: string; panePid: number; rows: BusyProcessRow[] };
const realTransitions = () => JSON.parse(readFileSync(join(REAL, "qa-s10-parked-row.transitions.json"), "utf8")) as Array<{ state: string; note: string | null }>;
/** The gates qualidade-qa-s10 parked its only row on (the real transitions, "parked on <gate>"). */
const realGates = () => realTransitions().flatMap((t) => (t.state === "blocked" && t.note?.startsWith("parked on ") ? [t.note.slice("parked on ".length)] : []));

/** What the Billverso pool sets (T-006090): the gates and programs of heavy local work. */
const GATES = ["congelar", "suite-flock", "rodada-longa"];
const PROGRAMS = ["suite-flock", "congelar-eval"];
const HEAVY = { dispatch_to_parked: { refuse_gates: GATES, busy_processes: PROGRAMS } };

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
  /** T-006090: why each seat is busy as the process read sees it (absent: not read), and whether that read throws. */
  readonly busy = new Map<string, string | null>();
  busyThrows = false;
  /** Seats the read leaves out, as one that was not parked when the read picked its seats. */
  readonly busyLeftOut = new Set<string>();
  busyReads = 0;
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

  /** `planBusy`: the reader as the planner gets it in startup.ts (with the busy map); the executor's own reader has none,
   *  as in production, so its recheck and the demand command see a busy seat only through busySessions. */
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }, planBusy = true): SeatPoolState {
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
      busyProcess: planBusy ? (sessionName) => (this.busy.has(sessionName) ? this.busy.get(sessionName) : undefined) : undefined,
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
      readState: (job) => this.readState(job, false),
      effects: this.effects(),
      hostSample: () => this.host,
      // As startup.ts's busyPoolSessions (readSeatPoolBusyCandidates): the seats that could take a row now, free or
      // parked (T-006251), each with why it is busy or null.
      busySessions: async (job) => {
        this.busyReads += 1;
        if (this.busyThrows) throw new Error("no /proc");
        const read = new Map<string, string | null>();
        for (const slot of this.readState(job, false).slots) {
          if (this.busyLeftOut.has(slot.sessionName) || seatPoolCanReceive(job.spec, slot, this.nowMs) === null) continue;
          read.set(slot.sessionName, this.busy.get(slot.sessionName) ?? null);
        }
        return read;
      },
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

// ---------------------------------------------------------------------------------------------------------------
// The pure layer: the spec, the predicate and the planner (the helpers of qa-t5481-c1-policy.test.ts)
// ---------------------------------------------------------------------------------------------------------------
const NOW = Date.parse("2026-11-12T00:25:00.000Z");
const ago = (seconds: number) => iso(NOW - seconds * 1000);

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", cooldown_s: 60, idle_grace_s: 300, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

let nextSlot = 1;
/** The base case of e-c1: one row blocked on `blockedOn`, a 45-minute timer armed and live by the slot itself 30 min
 *  ago, after it took the row 1 h ago; at its prompt, "working" because of a background shell. */
function parkedSlot(blockedOn: string, overrides: Partial<SeatPoolSlotView> = {}, park: Partial<ParkFacts> = {}): SeatPoolSlotView {
  const slotId = overrides.slotId ?? nextSlot++;
  const sessionName = overrides.sessionName ?? `dev-impl-s${slotId}@r`;
  return {
    slotId, logicalId: `dev.impl-s${slotId}`, sessionName, nodeId: `node-${slotId}`, phase: "live", adopted: false, nodeExists: true,
    openRows: 1, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
    parked: { qitemId: `a-${slotId}`, blockedOn, timerIntervalS: 2700, timerArmedLive: true, parkActor: sessionName, parkedAt: ago(1800), takenAt: ago(3600), ...park },
    ...overrides,
  };
}

let nextRow = 1;
function pendingRows(count: number): SeatPoolPendingRow[] {
  return Array.from({ length: count }, () => ({ qitemId: `q-${nextRow++}`, destination: MODEL, waitingSince: ago(120), tags: ["tarefa-nova"] }));
}

function poolState(slots: SeatPoolSlotView[], pending: SeatPoolPendingRow[]): SeatPoolState {
  return {
    model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: MODEL },
    modelCause: "working", restoring: false, slots, adoptable: [], pending, demand: null, lastScaleAt: null,
  };
}

const can = (s: SeatPoolSpec, slot: SeatPoolSlotView) => seatPoolCanReceive(s, slot, NOW);
const dispatchesOf = (intents: SeatPoolIntent[]) => intents.filter((i) => i.kind === "dispatch");

describe("T-006090: dispatch_to_parked.refuse_gates and busy_processes (spec)", () => {
  it("{} reads as before: no refuse_gates, no busy_processes property", () => {
    const rule = spec(ON).dispatchToParked!;
    expect(rule).not.toHaveProperty("refuseGates");
    expect(rule).not.toHaveProperty("busyProcesses");
  });

  it("the lists are kept as given", () => {
    expect(spec(HEAVY).dispatchToParked).toMatchObject({ refuseGates: GATES, busyProcesses: PROGRAMS });
    expect(spec({ dispatch_to_parked: { refuse_gates: [] } }).dispatchToParked).toMatchObject({ refuseGates: [] });
  });

  it.each([
    ["refuse_gates", "congelar"],
    ["refuse_gates", [""]],
    ["refuse_gates", ["a b"]],
    ["refuse_gates", [3]],
    ["refuse_gates", null],
    ["refuse_gates", Array.from({ length: 33 }, (_, i) => `g${i}`)],
    ["refuse_gates", ["x".repeat(129)]],
    ["busy_processes", ["ops/bin/suite-flock"]],
    ["busy_processes", ["suite flock"]],
    ["busy_processes", {}],
    ["busy_processes", ["bash"]],
    ["busy_processes", ["python3.12"]],
    ["busy_processes", ["node"]],
  ])("%s = %j is refused, at registration and at update", (key, value) => {
    const context = { max: 3, cwd_template: "/work/{rig}/{slot}", dispatch_to_parked: { [key]: value } };
    const parsed = parseSeatPoolSpec(context);
    expect("problem" in parsed && parsed.problem.field).toBe(`dispatch_to_parked.${key}`);
    expect(seatPoolSpecProblem({ context })).toMatchObject({ field: `dispatch_to_parked.${key}` });
  });

  it("an unknown field still names the two new ones among the known", () => {
    const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", dispatch_to_parked: { refuse_gate: ["x"] } });
    expect("problem" in parsed && parsed.problem.message).toMatch(/known fields are .*refuse_gates.*busy_processes/);
  });
});

describe("T-006090: a parked seat on heavy local work takes no 2nd row (predicate and planner)", () => {
  it("golden of the real (T-006052): the gates qualidade-qa-s10 parked on are refused with the Billverso lists", () => {
    const gates = realGates();
    expect(gates).toEqual(["external:kro3-rodada-longa", "external:kro1v6-congelar-eval", "external:kro1v6-congelar-eval"]);
    for (const gate of new Set(gates)) {
      const slot = parkedSlot(gate, { sessionName: "qualidade-qa-s10@fabrica", cause: "idle", backgroundWork: false });
      // Before T-006090 (and with `{}` now) the seat counts as parked: that is how the row went round.
      expect(can(spec(ON), slot)).toBe("parked");
      expect(can(spec(HEAVY), slot)).toBeNull();
    }
  });

  it.each(["external:congelar-eval-fila", "external:qa-s6-congelar-e1-v4", "external:suite-flock-x", "external:t5-rodada-longa"])(
    "refused gate %s: null", (gate) => {
      expect(can(spec(HEAVY), parkedSlot(gate))).toBeNull();
    },
  );

  it.each(["external:ci-upstream-pr-2946", "external:revisao-c1", "external:push-ci5", "external:vez-lote-emissor3", "external:tudo-43107111549", "external:pronto-t5438", "external:suite-w4e"])(
    "a wait on CI, review or push still parks: %s", (gate) => {
      expect(can(spec(HEAVY), parkedSlot(gate))).toBe("parked");
    },
  );

  it("a busy process holds a parked seat back, whatever its gate; null and absent hold nothing back", () => {
    expect(can(spec(HEAVY), parkedSlot("external:ci-x", { busyProcess: "suite-flock (pid 9)" }))).toBeNull();
    expect(can(spec(HEAVY), parkedSlot("external:ci-x", { busyProcess: "process table not read: EACCES" }))).toBeNull();
    expect(can(spec(HEAVY), parkedSlot("external:ci-x", { busyProcess: null }))).toBe("parked");
    expect(can(spec(HEAVY), parkedSlot("external:ci-x"))).toBe("parked");
  });

  it("a free seat is held back by a busy process only with busy_processes (T-006251, B1; it was never held back before)", () => {
    const free: SeatPoolSlotView = {
      slotId: 90, logicalId: "dev.impl-s90", sessionName: "dev-impl-s90@r", nodeId: "node-90", phase: "live", adopted: false, nodeExists: true,
      openRows: 0, heldMessages: 0, idleSince: ago(600), cause: "idle", atPrompt: true, backgroundWork: false, busyProcess: "suite-flock (pid 1)",
    };
    expect(can(spec(HEAVY), free)).toBeNull();
    expect(can(spec(HEAVY), { ...free, busyProcess: null })).toBe("free");
    expect(can(spec(HEAVY), { ...free, busyProcess: undefined })).toBe("free");
    // refuse_gates alone (no busy_processes), `{}` and no dispatch_to_parked: a busyProcess holds nothing back.
    expect(can(spec({ dispatch_to_parked: { refuse_gates: GATES } }), free)).toBe("free");
    expect(can(spec(ON), free)).toBe("free");
    expect(can(spec({}), free)).toBe("free");
  });

  it("the planner gives the row to the seat waiting on CI, not to the one freezing an eval (nor to a busy one)", () => {
    const freezing = parkedSlot("external:kro1v6-congelar-eval", { slotId: 1 });
    const busy = parkedSlot("external:ci-a", { slotId: 2, busyProcess: "congelar-eval (pid 7)" });
    const ci = parkedSlot("external:ci-b", { slotId: 3 });
    const pending = pendingRows(2);
    const plan = planSeatPool(spec({ ...HEAVY, dispatch_after_s: 60 }), poolState([freezing, busy, ci], pending), NOW, 120_000);
    expect(dispatchesOf(plan.intents)).toEqual([{ kind: "dispatch", qitemId: pending[0]!.qitemId, from: MODEL, slotId: 3, mode: "parked" }]);
    expect(seatPoolReceives(spec(HEAVY), freezing, NOW)).toBeNull();
  });
});

describe("T-006090: the busy program under a seat's pane (seatPoolBusyCommand, seatPoolBusyProcess)", () => {
  it.each([
    ["bash /home/bruno/dev/setup/ops/bin/suite-flock nice -n 10 bash congelar-lugar.sh novo-6", "suite-flock"],
    ["nice -n 10 /home/bruno/dev/setup/ops/fabrica/congelar-eval /x --clone /y", "congelar-eval"],
    ["python3 -I /home/bruno/dev/setup/ops/fabrica/congelar-eval /x", "congelar-eval"],
    ["timeout 5600 suite-flock python3 -I ops/ci/pronto.py", "suite-flock"],
    ["SUITE_FLOCK_WAIT=0 suite-flock nice -n 10 python3 -I ops/ci/pronto.py", "suite-flock"],
    ["sh -c 'suite-flock make test'", "suite-flock"],
  ])("%s → %s", (command, program) => {
    expect(seatPoolBusyCommand(command, PROGRAMS)).toBe(program);
  });

  it.each([
    "grep -n suite-flock ops/ci/pronto.py",
    "rg congelar-eval docs",
    "/bin/bash -c source /home/bruno/.claude-proxy/shell-snapshots/snapshot-bash-1.sh 2>/dev/null || true && eval 'suite-flock x'",
    "flock 6",
    "/home/bruno/.local/bin/claude [args omitted]",
    "gh run watch 43107111549",
    "",
  ])("not a match: %s", (command) => {
    expect(seatPoolBusyCommand(command, PROGRAMS)).toBeNull();
  });

  it("golden of the real: qualidade-qa-7's pane holds 3 suite-flock in the background; coordenacao-coord-3's holds none", () => {
    const qa7 = realTree("qa-7-pane-tree.json");
    expect(qa7.rows.some((r) => r.pid === qa7.panePid)).toBe(true);
    const why = seatPoolBusyProcess(qa7.rows, qa7.panePid, PROGRAMS);
    // The lowest matching pid: of the four suite-flock under the pane, 58489.
    expect(why).toBe("suite-flock (pid 58489)");
    expect(qa7.rows.find((r) => r.pid === 58489)!.command).toContain("/suite-flock ");
    expect(seatPoolBusyProcess(qa7.rows, qa7.panePid, ["congelar-eval"])).toBeNull();
    const coord3 = realTree("coord-3-pane-tree.json");
    expect(seatPoolBusyProcess(coord3.rows, coord3.panePid, PROGRAMS)).toBeNull();
  });

  it("only descendants count: the same process under another pane is not this seat's", () => {
    const rows: BusyProcessRow[] = [
      { pid: 10, ppid: 1, command: "/bin/bash -l" }, { pid: 11, ppid: 10, command: "claude" },
      { pid: 20, ppid: 1, command: "/bin/bash -l" }, { pid: 21, ppid: 20, command: "bash /x/suite-flock nice true" },
      { pid: 30, ppid: 30, command: "bash /x/suite-flock" },
    ];
    expect(seatPoolBusyProcess(rows, 10, PROGRAMS)).toBeNull();
    expect(seatPoolBusyProcess(rows, 20, PROGRAMS)).toBe("suite-flock (pid 21)");
  });

  it("readSeatPoolBusySessions reads the table once; an unreadable table or pane holds the seat back", async () => {
    const qa7 = realTree("qa-7-pane-tree.json");
    const processes = vi.fn(async () => qa7.rows);
    // T-006251 (S3): the server's panes in one read; a seat's pane by its bound id.
    const panes = vi.fn(async () => [
      { paneId: "%1", panePid: qa7.panePid, session: "a@r", active: true }, { paneId: "%2", panePid: 999_999_999, session: "b@r", active: true },
    ]);
    const paneOf = (s: string) => ({ paneId: ({ "a@r": "%1", "b@r": "%2", "c@r": "%3" } as Record<string, string>)[s]!, tmuxSession: s });
    const read = await readSeatPoolBusySessions(["a@r", "b@r", "c@r"], PROGRAMS, { processes, panes, paneOf });
    expect(processes).toHaveBeenCalledTimes(1);
    expect(panes).toHaveBeenCalledTimes(1);
    expect(read.get("a@r")).toMatch(/^suite-flock/);
    expect(read.get("b@r")).toBeNull();
    expect(read.get("c@r")).toBe("pane not found");
    const failed = await readSeatPoolBusySessions(["a@r"], PROGRAMS, { processes: async () => { throw new Error("EACCES"); }, panes, paneOf });
    expect(failed.get("a@r")).toBe("process table not read: EACCES");
    const thrown = await readSeatPoolBusySessions(["a@r"], PROGRAMS, { processes, panes: async () => { throw new Error("tmux gone"); }, paneOf });
    expect(thrown.get("a@r")).toBe("panes not read: tmux gone");
    expect((await readSeatPoolBusySessions([], PROGRAMS, { processes, panes, paneOf })).size).toBe(0);
  });

  it("golden of the real: a suite-flock reparented out of the pane's tree still holds its seat by the TMUX_PANE it inherited", async () => {
    const orphans = JSON.parse(readFileSync(join(REAL, "orfao-tmux-pane.json"), "utf8")) as Array<{
      process: BusyProcessRow; parents: BusyProcessRow[]; tmuxPane: string; paneSessionNow: string;
    }>;
    const o = orphans.find((x) => x.paneSessionNow === "qualidade-qa-s8@fabrica")!;
    expect(o.tmuxPane).toBe("%93");
    const panePid = 4_000_000; // the seat's pane: the orphan is not among its descendants
    const rows: BusyProcessRow[] = [{ pid: panePid, ppid: 1, command: "/bin/bash -l" }, { pid: panePid + 1, ppid: panePid, command: "claude" },
      o.process, ...o.parents];
    expect(seatPoolBusyProcess(rows, panePid, PROGRAMS)).toBeNull();
    const env = new Map(orphans.map((x) => [x.process.pid, x.tmuxPane]));
    const read = await readSeatPoolBusySessions(["qualidade-qa-s8@fabrica", "other@r"], PROGRAMS, {
      processes: async () => rows,
      panes: async () => [{ paneId: "%93", panePid, session: "qualidade-qa-s8@fabrica", active: true }, { paneId: "%7", panePid, session: "other@r", active: true }],
      paneOf: (s) => ({ paneId: s === "qualidade-qa-s8@fabrica" ? "%93" : "%7", tmuxSession: s }), processPane: async (pid) => env.get(pid) ?? null,
    });
    expect(read.get("qualidade-qa-s8@fabrica")).toBe(`suite-flock (pid ${o.process.pid}, TMUX_PANE %93)`);
    expect(read.get("other@r")).toBeNull();
  });

  it("a read that never answers counts as failed after the timeout, and holds the seat", async () => {
    const never = new Promise<never>(() => {});
    const paneOf = (s: string) => ({ paneId: "%1", tmuxSession: s });
    const panes = async () => [{ paneId: "%1", panePid: 1, session: "a@r", active: true }];
    const slowTable = await readSeatPoolBusySessions(["a@r"], PROGRAMS, { processes: () => never, panes, paneOf }, 20);
    expect(slowTable.get("a@r")).toBe("process table not read: the process table took over 20 ms");
    const slowPane = await readSeatPoolBusySessions(["a@r"], PROGRAMS, { processes: async () => [], panes: () => never, paneOf }, 20);
    expect(slowPane.get("a@r")).toBe("panes not read: the tmux panes took over 20 ms");
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The daemon's own wiring (the e-c1 harness): reader, planner, executor recheck, demand command
// ---------------------------------------------------------------------------------------------------------------
describe("T-006090: the executor (real queue, reader and history)", () => {
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

  it("the case of T-006052: a slot idle at its prompt with its only row parked on a congelar gate keeps no 2nd row; the row waits at the model", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    // The spec is valid and on: the pool plans, it does not hold on a bad spec.
    expect(h.spec().dispatchToParked).toMatchObject({ refuseGates: GATES, busyProcesses: PROGRAMS });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await h.park(p, { gate: "external:kro1v6-congelar-eval" });
    expect(h.state().slots[0]!.parked).toMatchObject({ blockedOn: "external:kro1v6-congelar-eval", timerArmedLive: true, parkActor: p.session });
    const b = await h.row(MODEL);
    await h.tickAt(61);
    await h.tickAt(200);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("control of the case above: the same slot and gate with `dispatch_to_parked: {}` takes the row (the loop of T-006052)", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "idle");
    await h.park(p, { gate: "external:kro1v6-congelar-eval" });
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toEqual([expect.objectContaining({ qitemId: b, to: p.session, mode: "parked" })]);
  });

  it("the same slot parked on a CI gate takes the row", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p, { gate: "external:ci-43107111549" });
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toEqual([expect.objectContaining({ qitemId: b, to: p.session, mode: "parked" })]);
  });

  it("the reader carries busyProcess only with busy_processes in the spec", async () => {
    h.register({ ...BASE, max: 1, ...ON });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p, { gate: "external:ci-x" });
    h.busy.set(p.session, "suite-flock (pid 5)");
    expect(h.state().slots[0]).not.toHaveProperty("busyProcess");
    expect(seatPoolCanReceive(h.spec(), h.state().slots[0]!, h.nowMs)).toBe("parked");
  });

  it("a busy process seen by the plan holds the seat back; the row waits", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p, { gate: "external:ci-x" });
    h.busy.set(p.session, "suite-flock (pid 5)");
    expect(h.state().slots[0]).toMatchObject({ busyProcess: "suite-flock (pid 5)" });
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b).destinationSession).toBe(MODEL);
  });

  it("recheck: a suite that starts between the plan and the move skips the row with parked_busy; the row stays at the model", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    const p = h.poolSeat("dev.impl-s1", "background");
    const a = await h.park(p, { gate: "external:ci-x" });
    h.nowMs = T0 - 120_000;
    const b = await h.row(MODEL);
    h.nowMs = T0;
    const plan = planSeatPool(h.spec(), h.state(), h.nowMs, 120_000);
    const intent = dispatchesOf(plan.intents)[0]!;
    expect(intent).toMatchObject({ qitemId: b, slotId: p.slotId, mode: "parked" });
    // The read the executor makes just before the move is the only one that sees the suite.
    h.busy.set(p.session, "suite-flock (pid 42)");
    const before = h.busyReads;
    await h.submit([intent]);
    expect(h.busyReads).toBe(before + 1);
    expect(h.notes(SKIPPED)).toEqual([{ qitemId: b, from: MODEL, to: p.session, code: "parked_busy", busyProcess: "suite-flock (pid 42)" }]);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.get(a)).toMatchObject({ state: "blocked", blockedOn: "external:ci-x" });
  });

  it("recheck: a process read that throws holds the parked seat back (parked_busy), and since T-006251 the free one too (free_busy)", async () => {
    h.register({ ...BASE, max: 2, ...HEAVY });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p, { gate: "external:ci-x" });
    h.nowMs = T0 - 120_000;
    const b = await h.row(MODEL);
    h.nowMs = T0;
    const intent = dispatchesOf(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    h.busyThrows = true;
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toEqual([expect.objectContaining({ code: "parked_busy", busyProcess: "busy processes not read: no /proc" })]);
    expect(h.get(b).destinationSession).toBe(MODEL);
    // A free seat's move reads the processes too (T-006251, B1), and a read that throws holds it back.
    const before = h.busyReads;
    const f = h.poolSeat("dev.impl-s2", "idle");
    await h.submit([{ kind: "dispatch", qitemId: b, from: MODEL, slotId: f.slotId }]);
    expect(h.busyReads).toBe(before + 1);
    // Both entries carry the same instant, so they are looked up by code, not by order.
    expect(h.notes(SKIPPED).filter((n) => n["code"] === "free_busy"))
      .toEqual([{ qitemId: b, from: MODEL, to: f.session, code: "free_busy", busyProcess: "busy processes not read: no /proc" }]);
    expect(h.get(b).destinationSession).toBe(MODEL);
    h.busyThrows = false;
    await h.submit([{ kind: "dispatch", qitemId: b, from: MODEL, slotId: f.slotId }]);
    expect(h.get(b).destinationSession).toBe(f.session);
  });

  it("recheck: a parked seat the read left out counts as not read and keeps nothing", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p, { gate: "external:ci-x" });
    h.nowMs = T0 - 120_000;
    const b = await h.row(MODEL);
    h.nowMs = T0;
    const intent = dispatchesOf(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    h.busyLeftOut.add(p.session);
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toEqual([expect.objectContaining({ code: "parked_busy", busyProcess: "busy processes not read for this seat" })]);
    expect(h.get(b).destinationSession).toBe(MODEL);
  });

  it("two parked moves and the demand command in one run read the processes each time (no cache in the run)", async () => {
    h.register({ ...BASE, max: 2, ...HEAVY, demand_cmd: { argv: ["demand"] } });
    const p1 = h.poolSeat("dev.impl-s1", "background");
    await h.park(p1, { gate: "external:ci-a" });
    const p2 = h.poolSeat("dev.impl-s2", "background");
    await h.park(p2, { gate: "external:ci-b" });
    h.busy.set(p1.session, null);
    h.busy.set(p2.session, null);
    h.nowMs = T0 - 120_000;
    await h.row(MODEL);
    h.nowMs = T0 - 119_000;
    await h.row(MODEL);
    h.nowMs = T0;
    const before = h.busyReads;
    await h.tickAt(1);
    expect(h.notes(DISPATCHED)).toHaveLength(2);
    expect(h.busyReads - before).toBe(3);
  });

  it("without busy_processes no process is read, before a move or the demand command", async () => {
    h.register({ ...BASE, max: 1, ...ON, demand_cmd: { argv: ["demand"] } });
    const p = h.poolSeat("dev.impl-s1", "background");
    await h.park(p, { gate: "external:ci-x" });
    await h.row(MODEL);
    await h.tickAt(61);
    expect(h.notes(DISPATCHED)).toHaveLength(1);
    expect(h.busyReads).toBe(0);
  });

  it("the demand command's receives says null for a busy parked seat and parked for one waiting on CI", async () => {
    h.register({ ...BASE, max: 2, ...HEAVY, demand_cmd: { argv: ["demand"] } });
    const busy = h.poolSeat("dev.impl-s1", "background");
    await h.park(busy, { gate: "external:ci-a" });
    const ci = h.poolSeat("dev.impl-s2", "background");
    await h.park(ci, { gate: "external:ci-b" });
    h.busy.set(busy.session, "congelar-eval (pid 8)");
    h.busy.set(ci.session, null);
    await h.tickAt(1);
    const last = h.demandSlots.at(-1)!;
    expect(last.find((s) => s.session === busy.session)?.receives).toBeNull();
    expect(last.find((s) => s.session === ci.session)?.receives).toBe("parked");
    expect(h.busyReads).toBeGreaterThan(0);
    // A read that throws says null for every parked seat in the demand command's slots.
    h.busyThrows = true;
    await h.tickAt(40);
    const failed = h.demandSlots.at(-1)!;
    expect(failed.find((s) => s.session === ci.session)?.receives).toBeNull();
  });
});

// ---------------------------------------------------------------------------------------------------------------
// T-006251 (B1 of REVISAO-T-006182): a free seat (no open row) with heavy local work its runtime does not track
// ---------------------------------------------------------------------------------------------------------------
describe("T-006251: a free seat with an orphaned suite-flock takes no row (B1)", () => {
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

  it("the negative case: the orphan outlives the seat's last row; idle at its prompt with no row, the seat takes no tarefa-nova", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    const p = h.poolSeat("dev.impl-s1", "background");
    const a = await h.park(p, { gate: "external:ci-x" });
    // The seat started a suite-flock with nohup and closed its only row: the shell it ran in is gone, so its runtime
    // tracks no background work, and the seat is free (idle at its prompt, no open row) with the suite still queued.
    h.busy.set(p.session, "suite-flock (pid 781461, TMUX_PANE %24)");
    h.queue.update({ qitemId: a, actorSession: p.session, state: "done", closureReason: "no-follow-on", transitionNote: "done" } as never);
    h.setMode(p, "idle");
    const free = h.state().slots[0]!;
    expect(free).toMatchObject({ openRows: 0, cause: "idle", busyProcess: "suite-flock (pid 781461, TMUX_PANE %24)" });
    expect(seatPoolCanReceive(h.spec(), free, h.nowMs)).toBeNull();
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.events(DISPATCHED)).toHaveLength(0);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // The suite ends: the next plan gives the row to the seat.
    h.busy.set(p.session, null);
    await h.tickAt(120);
    expect(h.notes(DISPATCHED)).toEqual([expect.objectContaining({ qitemId: b, to: p.session })]);
    expect(h.get(b).destinationSession).toBe(p.session);
  });

  it("recheck: a suite orphaned between the plan and the move skips the free row with free_busy; the row stays at the model", async () => {
    h.register({ ...BASE, max: 1, ...HEAVY });
    const f = h.poolSeat("dev.impl-s1", "idle");
    h.nowMs = T0 - 120_000;
    const b = await h.row(MODEL);
    h.nowMs = T0;
    const intent = dispatchesOf(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    expect(intent).toMatchObject({ qitemId: b, slotId: f.slotId });
    expect(intent).not.toHaveProperty("mode", "parked");
    h.busy.set(f.session, "congelar-eval (pid 77, TMUX_PANE %9)");
    await h.submit([intent]);
    expect(h.notes(SKIPPED)).toEqual([{ qitemId: b, from: MODEL, to: f.session, code: "free_busy", busyProcess: "congelar-eval (pid 77, TMUX_PANE %9)" }]);
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("without busy_processes a free seat's move and plan read no processes, and a busy one still receives", async () => {
    h.register({ ...BASE, max: 1, dispatch_to_parked: { refuse_gates: GATES } });
    const f = h.poolSeat("dev.impl-s1", "idle");
    h.busy.set(f.session, "suite-flock (pid 5)");
    expect(h.state().slots[0]).not.toHaveProperty("busyProcess");
    const b = await h.row(MODEL);
    await h.tickAt(61);
    expect(h.busyReads).toBe(0);
    expect(h.get(b).destinationSession).toBe(f.session);
  });

  it("the demand command's receives says null for a free seat with a busy process, free for one without", async () => {
    h.register({ ...BASE, max: 2, ...HEAVY, demand_cmd: { argv: ["demand"] } });
    const busy = h.poolSeat("dev.impl-s1", "idle");
    const quiet = h.poolSeat("dev.impl-s2", "idle");
    h.busy.set(busy.session, "suite-flock (pid 8)");
    h.busy.set(quiet.session, null);
    await h.tickAt(1);
    const last = h.demandSlots.at(-1)!;
    expect(last.find((x) => x.session === busy.session)?.receives).toBeNull();
    expect(last.find((x) => x.session === quiet.session)?.receives).toBe("free");
  });
});

describe("T-006090: the daemon's wiring (startup.ts, read as text as seat-pool-c1c-wiring.test.ts does)", () => {
  const startup = readFileSync(join(HERE, "..", "src", "startup.ts"), "utf8");
  it("the reader gets the busy map, the planner reads it after the usage, and the executor reads it again", () => {
    expect(startup).toContain("busyProcess: busy ? (sessionName) => busy.get(sessionName) : undefined,");
    expect(startup).toContain("return readPoolState(job, usageLimited, await busyPoolSessions(job, usageLimited));");
    expect(startup).toContain("busySessions: async (job) => (await busyPoolSessions(job)) ?? new Map(),");
    // T-006251: the seats that could take a row (free or parked, readSeatPoolBusyCandidates), the panes in one read and
    // the environment off the event loop.
    expect(startup).toContain("return readSeatPoolBusyCandidates(job.spec, readPoolState(job, usageLimited).slots, Date.now(), {");
    expect(startup).toContain("panes: () => tmuxAdapter.listAllPanePids(),");
    expect(startup).toContain("processPane: processTmuxPane,");
    expect(startup).toContain("readFile: (path: string) => fs.promises.readFile(path, \"latin1\"),");
    expect(startup).not.toContain("readFileSync(`/proc/${pid}/environ`");
    // The executor's own reader has no busy map: its recheck reads through busySessions.
    expect(startup).toMatch(/new SeatPoolExecutor\(\{[\s\S]*?readState: readPoolState,[\s\S]*?busySessions: async \(job\) =>/);
  });
});
