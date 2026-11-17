// Frozen eval of T-004870 (F3 of T-004860), v2 (r2.1): a seat pool's drain ends at a deadline, and its rows go back to
// the model. It replaces the v1 file of the same name (eval/f3, SHA256SUMS 9cf19ccd).
// Contract: ARQUITETURA-F3.md, section 3 (cases 1-10), the held-messages case of section 2.2 (today's behaviour), r1
// (rule 3: a pruned expired entry makes the stop refuse again) and r2 as amended by r2.1 (the release command is told
// why it is asked, OPENRIG_SEAT_POOL_RELEASE_REASON = idle | drain-deadline | reap; the adapter lets only pending and
// blocked rows go at the deadline; exit 3, 5 and a timeout are retried up to 10 min after the deadline; S2 and S3).
//
// The harness is the daemon's own wiring with only the terminal side faked: the production schema (ALL_MIGRATIONS),
// the real queue (QueueRepository through seatPoolQueueEffects), the real pool rows (SeatPoolSlots), the real state
// reader (readSeatPoolState), the real planner and policy (makeSeatPoolPolicy), the real executor (SeatPoolExecutor), the
// real watchdog history (WatchdogHistoryLog) and the real watchdog routes (stop and show). Faked: tmux (stop, clean,
// launch), the node removal (refused while work is still addressed to the seat, as removeNode is without a fallback),
// the pool's commands and the clock. A "restart" drops the executor and the policy and builds new ones on the same
// database, as a daemon restart does.
//
// The release command is never answered 0 by default. Its answer is the real recibos.py's (ops/autoscale/recibos.py,
// billverso), read from the real captures copied next to this file (fixtures/qa-t4870-f3/recibos-real/, byte for byte
// from eval/f3.qa/v2r2/real/, 2026-11-10 10:16Z and 10:27Z): the `falhas` list, built the way recibos.py builds it
// (receipt 1, processes: the real "processo no pgid …" entry, or "pgid não informado …" when the seat has no pane;
// receipt 2, the queue: "linha na fila: <id> (<state>), …" from the seat's open rows; receipt 3, the ledger: the real
// "execução em_execucao: …" entry), decides it. Exit 0 when the list is empty; with RELEASE_REASON exactly
// "drain-deadline", also when only pending and blocked items of the queue entry are left out of it (the f3-par2 rule);
// otherwise exit 1 with the real stdout. Each call is recorded with its RELEASE_REASON. The harness is checked against
// the captures first (describe "the real recibos.py").
//
// What no capture shows (r2.1 item 1, real of 2026-11-10 for the refusing mode): no real slot fails on the queue receipt
// alone, and no exit 0 of recibos.py was captured. On an accepted release, receipts 1 (processes) and 4 (git, not
// modeled) hold by the harness's default (a live seat given no `receipts`). The second mode of real/ (the adapter with
// the f3-par2 rule) is captured again once f3-par2 is on the billverso main and ff-vm.sh ran.
//
// The golden case 1 also reads the real build-dev spec (drain off, then the drain spec), and the real columns of
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
const fixtureText = (name: string): string => readFileSync(new URL(`./fixtures/qa-t4870-f3/${name}`, import.meta.url), "utf8");
const fixture = (name: string): unknown => JSON.parse(fixtureText(name));
type RealSpec = { policy: string; context: Record<string, unknown> };
const REAL_SPEC = fixture("spec-seat-pool-build-dev.json") as RealSpec;
const REAL_DRAIN_SPEC = fixture("spec-seat-pool-build-dev-drain.json") as RealSpec;
type RealColumn = { name: string };
const REAL_SHAPE = fixture("forma-depois-troca.json") as {
  tables: Record<"seat_pool_slots" | "watchdog_history" | "watchdog_jobs", { columns: RealColumn[] }>;
  example_history_row_shape: { evaluated_at: string; evaluation_notes: { type: string } };
};

/** The real recibos.py runs, from the capture files: the slot, its open rows then, the stdout line and the exit. */
interface RealRelease { file: string; slot: string; openRows: number; line: string; falhas: string[]; ok: boolean; exit: number }
const REAL_FILES = [
  "recibos-real-revisor2-20261110T1016Z.txt",
  "recibos-real-qa4-sem-reason-20261110T1027Z.txt",
  "recibos-real-qa4-drain-deadline-20261110T1027Z.txt",
] as const;

function readCaptures(): RealRelease[] {
  const out: RealRelease[] = [];
  for (const file of REAL_FILES) {
    const lines = fixtureText(`recibos-real/${file}`).split("\n");
    for (let i = 0; i < lines.length; i++) {
      const head = /^## slot (\S+); linhas abertas: (\d+)$/.exec(lines[i]!);
      if (!head) continue;
      const line = lines[i + 1]!;
      const exit = /^exit=(\d+)$/.exec(lines[i + 2]!);
      if (!line.startsWith("{") || !exit) throw new Error(`${file}: not the capture's form after ${lines[i]}`);
      const parsed = JSON.parse(line) as { falhas: string[]; ok: boolean; slot: string };
      out.push({ file, slot: head[1]!, openRows: Number(head[2]), line, falhas: parsed.falhas, ok: parsed.ok, exit: Number(exit[1]) });
    }
  }
  return out;
}
const REAL_RELEASES = readCaptures();
const realRun = (file: (typeof REAL_FILES)[number], slot: string): RealRelease => {
  const found = REAL_RELEASES.find((r) => r.file === file && r.slot === slot);
  if (!found) throw new Error(`no capture of ${slot} in ${file}`);
  return found;
};
const realEntry = (run: RealRelease, prefix: string): string => {
  const found = run.falhas.find((f) => f.startsWith(prefix));
  if (!found) throw new Error(`no "${prefix}" entry for ${run.slot} in ${run.file}`);
  return found;
};
/** Receipt 3, the ledger, as captured (build-dev-10, 10:16Z): a tarefa em_execucao whose executor is the slot. */
const REAL_EXECUTION = realEntry(realRun(REAL_FILES[0], "build-dev-10@fabrica"), "execução em_execucao: ");
/** Receipt 1, processes, as captured (build-dev-12, 10:27Z): a process below the seat's runtime. */
const REAL_PROCESS = realEntry(realRun(REAL_FILES[1], "build-dev-12@fabrica"), "processo no pgid ");
/** Receipt 1 when the seat has no pane: recibos.py:230 at 672fa14f (the seat's session gone; not in a capture). */
const NO_PANE = "pgid não informado (--pid ou --pgid): processos não conferidos";

// ---------------------------------------------------------------------------------------------------------------
// recibos.py's answer (f3-par2 rule, r2.1), as a pure function over the capture's `falhas`.
// ---------------------------------------------------------------------------------------------------------------
const REASON_ENV = "OPENRIG_SEAT_POOL_RELEASE_REASON";
const SLOT_ENV = "OPENRIG_SEAT_POOL_SLOT";
const DRAIN_DEADLINE = "drain-deadline";
const QUEUE_PREFIX = "linha na fila: ";
type QueueItem = { qitemId: string; state: string };

/** The items of a "linha na fila" entry, or null for another receipt's entry. Throws on an item not in the real form. */
function queueItems(entry: string): QueueItem[] | null {
  if (!entry.startsWith(QUEUE_PREFIX)) return null;
  return entry.slice(QUEUE_PREFIX.length).split(", ").map((part) => {
    const m = /^(\S+) \((pending|in-progress|blocked)\)$/.exec(part);
    if (!m) throw new Error(`not the real form of a queue item: ${part}`);
    return { qitemId: m[1]!, state: m[2]! };
  });
}
const queueEntry = (items: readonly QueueItem[]): string => QUEUE_PREFIX + items.map((r) => `${r.qitemId} (${r.state})`).join(", ");

/** json.dumps(obj, ensure_ascii=False, sort_keys=True), as comum.emitir prints it. */
function pyDumps(obj: Record<string, unknown>): string {
  const value = (x: unknown): string => (Array.isArray(x) ? `[${x.map(value).join(", ")}]` : JSON.stringify(x));
  return `{${Object.keys(obj).sort().map((k) => `${JSON.stringify(k)}: ${value(obj[k])}`).join(", ")}}`;
}

interface Answer { exitCode: number | null; stdout: string; error?: string }

/** What recibos.py (with the f3-par2 rule) answers for a slot whose receipts failed with `falhas`. */
function recibosAnswer(slot: string, falhas: readonly string[], reason: string | undefined): Answer {
  if (falhas.length === 0) return { exitCode: 0, stdout: pyDumps({ falhas: [], ok: true, slot }) };
  if (reason === DRAIN_DEADLINE) {
    const toModel: string[] = [];
    const kept: string[] = [];
    for (const entry of falhas) {
      const items = queueItems(entry);
      if (!items) { kept.push(entry); continue; }
      toModel.push(...items.filter((r) => r.state !== "in-progress").map((r) => r.qitemId));
      if (items.some((r) => r.state === "in-progress")) kept.push(entry);
    }
    if (kept.length === 0) return { exitCode: 0, stdout: pyDumps({ falhas: [], linhas_ao_modelo: toModel, ok: true, slot }) };
  }
  return { exitCode: 1, stdout: pyDumps({ falhas: [...falhas], ok: false, slot }) };
}

// ---------------------------------------------------------------------------------------------------------------
// Names fixed by the contract (NOTAS.md, "API mínima"; r2.1).
// ---------------------------------------------------------------------------------------------------------------
const DRAIN_STARTED = "seat_pool_drain_started";
const DRAIN_ENDED = "seat_pool_drain_ended";
const DRAIN_EXPIRED = "seat_pool_drain_expired";
const SLOT_LEFT = "seat_pool_left";
const DEFAULT_DEADLINE_S = 900;
/** r2.1 item 4: exit 3, 5 and a timeout are tried again for up to 10 minutes after the deadline. */
const UNAVAILABLE_WINDOW_S = 600;

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
const RELEASE = { argv: ["release"] };
const REAL_DEADLINE_S = (REAL_DRAIN_SPEC.context["drain_deadline_s"] as number | undefined) ?? DEFAULT_DEADLINE_S;

type Activity = "idle" | "working";
interface Seat { logicalId: string; session: string; nodeId: string }
interface PoolSeat extends Seat { slotId: number }
interface ReleaseCall extends Answer { slot: string; reason: string | undefined; at: number }

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
  /** Returns true when a pool command OTHER than release_cmd must exit 1 (release_cmd answers as recibos.py). */
  refuse: (argv: string[], env: Record<string, string>) => boolean = () => false;
  /** Receipts 1 and 3 that fail for a seat, by session name: real entries from the capture (REAL_PROCESS, REAL_EXECUTION). */
  readonly receipts = new Map<string, string[]>();
  /** An answer of the release command that is not a receipt (exit 2, 3, 5 or a timeout), by call; null: recibos.py. */
  releaseExit: ((call: { slot: string; reason: string | undefined; n: number }) => Answer | null) | null = null;
  /** Every call of release_cmd, with the RELEASE_REASON it was given and what it answered. */
  releaseCalls: ReleaseCall[] = [];
  /** Session names whose stop fails. */
  readonly stopFails = new Set<string>();
  /** Logical ids whose node removal fails. */
  readonly removeFails = new Set<string>();
  /** The session id each stop was given as `expected`, by session name. */
  readonly stopExpected = new Map<string, string | null>();
  /** Runs inside a pool command, before it exits (a release that relaunches the seat, for one). */
  duringCommand: ((argv: string[], env: Record<string, string>) => void) | null = null;
  /** Runs once, right after the first row the pool moves has moved. */
  afterFirstMove: (() => void) | null = null;
  /** Runs when a seat's lifecycle lease is taken, before the work under it starts. */
  beforeLease: ((logicalId: string) => void) | null = null;
  /** Decides, before each row move the pool makes (n: the moves made so far), whether it fails with a code no
   *  ROW_MOVED_ON covers: a failure after the stop (CETICO v2r2 S1). */
  moveFails: ((qitemId: string, n: number) => boolean) | null = null;
  private moves = 0;
  /** Every pool command run, with its environment: only release_cmd is told RELEASE_REASON (r2 §1; CETICO v2r2 N4). */
  commandEnvs: Array<{ argv: string[]; env: Record<string, string> }> = [];
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

  /** The seat's session ends by itself (it exited, or its tmux session is gone). */
  endSession(seat: Seat): void {
    this.db.prepare("UPDATE sessions SET status = 'exited' WHERE node_id = ?").run(seat.nodeId);
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

  // ---- the release command, as recibos.py ------------------------------------------------------------------------
  /** The receipts of the slot that fail now, in recibos.py's order: processes, the queue, the ledger (git not modeled). */
  recibosFalhas(slot: string): string[] {
    const session = this.db.prepare("SELECT status FROM sessions WHERE session_name = ? ORDER BY rowid DESC LIMIT 1").get(slot) as
      | { status: string } | undefined;
    const given = this.receipts.get(slot) ?? [];
    const falhas: string[] = [];
    // No pane (the session is gone): recibos.py has no pid, and says the processes were not checked.
    if (session?.status !== "running") falhas.push(NO_PANE);
    else falhas.push(...given.filter((f) => !f.startsWith("execução ")));
    const rows = this.queue.listOpenObligations(slot, 500);
    if (rows.length > 0) falhas.push(queueEntry(rows));
    falhas.push(...given.filter((f) => f.startsWith("execução ")));
    return falhas;
  }

  isRelease(argv: string[]): boolean {
    const release = this.context()["release_cmd"] as { argv?: string[] } | undefined;
    return !!release?.argv && release.argv.join("\u0000") === argv.join("\u0000");
  }

  release(env: Record<string, string>): Answer {
    const slot = env[SLOT_ENV] ?? "";
    const reason = env[REASON_ENV];
    const n = this.releaseCalls.filter((c) => c.slot === slot).length;
    const answer = this.releaseExit?.({ slot, reason, n }) ?? recibosAnswer(slot, this.recibosFalhas(slot), reason);
    this.releaseCalls.push({ slot, reason, at: this.nowMs, ...answer });
    return answer;
  }

  releasesOf(seat: Seat): ReleaseCall[] {
    return this.releaseCalls.filter((c) => c.slot === seat.session);
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
        const n = this.moves++;
        if (this.moveFails?.(input.qitemId, n)) return { ok: false, code: "eval_move_refused", error: "the eval refuses this move" };
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
        try {
          this.beforeLease?.(logical);
          return await fn();
        } finally { inLease = outer; this.calls.push(`released ${logical}`); }
      },
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: this.latestSession(nodeId)?.id ?? null }),
      stopSlot: async ({ sessionName, expected }) => {
        this.calls.push(`stop ${sessionName}${inLease ? ` [lease ${inLease}]` : ""}`);
        this.stopExpected.set(sessionName, expected.sessionId);
        if (this.stopFails.has(sessionName)) return { ok: false, code: "tmux_probe_failed", error: "the eval refuses this stop" };
        // A session that is not running any more: the checked stop says so, which the executor takes as stopped.
        if (!this.sessionRunning(expected.nodeId)) return { ok: false, code: "session_not_live", error: "the session is not running" };
        this.db.prepare("UPDATE sessions SET status = 'exited' WHERE node_id = ?").run(expected.nodeId);
        return { ok: true };
      },
      removeSlot: async ({ nodeId }) => {
        const logical = logicalOf(nodeId);
        this.calls.push(`remove ${logical ?? nodeId}`);
        if (!logical) return { ok: false, code: "node_not_found", error: "the node is gone" };
        if (this.removeFails.has(logical)) return { ok: false, code: "eval_remove_refused", error: "the eval refuses this removal" };
        // As rigLifecycleService.removeNode without a fallback destination: work addressed to the seat keeps the node.
        if (this.queue.listOpenObligations(seatSessionName(logical, RIG), 10).length > 0) {
          return { ok: false, code: "active_qitems", error: `${logical} still has open rows` };
        }
        this.db.prepare("DELETE FROM sessions WHERE node_id = ?").run(nodeId);
        this.rigRepo.deleteNode(nodeId);
        return { ok: true };
      },
      runCommand: async (command, env, signal) => {
        this.calls.push(`run ${command.argv.join(" ")}${env[SLOT_ENV] ? ` @${env[SLOT_ENV]}` : ""}`);
        this.commandEnvs.push({ argv: [...command.argv], env: { ...env } });
        if (signal.aborted) return { exitCode: null, stdout: "", error: "aborted" };
        this.duringCommand?.(command.argv, env);
        // release_cmd answers as the real recibos.py, never 0 by default.
        if (this.isRelease(command.argv)) return this.release(env);
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

  stopped(seat: Seat): boolean {
    return this.calls.some((c) => c === `stop ${seat.session}` || c.startsWith(`stop ${seat.session} `));
  }
}

type LeftItem = { slot: string; why: string; exitCode?: number | null };

/** The single expired event of the current drain, with its lists. */
function expiredOf(h: Harness): { left: LeftItem[]; moved: Array<{ qitemId: string; to: string }> } {
  const expired = h.events(DRAIN_EXPIRED);
  expect(expired).toHaveLength(1);
  const notes = expired[0]!.evaluationNotes as { left?: unknown; moved?: unknown };
  expect(Array.isArray(notes.left)).toBe(true);
  expect(Array.isArray(notes.moved)).toBe(true);
  return { left: notes.left as LeftItem[], moved: notes.moved as Array<{ qitemId: string; to: string }> };
}

/** Each expected seat is in `left` with its why and, when given, the release's exit code in that pass (r2.1: each item). */
function expectLeft(left: LeftItem[], expected: Array<{ seat: Seat; why: string; exitCode?: number | null }>): void {
  expect(left).toHaveLength(expected.length);
  for (const { seat, why, exitCode } of expected) {
    const entry = left.find((l) => refsOf(seat).includes(l.slot));
    expect(entry, `left has ${seat.logicalId}`).toBeDefined();
    expect(entry!.why).toBe(why);
    if (exitCode !== undefined) expect(entry, `left ${seat.logicalId} carries the exit code`).toHaveProperty("exitCode", exitCode);
  }
}

function deadlineOf(entry: WatchdogHistoryEntry): number {
  return Date.parse(String((entry.evaluationNotes as { deadlineAt?: unknown } | null)?.deadlineAt));
}

// ---------------------------------------------------------------------------------------------------------------
// The harness against the real capture: the release command answers as recibos.py, not as a stub.
// ---------------------------------------------------------------------------------------------------------------
describe("qa T-004870 F3 v2: the real recibos.py, as the harness answers for release_cmd", () => {
  it("reads every captured run (2 slots per file), and each stdout is recibos.py's json.dumps, byte for byte", () => {
    expect(REAL_RELEASES.map((r) => `${r.file}:${r.slot}`)).toEqual([
      `${REAL_FILES[0]}:build-dev-10@fabrica`, `${REAL_FILES[0]}:build-dev-11@fabrica`,
      `${REAL_FILES[1]}:build-dev-12@fabrica`, `${REAL_FILES[1]}:build-dev-2@fabrica`,
      `${REAL_FILES[2]}:build-dev-12@fabrica`, `${REAL_FILES[2]}:build-dev-2@fabrica`,
    ]);
    for (const run of REAL_RELEASES) {
      expect(pyDumps(JSON.parse(run.line) as Record<string, unknown>), run.slot).toBe(run.line);
      expect(run.ok).toBe(false);
      expect(run.exit).toBe(1);
    }
  });

  it("parses the real 'linha na fila' entry: one item per open row the capture counted, rendered back to the same bytes", () => {
    for (const run of REAL_RELEASES) {
      const entries = run.falhas.filter((f) => f.startsWith(QUEUE_PREFIX));
      expect(entries, run.slot).toHaveLength(1);
      const items = queueItems(entries[0]!)!;
      expect(items, `${run.file}:${run.slot}`).toHaveLength(run.openRows);
      expect(queueEntry(items)).toBe(entries[0]);
    }
  });

  it("refuses every captured slot in both modes with the captured stdout: each one fails another receipt too (2 of 2 at 10:16Z)", () => {
    for (const run of REAL_RELEASES) {
      for (const reason of [undefined, DRAIN_DEADLINE]) {
        expect(recibosAnswer(run.slot, run.falhas, reason), `${run.file}:${run.slot} ${reason}`).toEqual({ exitCode: 1, stdout: run.line });
      }
    }
    // Today's adapter ignores the variable: the drain-deadline capture is the plain one but for an ephemeral pid.
    const without = REAL_RELEASES.filter((r) => r.file === REAL_FILES[1]);
    const withReason = REAL_RELEASES.filter((r) => r.file === REAL_FILES[2]);
    for (const [i, run] of without.entries()) {
      expect(withReason[i]!.falhas.filter((f) => !f.startsWith("processo "))).toEqual(run.falhas.filter((f) => !f.startsWith("processo ")));
    }
  });

  it("lets only pending and blocked items go, and only with RELEASE_REASON exactly drain-deadline (no real slot fails on the queue alone)", () => {
    // build-dev-12's real queue entry holds only blocked and pending rows; build-dev-11's also holds in-progress rows.
    const onlyWaiting = realEntry(realRun(REAL_FILES[1], "build-dev-12@fabrica"), QUEUE_PREFIX);
    const withClaimed = realEntry(realRun(REAL_FILES[0], "build-dev-11@fabrica"), QUEUE_PREFIX);
    const slot = "build-dev-12@fabrica";
    const accepted = recibosAnswer(slot, [onlyWaiting], DRAIN_DEADLINE);
    expect(accepted.exitCode).toBe(0);
    expect(JSON.parse(accepted.stdout)).toEqual({ falhas: [], linhas_ao_modelo: queueItems(onlyWaiting)!.map((r) => r.qitemId), ok: true, slot });
    expect(recibosAnswer(slot, [withClaimed], DRAIN_DEADLINE).exitCode).toBe(1);
    expect(recibosAnswer(slot, [onlyWaiting, REAL_EXECUTION], DRAIN_DEADLINE).exitCode).toBe(1);
    expect(recibosAnswer(slot, [REAL_PROCESS, onlyWaiting], DRAIN_DEADLINE).exitCode).toBe(1);
    for (const other of [undefined, "", "idle", "reap", "DRAIN-DEADLINE", "drain-deadline ", "drain_deadline", "drain"]) {
      expect(recibosAnswer(slot, [onlyWaiting], other).exitCode, `reason ${JSON.stringify(other)}`).toBe(1);
    }
    // A free slot (every receipt holds) goes whatever the reason, as recibos.py does today.
    for (const reason of [undefined, "idle", "reap", DRAIN_DEADLINE]) expect(recibosAnswer(slot, [], reason).exitCode).toBe(0);
  });
});

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

  it("1 (golden, real build-dev spec, real recibos.py): an idle seat holding a pending row stays until the deadline and leaves at it, its release told drain-deadline; the row reaches the model whole", async () => {
    // The real spec (capture of 2026-11-10): the operator registers it with drain off, then turns the drain spec on.
    expect(REAL_SPEC.policy).toBe("seat-pool");
    expect(REAL_DRAIN_SPEC.context["drain"]).toBe(true);
    expect({ ...REAL_DRAIN_SPEC.context, drain: false }).toEqual(REAL_SPEC.context);
    const deadlineS = REAL_DEADLINE_S;
    const releaseScript = (REAL_DRAIN_SPEC.context["release_cmd"] as { argv: string[] }).argv.join(" ");
    const removeScript = (REAL_DRAIN_SPEC.context["remove_cmd"] as { argv: string[] }).argv.join(" ");
    expect(releaseScript).toContain("ops/autoscale/recibos.py");
    h.setContext(REAL_SPEC.context);
    const seat = h.poolSeat("dev.impl-s1", "idle");
    const row = await h.row(seat.session);

    h.setContext(REAL_DRAIN_SPEC.context);
    await h.tickAt(0);
    const started = h.events(DRAIN_STARTED);
    expect(started).toHaveLength(1);
    expect(deadlineOf(started[0]!)).toBe(T0 + deadlineS * 1000);

    // Before the deadline the drain is today's: an idle seat holding a row is not free, so nothing happens to it, and
    // its release command is not even asked.
    await h.tickAt(deadlineS - 1);
    expect(h.slotOpen(seat.slotId)).toBe(true);
    expect(h.sessionRunning(seat.nodeId)).toBe(true);
    expect(h.get(row).destinationSession).toBe(seat.session);
    expect(h.poolActions(seat).filter((c) => !c.includes("demanda.py"))).toEqual([]);
    expect(h.releasesOf(seat)).toEqual([]);
    expect(h.events(DRAIN_EXPIRED)).toEqual([]);

    // At the deadline: release, stop and the row move under the seat's lease, then the node removal and remove_cmd.
    h.calls = [];
    await h.tickAt(deadlineS);
    // The release was told why it was asked, and recibos.py let the pending row go to the model.
    const releases = h.releasesOf(seat);
    expect(releases.map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
    expect(JSON.parse(releases[0]!.stdout)).toEqual({ falhas: [], linhas_ao_modelo: [row], ok: true, slot: seat.session });
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
    // Only release_cmd is told why it is asked: remove_cmd (and demand_cmd) run as today (r2 §1; CETICO v2r2 N4).
    const others = h.commandEnvs.filter((c) => !h.isRelease(c.argv));
    expect(others.map((c) => c.argv.join(" "))).toContain(removeScript);
    for (const c of others) expect(c.env, c.argv.join(" ")).not.toHaveProperty(REASON_ENV);
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

    // The real shape: every production column is still there, and the drain events are ordinary history rows.
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

  it("2 (real spec, real recibos.py): at the deadline a blocked row and a pending row go to the model, the blocked one blocked; an in-progress row refuses its seat", async () => {
    h.setContext(REAL_SPEC.context);
    const waiting = h.poolSeat("dev.impl-s1", "idle");
    const claiming = h.poolSeat("dev.impl-s2", "working");
    const anchor = await h.row(LEAD);
    const blocked = await h.row(waiting.session);
    h.block(blocked, waiting, anchor);
    const pending = await h.row(waiting.session);
    const claimed = await h.row(claiming.session);
    h.claim(claimed, claiming);

    h.setContext(REAL_DRAIN_SPEC.context);
    await h.tickAt(0);
    await h.tickAt(REAL_DEADLINE_S - 1);
    expect(h.releaseCalls).toEqual([]);
    await h.tickAt(REAL_DEADLINE_S);

    // The seat holding only rows it did not take: released with drain-deadline, stopped, rows at the model.
    expect(h.releasesOf(waiting).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
    expect(h.slotOpen(waiting.slotId)).toBe(false);
    expect(h.get(blocked)).toMatchObject({ destinationSession: MODEL, state: "blocked" });
    expect(h.get(pending)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    for (const id of [blocked, pending]) {
      expect(h.fallbackNotes(id)).toHaveLength(1);
      expect(h.fallbackNotes(id)[0]).toMatch(drainNote(waiting));
      expect(h.reapCount(id)).toBe(0);
    }
    expect(h.fallbackWakes(blocked)).toEqual([]);

    // The seat with an in-progress row: recibos.py refuses it even at the deadline (r2.1 item 3).
    expect(h.releasesOf(claiming).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 1]]);
    expect(h.stopped(claiming)).toBe(false);
    expect(h.calls).not.toContain(`unclaim ${claimed}`);
    expect(h.sessionRunning(claiming.nodeId)).toBe(true);
    expect(h.get(claimed)).toMatchObject({ destinationSession: claiming.session, state: "in-progress" });
    expect(h.slotOpen(claiming.slotId)).toBe(true);

    const expired = expiredOf(h);
    expectLeft(expired.left, [{ seat: claiming, why: "release_refused", exitCode: 1 }]);
    expect(expired.moved.map((m) => m.qitemId).sort()).toEqual([blocked, pending].sort());
  });

  it("2c (no release_cmd): an in-progress row is released and moved, a blocked row arrives blocked, even from a working seat", async () => {
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
    expect(h.stopped(seat)).toBe(true);
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

  it("3r (real spec, real receipts): a seat refused by another receipt at the deadline (a tarefa em_execucao, a process, exit 2) stays alive with its rows, left at once with the exit code", async () => {
    h.setContext(REAL_SPEC.context);
    const executing = h.poolSeat("dev.impl-s1", "idle");
    const busy = h.poolSeat("dev.impl-s2", "idle");
    const misused = h.poolSeat("dev.impl-s3", "idle");
    const rows = new Map<Seat, string>();
    for (const seat of [executing, busy, misused]) rows.set(seat, await h.row(seat.session));
    h.receipts.set(executing.session, [REAL_EXECUTION]);
    h.receipts.set(busy.session, [REAL_PROCESS]);
    // Exit 2 (usage) is a refusal at once, as exit 1 is (r2.1 item 4).
    h.releaseExit = ({ slot }) => (slot === misused.session ? { exitCode: 2, stdout: "" } : null);

    h.setContext(REAL_DRAIN_SPEC.context);
    await h.tickAt(0);
    await h.tickAt(REAL_DEADLINE_S);
    for (const seat of [executing, busy, misused]) {
      expect(h.releasesOf(seat).map((c) => c.reason), seat.logicalId).toEqual([DRAIN_DEADLINE]);
      expect(h.stopped(seat), seat.logicalId).toBe(false);
      expect(h.calls).not.toContain(`remove ${seat.logicalId}`);
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.nodeExists(seat.nodeId)).toBe(true);
      expect(h.get(rows.get(seat)!)).toMatchObject({ destinationSession: seat.session, state: "pending" });
      expect(h.fallbackNotes(rows.get(seat)!)).toEqual([]);
      expect(h.slotOpen(seat.slotId)).toBe(true);
    }
    expect(h.releasesOf(executing)[0]!.exitCode).toBe(1);
    expect(JSON.parse(h.releasesOf(executing)[0]!.stdout).falhas).toContain(REAL_EXECUTION);
    expect(h.releasesOf(busy)[0]!.exitCode).toBe(1);
    expect(JSON.parse(h.releasesOf(busy)[0]!.stdout).falhas).toContain(REAL_PROCESS);
    const expired = expiredOf(h);
    expectLeft(expired.left, [
      { seat: executing, why: "release_refused", exitCode: 1 },
      { seat: busy, why: "release_refused", exitCode: 1 },
      { seat: misused, why: "release_refused", exitCode: 2 },
    ]);
    expect(expired.moved).toEqual([]);
    // A refusal is not tried again.
    await h.tickAt(REAL_DEADLINE_S + 30);
    expect(h.releaseCalls).toHaveLength(3);
    expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
  });

  it("4: a release command that refuses keeps the seat alive with its rows; a stop that fails moves nothing; both are left", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
    const refusing = h.poolSeat("dev.impl-s1", "idle");
    const unstoppable = h.poolSeat("dev.impl-s2", "working");
    const kept = await h.row(refusing.session);
    const stuck = await h.row(unstoppable.session);
    // v1 refused by fiat; v2: recibos.py refuses it by its real ledger receipt.
    h.receipts.set(refusing.session, [REAL_EXECUTION]);
    h.stopFails.add(unstoppable.session);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    // The refused seat: never stopped nor removed, its row where it was.
    expect(h.stopped(refusing)).toBe(false);
    expect(h.calls).not.toContain(`remove ${refusing.logicalId}`);
    expect(h.sessionRunning(refusing.nodeId)).toBe(true);
    expect(h.nodeExists(refusing.nodeId)).toBe(true);
    expect(h.get(kept)).toMatchObject({ destinationSession: refusing.session, state: "pending" });
    expect(h.fallbackNotes(kept)).toEqual([]);
    expect(h.slotOpen(refusing.slotId)).toBe(true);
    // The seat whose stop failed (its release let its pending row go): still there, its row not moved.
    expect(h.releasesOf(unstoppable).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
    expect(h.stopped(unstoppable)).toBe(true);
    expect(h.calls).not.toContain(`remove ${unstoppable.logicalId}`);
    expect(h.get(stuck)).toMatchObject({ destinationSession: unstoppable.session, state: "pending" });
    expect(h.fallbackNotes(stuck)).toEqual([]);
    expect(h.slotOpen(unstoppable.slotId)).toBe(true);

    const expired = expiredOf(h);
    expectLeft(expired.left, [{ seat: refusing, why: "release_refused", exitCode: 1 }, { seat: unstoppable, why: "stop_failed", exitCode: 0 }]);
    expect(expired.moved).toEqual([]);
  });

  describe("4a/4b (r2.1): the release outside the deadline", () => {
    it("4a: a free seat under drain, before the deadline, is released with idle and leaves", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([["idle", 0]]);
      expect(h.slotOpen(seat.slotId)).toBe(false);
      expect(h.nodeExists(seat.nodeId)).toBe(false);
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
    });

    it("4a: a free seat past idle_grace_s without drain (the surplus) is released with idle and leaves", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      // The first tick samples demand_cmd ({"desired": 0}); the next one drains the surplus.
      for (const at of [0, 30, 60]) await h.tickAt(at);
      expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([["idle", 0]]);
      expect(h.slotOpen(seat.slotId)).toBe(false);
      expect(h.events(DRAIN_STARTED)).toEqual([]);
    });

    it("4b: a seat holding a row is never asked for its release before the deadline, with drain or without", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      for (const at of [0, 30, 60, 3600]) await h.tickAt(at);
      h.setContext(REAL_DRAIN_SPEC.context);
      for (const at of [3700, 3700 + REAL_DEADLINE_S - 1]) await h.tickAt(at);
      expect(h.releaseCalls).toEqual([]);
      expect(h.slotOpen(seat.slotId)).toBe(true);
      expect(h.get(row).destinationSession).toBe(seat.session);
    });
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
      expect(h.stopped(seat)).toBe(false);
      const refused = await h.stop();
      expect(refused.status).toBe(409);
      expect(refused.body["error"]).toBe("pool_has_slots");
      expect(h.jobs.getByIdOrThrow(h.jobId).state).toBe("active");
    });

    it("accepts once the drain expired, retiring each seat left and listing it as still running", async () => {
      h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.receipts.set(seat.session, [REAL_EXECUTION]);
      h.setDrain(true);
      await h.tickAt(0);
      await h.tickAt(DEFAULT_DEADLINE_S);
      expectLeft(expiredOf(h).left, [{ seat, why: "release_refused", exitCode: 1 }]);

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

    it("r1 rule 3: once the expired entry is pruned, the stop refuses again, and the next tick starts a new drain", async () => {
      h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
      const seat = h.poolSeat("dev.impl-s1", "idle");
      await h.row(seat.session);
      h.receipts.set(seat.session, [REAL_EXECUTION]);
      h.setDrain(true);
      await h.tickAt(0);
      await h.tickAt(DEFAULT_DEADLINE_S);
      expectLeft(expiredOf(h).left, [{ seat, why: "release_refused", exitCode: 1 }]);

      // The retention prunes the oldest entries first: the drain's start and its expired entry go together.
      h.db.prepare("DELETE FROM watchdog_history WHERE job_id = ? AND evaluated_at <= ?").run(h.jobId, iso(h.nowMs));
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
      const refused = await h.stop();
      expect(refused.status).toBe(409);
      expect(refused.body["error"]).toBe("pool_has_slots");
      expect(h.jobs.getByIdOrThrow(h.jobId).state).toBe("active");
      expect(h.slotOpen(seat.slotId)).toBe(true);

      await h.tickAt(DEFAULT_DEADLINE_S + 30);
      const started = h.events(DRAIN_STARTED);
      expect(started).toHaveLength(1);
      expect(deadlineOf(started[0]!)).toBe(T0 + (2 * DEFAULT_DEADLINE_S + 30) * 1000);
    });
  });

  describe("r2.1 item 4: a release that did not answer (exit 3, exit 5, a timeout) is tried again, up to 10 minutes after the deadline", () => {
    const unanswered: Array<{ label: string; answer: Answer }> = [
      { label: "exit 5 (a source did not answer)", answer: { exitCode: 5, stdout: "" } },
      { label: "exit 3 (PARAR)", answer: { exitCode: 3, stdout: "" } },
      // runSeatPoolCommand's form of a release_cmd past its timeout_s.
      { label: "a timeout", answer: { exitCode: null, stdout: "", error: "timed out after 180s" } },
    ];
    for (const { label, answer } of unanswered) {
      it(`${label} at the deadline and 0 on the next tick: the seat leaves then, with one expired entry`, async () => {
        h.setContext(REAL_SPEC.context);
        const seat = h.poolSeat("dev.impl-s1", "idle");
        const row = await h.row(seat.session);
        h.releaseExit = ({ slot, n }) => (slot === seat.session && n === 0 ? answer : null);
        h.setContext(REAL_DRAIN_SPEC.context);
        await h.tickAt(0);

        await h.tickAt(REAL_DEADLINE_S);
        expect(h.releasesOf(seat).map((c) => c.reason)).toEqual([DRAIN_DEADLINE]);
        expect(h.events(DRAIN_EXPIRED)).toEqual([]);
        expect(h.stopped(seat)).toBe(false);
        expect(h.sessionRunning(seat.nodeId)).toBe(true);
        expect(h.slots.get(seat.slotId)).toMatchObject({ retiredAt: null });
        expect(h.get(row)).toMatchObject({ destinationSession: seat.session, state: "pending" });

        await h.tickAt(REAL_DEADLINE_S + 30);
        expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, answer.exitCode], [DRAIN_DEADLINE, 0]]);
        expect(h.slotOpen(seat.slotId)).toBe(false);
        expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
        const expired = expiredOf(h);
        expect(expired.left).toEqual([]);
        expect(expired.moved.map((m) => m.qitemId)).toEqual([row]);
        await h.tickAt(REAL_DEADLINE_S + 60);
        expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
      });
    }

    for (const { label, answer } of unanswered) {
      it(`${label} until 10 minutes past the deadline: the seat is left as release_unavailable, with its exit code, and the stop lists it`, async () => {
        h.setContext(REAL_SPEC.context);
        const silent = h.poolSeat("dev.impl-s1", "idle");
        const free = h.poolSeat("dev.impl-s2", "idle");
        const kept = await h.row(silent.session);
        const moved = await h.row(free.session);
        h.releaseExit = ({ slot }) => (slot === silent.session ? answer : null);
        h.setContext(REAL_DRAIN_SPEC.context);
        await h.tickAt(0);

        for (let at = REAL_DEADLINE_S; at < REAL_DEADLINE_S + UNAVAILABLE_WINDOW_S; at += 60) {
          await h.tickAt(at);
          expect(h.events(DRAIN_EXPIRED), `no expired entry at deadline + ${at - REAL_DEADLINE_S} s`).toEqual([]);
        }
        // Tried again on the ticks after the deadline, never stopped; the other seat left at the deadline.
        expect(h.releasesOf(silent).length).toBeGreaterThanOrEqual(2);
        expect(h.releasesOf(silent).every((c) => c.reason === DRAIN_DEADLINE)).toBe(true);
        expect(h.stopped(silent)).toBe(false);
        expect(h.slotOpen(free.slotId)).toBe(false);

        await h.tickAt(REAL_DEADLINE_S + UNAVAILABLE_WINDOW_S + 60);
        const expired = expiredOf(h);
        expectLeft(expired.left, [{ seat: silent, why: "release_unavailable", exitCode: answer.exitCode }]);
        // The row of the seat that left at the deadline is at the model (that `moved` keeps it across the retries is the
        // next test's, "one expired entry for the whole pass").
        expect(h.get(moved)).toMatchObject({ destinationSession: MODEL, state: "pending" });
        expect(h.sessionRunning(silent.nodeId)).toBe(true);
        expect(h.get(kept)).toMatchObject({ destinationSession: silent.session, state: "pending" });
        await h.tickAt(REAL_DEADLINE_S + UNAVAILABLE_WINDOW_S + 120);
        expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);

        const stopped = await h.stop();
        expect(stopped.status).toBe(200);
        expect(stopped.body["leftRunning"]).toEqual([expect.objectContaining({ sessionName: silent.session, why: "release_unavailable" })]);
      });
    }

    for (const restart of [false, true]) {
      it(`one expired entry for the whole pass${restart ? ", across a daemon restart within the retry window" : ""}: its moved has the rows moved at the deadline and on the retry; a seat refused or whose stop failed is left, and not asked again`, async () => {
        // §2.2 step 5: the pass records one seat_pool_drain_expired, at its end, with every row it moved and every seat
        // it left. A pass that retries a seat spans ticks: what it did on the first tick is still that pass's (CETICO
        // v2r2 S3: across a restart too, §3 case 7), and a refusal (exit 1) or a failed stop is final for the pass, so
        // that seat is not asked or stopped again on the retry (r2.1 item 4: "tenta esse slot de novo"; CETICO v2r2 S2).
        h.setContext(REAL_SPEC.context);
        const late = h.poolSeat("dev.impl-s1", "idle");
        const free = h.poolSeat("dev.impl-s2", "idle");
        const refused = h.poolSeat("dev.impl-s3", "idle");
        const unstoppable = h.poolSeat("dev.impl-s4", "idle");
        const lateRow = await h.row(late.session);
        const freeRow = await h.row(free.session);
        const refusedRow = await h.row(refused.session);
        const stuckRow = await h.row(unstoppable.session);
        h.receipts.set(refused.session, [REAL_EXECUTION]);
        h.stopFails.add(unstoppable.session);
        h.releaseExit = ({ slot, n }) => (slot === late.session && n === 0 ? { exitCode: 5, stdout: "" } : null);
        const stops = (seat: Seat) => h.calls.filter((c) => c === `stop ${seat.session}` || c.startsWith(`stop ${seat.session} `)).length;
        h.setContext(REAL_DRAIN_SPEC.context);
        await h.tickAt(0);

        await h.tickAt(REAL_DEADLINE_S);
        expect(h.slotOpen(free.slotId)).toBe(false);
        expect(h.get(freeRow)).toMatchObject({ destinationSession: MODEL, state: "pending" });
        expect(h.slotOpen(late.slotId)).toBe(true);
        expect(h.releasesOf(refused).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 1]]);
        expect(h.releasesOf(unstoppable).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
        expect(stops(unstoppable)).toBe(1);
        expect(h.events(DRAIN_EXPIRED)).toEqual([]);
        if (restart) await h.restart();

        await h.tickAt(REAL_DEADLINE_S + 30);
        expect(h.slotOpen(late.slotId)).toBe(false);
        expect(h.get(lateRow)).toMatchObject({ destinationSession: MODEL, state: "pending" });
        const expired = expiredOf(h);
        expectLeft(expired.left, [
          { seat: refused, why: "release_refused", exitCode: 1 },
          { seat: unstoppable, why: "stop_failed", exitCode: 0 },
        ]);
        expect(expired.moved.map((m) => m.qitemId).sort()).toEqual([freeRow, lateRow].sort());
        expect(h.releasesOf(refused)).toHaveLength(1);
        expect(h.releasesOf(unstoppable)).toHaveLength(1);
        expect(stops(unstoppable)).toBe(1);
        expect(h.sessionRunning(refused.nodeId)).toBe(true);
        expect(h.get(refusedRow)).toMatchObject({ destinationSession: refused.session, state: "pending" });
        expect(h.get(stuckRow)).toMatchObject({ destinationSession: unstoppable.session, state: "pending" });
        await h.tickAt(REAL_DEADLINE_S + 60);
        expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
        expect(h.releasesOf(refused)).toHaveLength(1);

        const stopped = await h.stop();
        expect(stopped.status).toBe(200);
        expect((stopped.body["leftRunning"] as Array<{ sessionName: string; why: string }>).map((l) => [l.sessionName, l.why]).sort())
          .toEqual([[refused.session, "release_refused"], [unstoppable.session, "stop_failed"]].sort());
      });
    }
  });

  describe("r2.1 S3 and S2: the pass after the release", () => {
    it("S3: drain turned off while the release runs: no stop, no expired entry, the seat admitted with no intent", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      h.duringCommand = (argv, env) => {
        if (h.isRelease(argv) && env[SLOT_ENV] === seat.session) { h.duringCommand = null; h.setDrain(false); }
      };
      await h.tickAt(REAL_DEADLINE_S);
      expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
      expect(h.stopped(seat)).toBe(false);
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.slots.get(seat.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(h.get(row)).toMatchObject({ destinationSession: seat.session, state: "pending" });
      expect(h.fallbackNotes(row)).toEqual([]);
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);

      await h.tickAt(REAL_DEADLINE_S + 30);
      expect(h.events(DRAIN_ENDED)).toHaveLength(1);
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
      expect(h.slotOpen(seat.slotId)).toBe(true);
    });

    it("drain turned off between the deadline and the seat's lease: the lease reads drain again, and nothing is released or stopped (D7)", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      h.beforeLease = (logical) => {
        if (logical === seat.logicalId) { h.beforeLease = null; h.setDrain(false); }
      };
      await h.tickAt(REAL_DEADLINE_S);
      expect(h.calls).toContain(`lease ${seat.logicalId}`);
      expect(h.releasesOf(seat)).toEqual([]);
      expect(h.stopped(seat)).toBe(false);
      expect(h.sessionRunning(seat.nodeId)).toBe(true);
      expect(h.get(row)).toMatchObject({ destinationSession: seat.session, state: "pending" });
      expect(h.events(DRAIN_EXPIRED)).toEqual([]);
    });

    it("S2: a seat the pass stopped before a restart (draining, intent stop, session gone) is not released again: its rows go to the model and it leaves", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      // The pass got through the release and the stop, and the daemon went down before the rows moved: the state as stored.
      h.nowMs = T0 + (REAL_DEADLINE_S + 10) * 1000;
      h.slots.update(seat.slotId, { phase: "draining", intent: "stop" });
      h.endSession(seat);
      await h.restart();
      h.calls = [];
      await h.tick();
      expect(h.releasesOf(seat)).toEqual([]);
      expect(h.calls).not.toContain(`relaunch ${seat.logicalId}`);
      expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
      expect(h.fallbackNotes(row)).toHaveLength(1);
      expect(h.fallbackNotes(row)[0]).toMatch(drainNote(seat));
      expect(h.nodeExists(seat.nodeId)).toBe(false);
      expect(h.slotOpen(seat.slotId)).toBe(false);
      const expired = expiredOf(h);
      expect(expired.left).toEqual([]);
      expect(expired.moved.map((m) => m.qitemId)).toEqual([row]);
    });

    it("S2: with intent stop and the session still running, the release is asked again", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      h.nowMs = T0 + (REAL_DEADLINE_S + 10) * 1000;
      h.slots.update(seat.slotId, { phase: "draining", intent: "stop" });
      await h.restart();
      await h.tick();
      expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
      const iRelease = h.calls.findIndex((c) => c.endsWith(`@${seat.session}`) && c.includes("recibos.py"));
      const iStop = h.calls.findIndex((c) => c.startsWith(`stop ${seat.session}`));
      expect(iRelease).toBeGreaterThanOrEqual(0);
      expect(iRelease).toBeLessThan(iStop);
      expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
      expect(h.slotOpen(seat.slotId)).toBe(false);
    });

    it("S2: a seat the pass was removing before a restart (draining, intent remove, session gone, its rows already moved) is not released again either: it leaves", async () => {
      // r2.1 item 2: the release is skipped with intent stop OR remove and the session not running (CETICO v2r2 B1, M1).
      // Working, so the common drain does not take it out (as a free seat) before the state below is stored.
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "working");
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      h.nowMs = T0 + (REAL_DEADLINE_S + 10) * 1000;
      h.slots.update(seat.slotId, { phase: "draining", intent: "remove" });
      h.endSession(seat);
      await h.restart();
      h.calls = [];
      await h.tick();
      expect(h.releasesOf(seat)).toEqual([]);
      expect(h.calls).not.toContain(`relaunch ${seat.logicalId}`);
      expect(h.nodeExists(seat.nodeId)).toBe(false);
      expect(h.slotOpen(seat.slotId)).toBe(false);
      const expired = expiredOf(h);
      expect(expired.left).toEqual([]);
      expect(expired.moved).toEqual([]);
    });

    it("S2 under drain, before the deadline: a seat stopped on its way out (draining, intent stop, session gone) is never put back in admission, and the pass finishes it at the deadline without a release", async () => {
      // r2.1 item 2: no failure after the stop puts the seat back in admission, where the reap would relaunch it; the
      // common drain resumes every draining seat each tick and must leave this one alone (CETICO v2r2 B1, M2).
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      // The state as stored: the seat stopped on its way out, and the daemon went down before its removal.
      h.slots.update(seat.slotId, { phase: "draining", intent: "stop" });
      h.endSession(seat);
      await h.restart();
      h.calls = [];
      for (const at of [60, 90, 300, REAL_DEADLINE_S - 1]) {
        await h.tickAt(at);
        const slot = h.slots.get(seat.slotId)!;
        expect(slot, `at ${at} s`).toMatchObject({ phase: "draining", retiredAt: null });
        expect(["stop", "remove"]).toContain(slot.intent);
        expect(h.get(row).destinationSession).toBe(seat.session);
      }
      expect(h.releaseCalls).toEqual([]);
      expect(h.calls).not.toContain(`relaunch ${seat.logicalId}`);

      await h.tickAt(REAL_DEADLINE_S);
      expect(h.releasesOf(seat)).toEqual([]);
      expect(h.calls).not.toContain(`relaunch ${seat.logicalId}`);
      expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
      expect(h.fallbackNotes(row)).toHaveLength(1);
      expect(h.fallbackNotes(row)[0]).toMatch(drainNote(seat));
      expect(h.slotOpen(seat.slotId)).toBe(false);
      const expired = expiredOf(h);
      expect(expired.left).toEqual([]);
      expect(expired.moved.map((m) => m.qitemId)).toEqual([row]);
    });

    it("S2: a failure after the stop leaves the seat draining with its intent, never live", async () => {
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const row = await h.row(seat.session);
      h.removeFails.add(seat.logicalId);
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      await h.tickAt(REAL_DEADLINE_S);
      expect(h.stopped(seat)).toBe(true);
      expect(h.calls).toContain(`remove ${seat.logicalId}`);
      expect(h.sessionRunning(seat.nodeId)).toBe(false);
      const slot = h.slots.get(seat.slotId)!;
      expect(slot.retiredAt).toBeNull();
      expect(slot.phase).toBe("draining");
      expect(["stop", "remove"]).toContain(slot.intent);
      // §2.2 steps 3-5 and §2.3: the row went to the model before the removal, and the seat is left with why and the
      // release's exit code, so the stop takes it (CETICO v2r2 B2, M3).
      expect(h.get(row)).toMatchObject({ destinationSession: MODEL, state: "pending" });
      const expired = expiredOf(h);
      expectLeft(expired.left, [{ seat, why: "remove_failed", exitCode: 0 }]);
      expect(expired.moved.map((m) => m.qitemId)).toEqual([row]);

      // Nor on the next tick under drain: the common drain resumes every draining seat, and it does not put this one
      // back in admission (where the reap would relaunch it). It stays draining, unasked and stopped.
      const asked = h.releasesOf(seat).length;
      await h.tickAt(REAL_DEADLINE_S + 30);
      const next = h.slots.get(seat.slotId)!;
      expect(next.retiredAt).toBeNull();
      expect(next.phase).toBe("draining");
      expect(["stop", "remove"]).toContain(next.intent);
      expect(h.releasesOf(seat)).toHaveLength(asked);
      expect(h.calls).not.toContain(`relaunch ${seat.logicalId}`);
      expect(h.sessionRunning(seat.nodeId)).toBe(false);
      expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
      const stopped = await h.stop();
      expect(stopped.status).toBe(200);
      expect(stopped.body["leftRunning"]).toEqual([expect.objectContaining({ sessionName: seat.session, why: "remove_failed" })]);
    });

    it("S2: a row move that fails after the stop leaves the seat draining and stopped, the row moved before it in moved, never live", async () => {
      // r2.1 item 2 and §2.2 step 5 (CETICO v2r2 S1, M5): the move that fails is the second one the pool makes.
      h.setContext(REAL_SPEC.context);
      const seat = h.poolSeat("dev.impl-s1", "idle");
      const rows = [await h.row(seat.session), await h.row(seat.session)];
      h.moveFails = (_qitemId, n) => n === 1;
      h.setContext(REAL_DRAIN_SPEC.context);
      await h.tickAt(0);
      await h.tickAt(REAL_DEADLINE_S);
      expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
      expect(h.stopped(seat)).toBe(true);
      expect(h.sessionRunning(seat.nodeId)).toBe(false);
      expect(h.calls).not.toContain(`remove ${seat.logicalId}`);
      const expired = expiredOf(h);
      expectLeft(expired.left, [{ seat, why: "move_failed", exitCode: 0 }]);
      expect(expired.moved).toHaveLength(1);
      const movedRow = expired.moved[0]!.qitemId;
      expect(rows).toContain(movedRow);
      const keptRow = rows.find((r) => r !== movedRow)!;
      expect(h.get(movedRow)).toMatchObject({ destinationSession: MODEL, state: "pending" });
      expect(h.get(keptRow)).toMatchObject({ destinationSession: seat.session, state: "pending" });
      const slot = h.slots.get(seat.slotId)!;
      expect(slot).toMatchObject({ phase: "draining", retiredAt: null });
      expect(["stop", "remove"]).toContain(slot.intent);

      h.moveFails = null;
      const asked = h.releasesOf(seat).length;
      for (const at of [REAL_DEADLINE_S + 30, REAL_DEADLINE_S + 60]) {
        await h.tickAt(at);
        expect(h.slots.get(seat.slotId), `at deadline + ${at - REAL_DEADLINE_S} s`).toMatchObject({ phase: "draining", retiredAt: null });
      }
      expect(h.releasesOf(seat)).toHaveLength(asked);
      expect(h.calls).not.toContain(`relaunch ${seat.logicalId}`);
      expect(h.events(DRAIN_EXPIRED)).toHaveLength(1);
      const stopped = await h.stop();
      expect(stopped.status).toBe(200);
      expect(stopped.body["leftRunning"]).toEqual([expect.objectContaining({ sessionName: seat.session, why: "move_failed" })]);
    });
  });

  describe("r2.1: the reap as it is, told reap", () => {
    for (const drain of [false, true]) {
      it(`a seat whose session died${drain ? " under drain, before the deadline," : ""} is reaped with its release told reap, refused for no pane, and launched again`, async () => {
        h.setContext(REAL_SPEC.context);
        const seat = h.poolSeat("dev.impl-s1", "idle");
        await h.row(seat.session);
        if (drain) h.setContext(REAL_DRAIN_SPEC.context);
        h.endSession(seat);
        await h.tickAt(0);
        const releases = h.releasesOf(seat);
        expect(releases.map((c) => [c.reason, c.exitCode])).toEqual([["reap", 1]]);
        expect(JSON.parse(releases[0]!.stdout).falhas).toContain(NO_PANE);
        expect(h.calls).toContain(`relaunch ${seat.logicalId}`);
        expect(h.events(DRAIN_EXPIRED)).toEqual([]);
      });
    }
  });

  it("6: an adopted seat with an open row is never drained, stopped, removed or moved, and does not hold the stop", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
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
    expect(h.releasesOf(fixed)).toEqual([]);
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
      expect(h.stopped(seat)).toBe(false);

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
    // No release_cmd in this spec: the exit code is null (it did not run).
    expectLeft(expired.left, [{ seat, why: "remove_cmd_failed", exitCode: null }]);
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

  it("4b-v1: the forced pass runs once per drain: a seat refused at the deadline is not released again on the next ticks", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
    const refusing = h.poolSeat("dev.impl-s1", "idle");
    const kept = await h.row(refusing.session);
    h.receipts.set(refusing.session, [REAL_EXECUTION]);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expectLeft(expiredOf(h).left, [{ seat: refusing, why: "release_refused", exitCode: 1 }]);
    await h.tickAt(DEFAULT_DEADLINE_S + 30);
    await h.tickAt(DEFAULT_DEADLINE_S + 60);
    // Section 2.2: once per seat, and seat_pool_drain_expired once per drain (a refusal, exit 1, is final).
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
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
    const seat = h.poolSeat("dev.impl-s1", "idle");
    await h.row(seat.session);
    h.receipts.set(seat.session, [REAL_EXECUTION]);

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expectLeft(expiredOf(h).left, [{ seat, why: "release_refused", exitCode: 1 }]);

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
    expect(h.releasesOf(seat).map((c) => c.reason)).toEqual([DRAIN_DEADLINE, DRAIN_DEADLINE]);
    const notes = expired[1]!.evaluationNotes as { left: LeftItem[] };
    expectLeft(notes.left, [{ seat, why: "release_refused", exitCode: 1 }]);
    const stopped = await h.stop();
    expect(stopped.status).toBe(200);
    expect(stopped.body["leftRunning"]).toEqual([expect.objectContaining({ sessionName: seat.session, why: "release_refused" })]);
  });

  it("2.2: the stop is given the expected session read at the start of the pass, before the release command ran", async () => {
    h.setContext({ ...BASE_CONTEXT, drain: false, release_cmd: RELEASE });
    const seat = h.poolSeat("dev.impl-s1", "idle");
    await h.row(seat.session);
    const before = h.latestSession(seat.nodeId)!.id;
    // The release command brings up a new session on the node while it runs.
    h.duringCommand = (argv, env) => {
      if (argv[0] !== "release" || env[SLOT_ENV] !== seat.session) return;
      h.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${seat.logicalId}-2`, seat.nodeId, seat.session);
    };

    h.setDrain(true);
    await h.tickAt(0);
    await h.tickAt(DEFAULT_DEADLINE_S);
    expect(h.calls).toContain(`run release @${seat.session}`);
    expect(h.releasesOf(seat).map((c) => [c.reason, c.exitCode])).toEqual([[DRAIN_DEADLINE, 0]]);
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
