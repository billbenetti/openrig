import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import type Database from "better-sqlite3";
import type { StopPrecondition } from "./seat-lifecycle-service.js";
import type { ArbitratedSeatState } from "./activity-taxonomy.js";
import { deriveCanonicalSessionName } from "./session-name.js";
import { physicalSessionName } from "./session-registry.js";
import { FALLBACK_ACTOR, type QueueRepository } from "./queue-repository.js";
import { OPENRIG_SENDER } from "../lib/pane-envelope.js";
import type { Node } from "./types.js";
import type { LaunchBudget, LaunchTick } from "./launch-budget.js";
import {
  seatPoolReapable,
  seatPoolRoom,
  slotWorkingDirectory,
  type SeatPoolCommand,
  type SeatPoolIntent,
  type SeatPoolModel,
  type SeatPoolPendingRow,
  type SeatPoolSeatCause,
  type SeatPoolSlotPhase,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "./policies/seat-pool.js";

/**
 * The seat-pool executor: applies a pool's plan outside the scheduler tick.
 *
 * One run per pool at a time; a plan for a pool whose previous plan is still running is refused, so two runs never act
 * on the same seats. Before every effect the run reads the job, its spec and the pool's state again, and stops when the
 * job is no longer active, its model is gone or the rig is being restored. Each effect is recorded on the seat's row
 * (`intent`) before it starts and cleared once its result is written, and each result goes to the job's history. A
 * restart therefore resumes from the rows: a seat being added is resolved through its Operation-Id, a seat added but
 * not launched is prepared and launched or removed, and a seat being drained finishes draining. Moving a row and waking
 * a seat check the row and the seat again just before, and change nothing when either changed since the plan or the
 * pool was set to drain. An add, a launch and a relaunch check provider usage again just before, and start nothing
 * while a seat of the pool is out of usage. Each add, launch and relaunch also holds a reservation of the daemon's
 * launch budget (launch-budget.ts) until it settles, charged to the watchdog tick the plan was submitted in, and starts
 * nothing when the host or that tick has no room. A reap, under the seat's lifecycle lease, checks again that the
 * seat's session is gone, and leaves a seat that came back since the plan.
 */

export interface SeatPoolSlot {
  slotId: number;
  poolJobId: string;
  nodeId: string | null;
  logicalId: string;
  phase: SeatPoolSlotPhase;
  intent: string | null;
  /** The add's Operation-Id; null for an adopted seat. */
  opId: string | null;
  createdAt: string;
  retiredAt: string | null;
}

interface SlotRow {
  id: number;
  pool_job_id: string;
  node_id: string | null;
  logical_id: string;
  phase: SeatPoolSlotPhase;
  intent: string | null;
  op_id: string | null;
  created_at: string;
  retired_at: string | null;
}

const toSlot = (row: SlotRow): SeatPoolSlot => ({
  slotId: row.id, poolJobId: row.pool_job_id, nodeId: row.node_id, logicalId: row.logical_id, phase: row.phase,
  intent: row.intent, opId: row.op_id, createdAt: row.created_at, retiredAt: row.retired_at,
});

/** A slot row whose pool's job is no longer active: no evaluation acts on it again, so its seat is free. A job that
 *  stops or finishes with seats still in it leaves them running (a daemon without the seat-pool policy marks the job
 *  terminal, `unknown_policy:seat-pool`). */
const POOL_ENDED = "EXISTS (SELECT 1 FROM watchdog_jobs j WHERE j.job_id = seat_pool_slots.pool_job_id AND j.state <> 'active')";

/** The `seat_pool_slots` rows: the only link between a pool and its seats. */
export class SeatPoolSlots {
  constructor(private readonly db: Database.Database) {}

  /** The pool's seats that are not retired, oldest first. */
  listOpen(poolJobId: string): SeatPoolSlot[] {
    return (this.db.prepare("SELECT * FROM seat_pool_slots WHERE pool_job_id = ? AND retired_at IS NULL ORDER BY id")
      .all(poolJobId) as SlotRow[]).map(toSlot);
  }

  get(slotId: number): SeatPoolSlot | null {
    const row = this.db.prepare("SELECT * FROM seat_pool_slots WHERE id = ?").get(slotId) as SlotRow | undefined;
    return row ? toSlot(row) : null;
  }

  /** Whether a pool holds this node. A pool that has ended (its job stopped or finished) holds nothing. */
  isLinked(nodeId: string): boolean {
    return !!this.db.prepare(`SELECT 1 FROM seat_pool_slots WHERE node_id = ? AND retired_at IS NULL AND NOT ${POOL_ENDED}`).get(nodeId);
  }

  /** The logical ids of the seats the pool added and removed; an adopted seat it gave back is still there. */
  listRetired(poolJobId: string): string[] {
    return (this.db.prepare("SELECT logical_id FROM seat_pool_slots WHERE pool_job_id = ? AND retired_at IS NOT NULL AND op_id IS NOT NULL ORDER BY id")
      .all(poolJobId) as Array<{ logical_id: string }>).map((row) => row.logical_id);
  }

  /** The latest add or removal in the pool; adopting or unlinking a seat is neither. */
  lastScaleAt(poolJobId: string): string | null {
    const row = this.db.prepare(
      `SELECT MAX(CASE WHEN op_id IS NOT NULL THEN created_at END) AS added, MAX(CASE WHEN op_id IS NOT NULL THEN retired_at END) AS retired
         FROM seat_pool_slots WHERE pool_job_id = ?`,
    ).get(poolJobId) as { added: string | null; retired: string | null };
    const times = [row.added, row.retired].filter((t): t is string => t !== null).sort();
    return times.at(-1) ?? null;
  }

  /**
   * The next `<member>-s<n>` for a model. `n` is one more than any this table ever recorded for the model, in any
   * pool, and skips a logical id the rig already has, so a removed seat's name is never given to a new one.
   */
  nextLogicalId(model: Pick<SeatPoolModel, "rigId" | "logicalId">): string {
    const prefix = `${model.logicalId}-s`;
    let n = 0;
    const rows = this.db.prepare("SELECT logical_id FROM seat_pool_slots WHERE substr(logical_id, 1, ?) = ?")
      .all(prefix.length, prefix) as Array<{ logical_id: string }>;
    for (const { logical_id } of rows) {
      const suffix = logical_id.slice(prefix.length);
      if (/^[1-9]\d*$/.test(suffix)) n = Math.max(n, Number(suffix));
    }
    const taken = this.db.prepare("SELECT 1 FROM nodes WHERE rig_id = ? AND logical_id = ?");
    do n++; while (taken.get(model.rigId, `${prefix}${n}`));
    return `${prefix}${n}`;
  }

  insert(input: { poolJobId: string; logicalId: string; nodeId: string | null; phase: SeatPoolSlotPhase; intent: string | null; opId: string | null; at: string }): SeatPoolSlot {
    const id = this.db.prepare(
      `INSERT INTO seat_pool_slots (pool_job_id, node_id, logical_id, phase, intent, op_id, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    ).run(input.poolJobId, input.nodeId, input.logicalId, input.phase, input.intent, input.opId, input.at).lastInsertRowid;
    return this.get(Number(id))!;
  }

  update(slotId: number, fields: { nodeId?: string; phase?: Exclude<SeatPoolSlotPhase, "retired">; intent?: string | null }): void {
    const sets: string[] = [];
    const values: unknown[] = [];
    if (fields.nodeId !== undefined) { sets.push("node_id = ?"); values.push(fields.nodeId); }
    if (fields.phase !== undefined) { sets.push("phase = ?"); values.push(fields.phase); }
    if (fields.intent !== undefined) { sets.push("intent = ?"); values.push(fields.intent); }
    if (sets.length > 0) this.db.prepare(`UPDATE seat_pool_slots SET ${sets.join(", ")} WHERE id = ?`).run(...values, slotId);
  }

  retire(slotId: number, at: string): void {
    this.db.prepare("UPDATE seat_pool_slots SET phase = 'retired', intent = NULL, retired_at = ? WHERE id = ?").run(at, slotId);
  }

  /**
   * Links a seat the pool adopts, retiring first the link a pool that has ended left on it. A seat that pool added
   * keeps its add (Operation-Id and time), so it stays a seat a pool added, drained and reaped, not an adopted one.
   */
  adopt(input: { poolJobId: string; logicalId: string; nodeId: string; at: string }): SeatPoolSlot {
    return this.db.transaction(() => {
      const ended = this.db.prepare(`SELECT id, op_id, created_at FROM seat_pool_slots WHERE node_id = ? AND retired_at IS NULL AND ${POOL_ENDED}`)
        .get(input.nodeId) as { id: number; op_id: string | null; created_at: string } | undefined;
      if (ended) this.retire(ended.id, input.at);
      const added = ended?.op_id ? { opId: ended.op_id, at: ended.created_at } : { opId: null, at: input.at };
      return this.insert({ ...input, ...added, phase: "live", intent: null });
    })();
  }
}

/** The canonical session name of a seat, from its `<pod>.<member>` logical id. */
export function seatSessionName(logicalId: string, rigName: string): string {
  const dot = logicalId.indexOf(".");
  return deriveCanonicalSessionName(logicalId.slice(0, dot), logicalId.slice(dot + 1), rigName);
}

/**
 * The member fragment of a new seat: the model's launch shape (runtime, agent, profile, model, effort, permissions,
 * environment, launch wrapper) with its own id and working directory. Its session starts fresh: the model's
 * conversation is never carried over.
 */
export function slotMemberFragment(
  model: Node,
  memberId: string,
  cwd: string,
  permissions: { claude?: unknown; codex?: unknown } = {},
): Record<string, unknown> {
  return {
    id: memberId,
    runtime: model.runtime,
    cwd,
    ...(model.agentRef ? { agent_ref: model.agentRef } : {}),
    ...(model.profile ? { profile: model.profile } : {}),
    ...(model.codexConfigProfile ? { codex_config_profile: model.codexConfigProfile } : {}),
    ...(model.permissionPolicy ? { permission_policy: model.permissionPolicy } : {}),
    ...(permissions.claude === undefined ? {} : { claude_permissions: permissions.claude }),
    ...(permissions.codex === undefined ? {} : { codex_permissions: permissions.codex }),
    ...(model.model ? { model: model.model } : {}),
    ...(model.effort ? { effort: model.effort } : {}),
    ...(model.role ? { role: model.role } : {}),
    ...(model.restorePolicy ? { restore_policy: model.restorePolicy } : {}),
    ...(model.env ? { env: model.env } : {}),
    ...(model.launchWrapper?.length ? { launch_wrapper: model.launchWrapper } : {}),
  };
}

export interface SeatPoolStateReaderDeps {
  db: Database.Database;
  slots: SeatPoolSlots;
  /** Pending, in-progress and blocked rows addressed to a session. */
  openRows(sessionName: string): Array<{ state: string }>;
  /** Every destination a row addressed to this session may carry (the queue's own address resolution). */
  sessionAddresses(sessionName: string): string[];
  /** Messages the typing guard holds for a seat, not yet delivered. */
  heldMessages(nodeId: string): number;
  /** Whether the seat has no live session. */
  seatStopped(nodeId: string): boolean;
  /** The arbitrated activity of a seat whose session is not confirmed stopped; null otherwise. */
  seatState(nodeId: string): ArbitratedSeatState | null;
  /** Whether the provider account the session runs on is out of usage. Absent: never. */
  usageLimited?(sessionName: string): boolean;
  restoring(rigId: string): boolean;
  demand(jobId: string): { desired: number; sampledAt: string } | null;
}

/** Pending rows addressed to any of `sessions` (in any of their addresses), longest waiting first. A row waits from its
 *  latest reroute, or from its creation. Only pending rows: a claimed, blocked or closed row is never the pool's to
 *  move. */
function readPendingRows(deps: SeatPoolStateReaderDeps, sessions: string[]): SeatPoolPendingRow[] {
  const sessionOf = new Map<string, string>();
  for (const session of sessions) {
    for (const address of deps.sessionAddresses(session)) if (!sessionOf.has(address)) sessionOf.set(address, session);
  }
  if (sessionOf.size === 0) return [];
  const rows = deps.db.prepare(
    `SELECT q.qitem_id, q.destination_session, q.tags,
            COALESCE((SELECT MAX(t.ts) FROM queue_transitions t WHERE t.qitem_id = q.qitem_id AND t.actor_session = ?), q.ts_created) AS since
       FROM queue_items q
      WHERE q.state = 'pending' AND q.destination_session IN (${[...sessionOf.keys()].map(() => "?").join(",")})
      ORDER BY since, q.qitem_id`,
  ).all(FALLBACK_ACTOR, ...sessionOf.keys()) as Array<{ qitem_id: string; destination_session: string; tags: string | null; since: string }>;
  return rows.map((row) => ({
    qitemId: row.qitem_id, destination: sessionOf.get(row.destination_session)!, waitingSince: row.since,
    tags: row.tags ? JSON.parse(row.tags) as string[] : [],
  }));
}

/** The pool's state as the planner sees it. */
export function readSeatPoolState(deps: SeatPoolStateReaderDeps, job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState {
  const { db } = deps;
  const row = db.prepare(
    `SELECT n.id AS node_id, n.logical_id, r.id AS rig_id, r.name AS rig_name, p.namespace
       FROM sessions s JOIN nodes n ON n.id = s.node_id JOIN rigs r ON r.id = n.rig_id JOIN pods p ON p.id = n.pod_id
      WHERE s.session_name = ? ORDER BY s.id DESC LIMIT 1`,
  ).get(physicalSessionName(db, job.targetSession)) as
    | { node_id: string; logical_id: string; rig_id: string; rig_name: string; namespace: string }
    | undefined;
  const model: SeatPoolModel | null = row && row.logical_id.startsWith(`${row.namespace}.`)
    ? {
        rigId: row.rig_id, rigName: row.rig_name, podNamespace: row.namespace, memberId: row.logical_id.slice(row.namespace.length + 1),
        logicalId: row.logical_id, nodeId: row.node_id, sessionName: job.targetSession,
      }
    : null;
  const empty: SeatPoolState = { model, modelCause: "working", restoring: false, slots: [], adoptable: [], pending: [], demand: null, lastScaleAt: null };
  if (!model) return empty;

  const causeOf = (nodeId: string, sessionName: string, state: ArbitratedSeatState | null): SeatPoolSeatCause => {
    if (deps.usageLimited?.(sessionName)) return "usage_limit";
    if (deps.seatStopped(nodeId)) return "stopped";
    if (state && state.needsInput.count > 0) return "needs_input";
    return state?.activity === "idle-at-prompt" && !state.backgroundWork ? "idle" : "working";
  };
  const nodeExists = (nodeId: string) => !!db.prepare("SELECT 1 FROM nodes WHERE id = ? AND rig_id = ?").get(nodeId, model.rigId);
  const slots = deps.slots.listOpen(job.jobId).map((slot): SeatPoolSlotView => {
    const present = slot.nodeId !== null && nodeExists(slot.nodeId);
    const state = present ? deps.seatState(slot.nodeId!) : null;
    const idle = state?.activity === "idle-at-prompt" && state.needsInput.count === 0 && !state.backgroundWork;
    const sessionName = seatSessionName(slot.logicalId, model.rigName);
    return {
      slotId: slot.slotId,
      logicalId: slot.logicalId,
      sessionName,
      nodeId: slot.nodeId,
      phase: slot.phase as Exclude<SeatPoolSlotPhase, "retired">,
      adopted: slot.opId === null,
      nodeExists: present,
      openRows: present ? deps.openRows(sessionName).length : 0,
      heldMessages: present ? deps.heldMessages(slot.nodeId!) : 0,
      idleSince: idle ? state!.changedAt : null,
      // A seat of the pool whose session is gone is reaped, which a usage limit doesn't hold, so for it a stopped session
      // comes before its account's usage; for the model, which the pool can only launch, the usage limit comes first.
      cause: !present || deps.seatStopped(slot.nodeId!) ? "stopped" : causeOf(slot.nodeId!, sessionName, state),
    };
  });
  const adoptable = job.spec.adopt.flatMap((logicalId) => {
    const node = db.prepare("SELECT id FROM nodes WHERE rig_id = ? AND logical_id = ?").get(model.rigId, logicalId) as { id: string } | undefined;
    return node && node.id !== model.nodeId && !deps.slots.isLinked(node.id) ? [{ logicalId, nodeId: node.id }] : [];
  });
  const retired = deps.slots.listRetired(job.jobId).map((logicalId) => seatSessionName(logicalId, model.rigName));
  return {
    model,
    modelCause: causeOf(model.nodeId, model.sessionName, deps.seatState(model.nodeId)),
    restoring: deps.restoring(model.rigId),
    slots,
    adoptable,
    pending: readPendingRows(deps, [model.sessionName, ...slots.map((slot) => slot.sessionName), ...retired]),
    demand: job.spec.demandCmd ? deps.demand(job.jobId) : null,
    lastScaleAt: deps.slots.lastScaleAt(job.jobId),
  };
}

export type SeatPoolEffectResult = { ok: true } | { ok: false; code: string; error: string };

export interface SeatPoolCommandResult {
  exitCode: number | null;
  stdout: string;
  error?: string;
}

/** The daemon operations a run uses, injected so the executor holds no service of its own. */
export interface SeatPoolEffects {
  /** Adds the seat without a session, under the Operation-Id; the same id again returns the node it created. */
  addSlot(input: { model: SeatPoolModel; memberId: string; cwd: string; operationId: string }): Promise<SeatPoolEffectResult & { nodeId?: string }>;
  /** The node an earlier add with this Operation-Id created, without adding anything. */
  operationNodeId(operationId: string): string | null;
  /** The seat's first launch. */
  launchSlot(input: { rigId: string; nodeId: string }): Promise<SeatPoolEffectResult>;
  sessionRunning(nodeId: string): boolean;
  /** Runs `fn` holding the seat's lifecycle lease, so no launch, stop or delivery of the seat comes between its steps. */
  lifecycle<T>(nodeId: string, fn: () => Promise<T>): Promise<T>;
  stopPrecondition(nodeId: string): StopPrecondition;
  stopSlot(input: { sessionName: string; expected: StopPrecondition }): Promise<SeatPoolEffectResult>;
  removeSlot(input: { rigId: string; nodeId: string; expected: StopPrecondition }): Promise<SeatPoolEffectResult>;
  runCommand(command: SeatPoolCommand, env: Record<string, string>, signal: AbortSignal): Promise<SeatPoolCommandResult>;
  /** Returns a seat whose session is gone to launchable, after checking in tmux that it is gone. */
  cleanSlot(input: { sessionName: string }): Promise<SeatPoolEffectResult>;
  /** The pending, in-progress and blocked rows addressed to a seat. */
  openRows(sessionName: string): Array<{ qitemId: string; state: "pending" | "in-progress" | "blocked" }>;
  /** Returns an in-progress row to pending, releasing its claim. */
  unclaimRow(input: { qitemId: string; seat: string; reason: string }): SeatPoolEffectResult;
  /** How many reaps moved the row before: its reroutes whose reason is `reap:<seat>`. */
  reapCount(qitemId: string): number;
  /**
   * Moves a row to `to` only if it is still in `state` (pending by default) and addressed to `from`, in one transaction,
   * and wakes `to` when the row is pending.
   */
  moveRow(input: { qitemId: string; from: string; to: string; reason: string; state?: "pending" | "blocked" }): SeatPoolEffectResult;
  /** Launches a seat whose session stopped, as `rig launch` does for it. */
  relaunchSeat(input: { rigId: string; logicalId: string }): Promise<SeatPoolEffectResult>;
  /** Where the model's escalations go, or why there is nowhere. */
  escalationOwner(modelSession: string): { owner: string | null; reason: string };
  /** Opens a row to the model's escalation owner about a seat that needs input, unless one about that seat is open. */
  escalate(input: { jobId: string; seat: string; model: string; qitemIds: string[] }):
    Promise<SeatPoolEffectResult & { qitemId?: string; destination?: string; existing?: boolean }>;
}

/** The tag on the open escalation about a seat that needs input, followed by that seat: one at a time per seat. */
export const SEAT_POOL_NEEDS_INPUT_TAG = "seat-pool-needs-input";
/** The reason a reap gives each row it moves, followed by the seat; counting these reroutes counts the row's reaps. */
export const SEAT_POOL_REAP_REASON = "reap:";

const errorCode = (error: unknown, fallback: string) => {
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : fallback;
};

/** The executor's row moves and escalations, through the queue repository. */
export function seatPoolQueueEffects(deps: {
  queueRepo: QueueRepository;
  /** Where an escalation about a seat of the pool goes: the model's escalation owner, or why it has none. */
  escalationOwner(modelSession: string): { owner: string | null; reason: string };
}): Pick<SeatPoolEffects, "openRows" | "unclaimRow" | "reapCount" | "moveRow" | "escalationOwner" | "escalate"> {
  const { queueRepo } = deps;
  return {
    openRows: (sessionName) => queueRepo.listOpenObligations(sessionName, 500),
    unclaimRow: ({ qitemId, seat, reason }) => {
      try {
        queueRepo.unclaim(qitemId, seat, reason);
        return { ok: true };
      } catch (error) {
        return { ok: false, code: errorCode(error, "unclaim_failed"), error: (error as Error).message };
      }
    },
    // The reroute's own transitions, as fallbackLineStart trusts them: written in-process (no identity provenance) by
    // the fallback actor. A transition the update route wrote can carry the same actor and note, and doesn't count.
    reapCount: (qitemId) => queueRepo.listTransitions(qitemId).filter((t) =>
      t.actorSession === FALLBACK_ACTOR && t.identityProvenance === null
      && /^fallback-routed: .* \(reap:[^\s()]+\)$/.test(t.transitionNote ?? "")).length,
    moveRow: ({ qitemId, from, to, reason, state = "pending" }) => {
      try {
        queueRepo.routeToFallback(qitemId, to, reason, { expectState: state, expectDestination: from, wake: state === "pending" });
        return { ok: true };
      } catch (error) {
        return { ok: false, code: errorCode(error, "fallback_failed"), error: (error as Error).message };
      }
    },
    escalationOwner: deps.escalationOwner,
    escalate: async ({ jobId, seat, model, qitemIds }) => {
      const tag = `${SEAT_POOL_NEEDS_INPUT_TAG}:${seat}`;
      const open = queueRepo.list({ tag, activeOnly: true, limit: 1 })[0];
      if (open) return { ok: true, qitemId: open.qitemId, destination: open.destinationSession, existing: true };
      const route = deps.escalationOwner(model);
      if (!route.owner) return { ok: false, code: "no_escalation_owner", error: route.reason };
      const created = await queueRepo.create({
        sourceSession: OPENRIG_SENDER,
        destinationSession: route.owner,
        summary: `${seat} needs input and holds ${qitemIds.length} waiting row(s)`,
        body: `${seat} is waiting for input (a permission prompt or a question), so it isn't taking the pending row(s) `
          + `addressed to it: ${qitemIds.join(", ")}. Answer its prompt; rig capture ${seat} shows it. `
          + `Raised by seat pool ${jobId}, which raises no other until this row is closed.`,
        tags: [tag],
        wakeTrigger: "openrig",
      });
      return { ok: true, qitemId: created.qitemId, destination: route.owner, existing: false };
    },
  };
}

/** Runs an argv without a shell, with the daemon's environment plus `env`, killed at its timeout or on abort. */
export function runSeatPoolCommand(command: SeatPoolCommand, env: Record<string, string>, signal: AbortSignal): Promise<SeatPoolCommandResult> {
  const [file, ...args] = command.argv;
  return new Promise((resolve) => {
    execFile(file!, args, {
      env: { ...process.env, ...env }, signal, timeout: command.timeoutSeconds * 1000, killSignal: "SIGKILL",
      maxBuffer: 1024 * 1024, encoding: "utf8",
    }, (error, stdout) => {
      if (!error) return resolve({ exitCode: 0, stdout });
      const failure = error as NodeJS.ErrnoException & { code?: number | string; killed?: boolean; signal?: string };
      const timedOut = failure.killed === true && failure.signal === "SIGKILL" && !signal.aborted;
      resolve({
        exitCode: typeof failure.code === "number" ? failure.code : null,
        stdout: stdout ?? "",
        error: timedOut ? `timed out after ${command.timeoutSeconds}s` : failure.message,
      });
    });
  });
}

/** The job as stored now, for the re-read before each effect. */
export interface SeatPoolJobView {
  active: boolean;
  targetSession: string;
  /** Null when the stored spec no longer parses: the run stops. */
  spec: SeatPoolSpec | null;
}

export interface SeatPoolExecutorDeps {
  slots: SeatPoolSlots;
  readJob(jobId: string): SeatPoolJobView | null;
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState;
  /** Whether the model or a seat of the pool is out of provider usage, read now. Absent: never. */
  outOfUsage?(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): Promise<boolean>;
  effects: SeatPoolEffects;
  /** The daemon's room for seat launches, shared by every pool. Absent: no limit. */
  launchBudget?: LaunchBudget;
  /** Writes one history entry for the job. */
  record(jobId: string, entry: { failed: boolean; reason: string; notes: Record<string, unknown> }): void;
  now?: () => Date;
  /** How long one add, launch or drain may take before the run stops waiting for it. Default 10 minutes. */
  effectTimeoutMs?: number;
}

interface Run {
  controller: AbortController;
  /** Effects the run stopped waiting for; the pool stays owned until they settle. */
  lingering: Array<Promise<unknown>>;
  /** Resolves when the run is cancelled. */
  cancelled: Promise<void>;
  /** The plan's own steps, without the effects the run stopped waiting for. */
  applied: Promise<void>;
  done: Promise<void>;
  /** The launch budget's tick the plan was submitted in: every launch of the run counts against it. */
  tick?: LaunchTick;
}

interface DrainOutcome {
  /** True stops the run. */
  stop: boolean;
  failed?: boolean;
  /** The history entry's reason; null writes none. */
  reason: string | null;
  notes?: Record<string, unknown>;
  /** A reap the release command refused: its result. */
  refused?: { exitCode: number | null; error: string | null };
  /** The seat's node was removed: the seat is forgotten next (forget). */
  removed?: true;
}

interface Context {
  jobId: string;
  spec: SeatPoolSpec;
  state: SeatPoolState;
  model: SeatPoolModel;
}

const STOPPED_CODES = new Set(["session_not_live", "no_session"]);
/** A reroute refused because the row was claimed, closed or moved since the plan: nothing failed. */
const ROW_MOVED_ON = new Set(["fallback_state_mismatch", "fallback_destination_mismatch", "qitem_not_found"]);
/** An unclaim refused because the row was closed or released since it was read: the move that follows decides. */
const UNCLAIM_MOVED_ON = new Set(["qitem_not_in_progress", "qitem_not_found"]);
const holdsWork = (slot: SeatPoolSlotView) => slot.openRows > 0 || slot.heldMessages > 0;
const TIMED_OUT = Symbol("timed out");
const NO_ROOM = Symbol("no room");

export class SeatPoolExecutor {
  private readonly runs = new Map<string, Run>();
  private readonly samples = new Map<string, { desired: number; sampledAt: string }>();
  private readonly now: () => Date;
  private readonly effectTimeoutMs: number;
  private stopping = false;

  constructor(private readonly deps: SeatPoolExecutorDeps) {
    this.now = deps.now ?? (() => new Date());
    this.effectTimeoutMs = deps.effectTimeoutMs ?? 10 * 60_000;
  }

  /** The demand command's last good answer for the pool, or null. */
  demandSample(jobId: string): { desired: number; sampledAt: string } | null {
    return this.samples.get(jobId) ?? null;
  }

  /** Starts applying a plan; false when the pool already has a run, or the executor is stopping. */
  submit(jobId: string, intents: SeatPoolIntent[]): boolean {
    if (this.stopping || this.runs.has(jobId)) return false;
    const controller = new AbortController();
    const run: Run = {
      controller, lingering: [], applied: Promise.resolve(), done: Promise.resolve(), tick: this.deps.launchBudget?.currentTick(),
      cancelled: new Promise((resolve) => controller.signal.addEventListener("abort", () => resolve(), { once: true })),
    };
    run.applied = this.apply(jobId, intents, run)
      .catch((error) => this.deps.record(jobId, { failed: true, reason: "seat_pool_run_failed", notes: { error: (error as Error).message } }));
    run.done = run.applied.then(() => Promise.allSettled(run.lingering)).then(() => { this.runs.delete(jobId); });
    this.runs.set(jobId, run);
    return true;
  }

  /** Resolves when the pool's current run, if any, has finished. */
  async settled(jobId: string): Promise<void> {
    await this.runs.get(jobId)?.done;
  }

  /**
   * Cancels every run (commands are killed, no further effect starts) and waits for their steps to end, not for an
   * effect a run stopped waiting for or was waiting for when cancelled: one that never settles can't hold a shutdown.
   * The intent recorded on its seat lets the next start resume it.
   */
  async stop(): Promise<void> {
    this.stopping = true;
    for (const run of this.runs.values()) run.controller.abort();
    await Promise.all([...this.runs.values()].map((run) => run.applied));
  }

  private at(): string {
    return this.now().toISOString();
  }

  private record(jobId: string, failed: boolean, reason: string, notes: Record<string, unknown>): void {
    this.deps.record(jobId, { failed, reason, notes });
  }

  /** The job, spec and pool state read now; null stops the run. */
  private fresh(jobId: string, run: Run): Context | null {
    if (run.controller.signal.aborted) return null;
    const job = this.deps.readJob(jobId);
    if (!job?.active || !job.spec) return null;
    const state = this.deps.readState({ jobId, targetSession: job.targetSession, spec: job.spec });
    if (!state.model || state.restoring) return null;
    return { jobId, spec: job.spec, state, model: state.model };
  }

  /** Read just before an add, a launch or a relaunch: the limit can appear after the plan, or while a command runs. */
  private async outOfUsage(ctx: Context): Promise<boolean> {
    return (await this.deps.outOfUsage?.({ jobId: ctx.jobId, targetSession: ctx.model.sessionName, spec: ctx.spec })) ?? false;
  }

  private setIntent(slotId: number, intent: string | null, phase?: Exclude<SeatPoolSlotPhase, "retired">): void {
    this.deps.slots.update(slotId, { intent, ...(phase ? { phase } : {}) });
  }

  /** Waits for an effect until it settles, its timeout passes or the run is cancelled; TIMED_OUT for the last two. */
  private async within<T>(run: Run, start: () => Promise<T>): Promise<T | typeof TIMED_OUT> {
    const pending = start();
    run.lingering.push(pending.catch(() => undefined));
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<typeof TIMED_OUT>((resolve) => { timer = setTimeout(() => resolve(TIMED_OUT), this.effectTimeoutMs); });
    try {
      return await Promise.race([pending, timeout, run.cancelled.then((): typeof TIMED_OUT => TIMED_OUT)]);
    } finally {
      clearTimeout(timer);
    }
  }

  /** Records an effect the run stopped waiting for, unless a cancel cut it short, and stops the run. */
  private gaveUp(jobId: string, run: Run, reason: string, notes: Record<string, unknown>): false {
    if (!run.controller.signal.aborted) this.record(jobId, true, reason, { ...notes, code: "effect_timeout" });
    return false;
  }

  /**
   * Runs `start` holding a reservation of the daemon's launch budget, charged to the run's tick, or records why there
   * is no room and returns NO_ROOM without running it. The reservation lasts until every effect `start` began has
   * settled, including one the run stopped waiting for, so a launch past its timeout still counts against the next one.
   */
  private async withLaunchRoom<T>(jobId: string, run: Run, notes: Record<string, unknown>, start: () => Promise<T>): Promise<T | typeof NO_ROOM> {
    if (!this.deps.launchBudget) return start();
    const room = this.deps.launchBudget.reserve(run.tick);
    if (!room.ok) {
      this.record(jobId, false, "seat_pool_held", { reason: "launch_budget", why: room.why, ...room.notes, ...notes });
      return NO_ROOM;
    }
    const started = run.lingering.length;
    try {
      return await start();
    } finally {
      // A cancelled run stops waiting; the room is kept until those effects settle all the same.
      await Promise.race([Promise.allSettled(run.lingering.slice(started)).then(() => room.release()), run.cancelled]);
    }
  }

  private commandEnv(ctx: Context, extra: Record<string, string>): Record<string, string> {
    return { OPENRIG_SEAT_POOL_JOB: ctx.jobId, OPENRIG_SEAT_POOL_MODEL: ctx.model.sessionName, ...extra };
  }

  private async apply(jobId: string, intents: SeatPoolIntent[], run: Run): Promise<void> {
    for (const intent of intents) {
      if (!this.fresh(jobId, run)) return;
      if (intent.kind === "adopt") this.adopt(jobId, run, intent);
      else if (intent.kind === "add") {
        for (let i = 0; i < intent.count; i++) {
          // The pool's own room is checked before the launch budget's is reserved: an add past `max` is recorded and
          // spends none of the tick's launches.
          const ctx = this.fresh(jobId, run);
          if (!ctx) return;
          if (ctx.spec.drain) break;
          if (seatPoolRoom(ctx.spec, ctx.state.slots) === 0) {
            this.recordNoRoom(ctx, intent.count - i);
            break;
          }
          // Room is reserved before the seat is recorded, so a host without room leaves no seat half-added.
          const added = await this.withLaunchRoom(jobId, run, { add: intent.count - i }, async () => {
            const slot = await this.allocate(jobId, run);
            return !!slot && await this.bringUp(jobId, run, slot.slotId);
          });
          if (added !== true) break;
        }
      } else if (intent.kind === "launch") {
        const slot = this.deps.slots.get(intent.slotId);
        // A retired seat, or one whose launch already ran (its result lost to a restart), starts nothing: no room needed.
        const started = slot && !slot.retiredAt && !(slot.nodeId && this.deps.effects.sessionRunning(slot.nodeId))
          ? await this.withLaunchRoom(jobId, run, { slot: slot.logicalId }, () => this.bringUp(jobId, run, intent.slotId))
          : await this.bringUp(jobId, run, intent.slotId);
        if (started === false) return;
      } else if (intent.kind === "drain") {
        if (!(await this.drain(jobId, run, intent))) return;
      } else if (intent.kind === "remove") {
        if (!(await this.removeUnstarted(jobId, run, intent.slotId))) return;
      } else if (intent.kind === "unlink") {
        this.unlink(jobId, run, intent.slotId);
      } else if (intent.kind === "dispatch") {
        if (!this.dispatch(jobId, run, intent)) return;
      } else if (intent.kind === "reap") {
        if (!(await this.reap(jobId, run, intent.slotId))) return;
      } else if (!(await this.wake(jobId, run, intent))) return;
    }
    await this.refreshDemand(jobId, run);
  }

  private adopt(jobId: string, run: Run, intent: Extract<SeatPoolIntent, { kind: "adopt" }>): void {
    const ctx = this.fresh(jobId, run);
    if (!ctx || ctx.spec.drain || !ctx.state.adoptable.some((seat) => seat.nodeId === intent.nodeId)) return;
    const slot = this.deps.slots.adopt({ poolJobId: jobId, logicalId: intent.logicalId, nodeId: intent.nodeId, at: this.at() });
    this.record(jobId, false, "seat_pool_adopted", { slot: slot.logicalId, nodeId: intent.nodeId });
  }

  /** Gives back an adopted seat while the pool drains: its link is retired, and its node and session are left alone. */
  private unlink(jobId: string, run: Run, slotId: number): void {
    const slot = this.deps.slots.get(slotId);
    if (!slot || slot.retiredAt || slot.opId !== null || !this.fresh(jobId, run)?.spec.drain) return;
    this.deps.slots.retire(slotId, this.at());
    this.record(jobId, false, "seat_pool_unlinked", { slot: slot.logicalId });
  }

  /** An add the pool has no room for under `max` (seatPoolRoom): said in the history, never a silent no-op. */
  private recordNoRoom(ctx: Context, add: number): void {
    this.record(ctx.jobId, false, "seat_pool_held", { reason: "max_reached", max: ctx.spec.max, seats: ctx.state.slots.length, add });
  }

  /** Records a new seat (phase `adding`) if the pool, read again, still has room for it and no seat out of usage. */
  private async allocate(jobId: string, run: Run): Promise<SeatPoolSlot | null> {
    const before = this.fresh(jobId, run);
    if (!before || await this.outOfUsage(before)) return null;
    const ctx = this.fresh(jobId, run);
    if (!ctx || ctx.spec.drain) return null;
    // Read again after the usage check: the same count as the planner's, so a seat filled in between is not exceeded.
    if (seatPoolRoom(ctx.spec, ctx.state.slots) === 0) {
      this.recordNoRoom(ctx, 1);
      return null;
    }
    const logicalId = this.deps.slots.nextLogicalId(ctx.model);
    return this.deps.slots.insert({ poolJobId: jobId, logicalId, nodeId: null, phase: "adding", intent: "add", opId: randomUUID(), at: this.at() });
  }

  /** Adds (or resolves the add of), prepares and launches a seat. False stops the run. */
  private async bringUp(jobId: string, run: Run, slotId: number): Promise<boolean> {
    let slot = this.deps.slots.get(slotId);
    if (!slot || slot.retiredAt) return true;
    let ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    const memberId = slot.logicalId.slice(ctx.model.podNamespace.length + 1);
    const cwd = slotWorkingDirectory(ctx.spec.cwdTemplate, ctx.model.rigName, memberId);

    if (slot.phase === "adding") {
      this.setIntent(slotId, "add");
      const added = await this.within(run, () => this.deps.effects.addSlot({ model: ctx!.model, memberId, cwd, operationId: slot!.opId! }));
      if (added === TIMED_OUT) return this.gaveUp(jobId, run, "seat_pool_add_failed", { slot: slot.logicalId });
      if (!added.ok || !added.nodeId) {
        // The name is spent; the next add would most likely fail the same way, so the run stops here.
        this.deps.slots.retire(slotId, this.at());
        this.record(jobId, true, "seat_pool_add_failed", { slot: slot.logicalId, ...(added.ok ? { code: "no_node" } : { code: added.code, error: added.error }) });
        return false;
      }
      this.deps.slots.update(slotId, { nodeId: added.nodeId, phase: "added", intent: null });
      slot = this.deps.slots.get(slotId)!;
    }
    if (slot.phase !== "added") return true;

    ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    // Drain turned on since the plan: a seat that hasn't started is removed instead.
    if (ctx.spec.drain) return this.removeUnstarted(jobId, run, slotId);
    const nodeId = slot.nodeId!;
    const sessionName = seatSessionName(slot.logicalId, ctx.model.rigName);
    // A launch that ran but whose result was lost: the session is the evidence.
    if (!this.deps.effects.sessionRunning(nodeId)) {
      if (ctx.spec.prepareCmd) {
        if (await this.outOfUsage(ctx)) return this.holdStart(jobId, slotId, slot.logicalId);
        this.setIntent(slotId, "prepare");
        const prepared = await this.deps.effects.runCommand(ctx.spec.prepareCmd,
          this.commandEnv(ctx, { OPENRIG_SEAT_POOL_SLOT: sessionName, OPENRIG_SEAT_POOL_SLOT_CWD: cwd }), run.controller.signal);
        if (run.controller.signal.aborted) return false;
        if (prepared.exitCode !== 0) {
          this.record(jobId, true, "seat_pool_prepare_failed", { slot: slot.logicalId, exitCode: prepared.exitCode, error: prepared.error ?? null });
          return this.removeUnstarted(jobId, run, slotId);
        }
      }
      ctx = this.fresh(jobId, run);
      if (!ctx) return false;
      if (ctx.spec.drain) return this.removeUnstarted(jobId, run, slotId);
      if (await this.outOfUsage(ctx)) return this.holdStart(jobId, slotId, slot.logicalId);
      this.setIntent(slotId, "launch");
      const launched = await this.within(run, () => this.deps.effects.launchSlot({ rigId: ctx!.model.rigId, nodeId }));
      if (launched === TIMED_OUT) return this.gaveUp(jobId, run, "seat_pool_launch_failed", { slot: slot.logicalId });
      if (!launched.ok) {
        this.record(jobId, true, "seat_pool_launch_failed", { slot: slot.logicalId, code: launched.code, error: launched.error });
        return this.removeUnstarted(jobId, run, slotId);
      }
    }
    this.setIntent(slotId, null, "live");
    this.record(jobId, false, "seat_pool_launched", { slot: slot.logicalId, nodeId });
    return true;
  }

  /** Leaves a seat added but not launched as it is, while a seat of the pool is out of usage: a later plan launches it. */
  private holdStart(jobId: string, slotId: number, logicalId: string): boolean {
    this.setIntent(slotId, null);
    this.record(jobId, false, "seat_pool_held", { reason: "usage_limit", slot: logicalId });
    return true;
  }

  /**
   * Takes a free seat out of admission and removes it: the release command, a checked stop and a checked removal, all
   * under one lifecycle lease. A seat that gets work (an open row or a held message) before the stop, that isn't idle
   * since the moment the plan read (a message or a nudge woke it, even one that left nothing behind), or whose release
   * command refuses, returns to admission.
   */
  private async drain(jobId: string, run: Run, intent: Extract<SeatPoolIntent, { kind: "drain" }>): Promise<boolean> {
    const { slotId } = intent;
    const slot = this.deps.slots.get(slotId);
    // An adopted seat is never stopped or removed by the pool.
    if (!slot?.nodeId || slot.retiredAt || slot.opId === null) return true;
    const ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    const view = ctx.state.slots.find((s) => s.slotId === slotId);
    if (!view?.nodeExists) return this.removeUnstarted(jobId, run, slotId);
    const stillFree = (s: SeatPoolSlotView) => !holdsWork(s) && s.cause === "idle" && s.idleSince === intent.idleSince;
    if (slot.phase === "live") {
      if (!stillFree(view)) return true;
      this.setIntent(slotId, null, "draining");
    } else if (slot.phase !== "draining") return true;

    const nodeId = slot.nodeId;
    const sessionName = seatSessionName(slot.logicalId, ctx.model.rigName);
    const tookWork = (read: Context) => !read.state.slots.some((s) => s.slotId === slotId && stillFree(s));
    const returned = (): DrainOutcome => {
      this.setIntent(slotId, null, "live");
      return { stop: false, reason: "seat_pool_returned", notes: { why: "work_arrived" } };
    };
    const outcome = await this.within(run, () => this.deps.effects.lifecycle(nodeId, async (): Promise<DrainOutcome> => {
      const inLease = this.fresh(jobId, run);
      if (!inLease) return { stop: true, reason: null };
      if (tookWork(inLease)) return returned();
      const expected = this.deps.effects.stopPrecondition(nodeId);
      if (inLease.spec.releaseCmd) {
        this.setIntent(slotId, "release");
        const released = await this.deps.effects.runCommand(inLease.spec.releaseCmd, this.commandEnv(inLease, { OPENRIG_SEAT_POOL_SLOT: sessionName }), run.controller.signal);
        if (run.controller.signal.aborted) return { stop: true, reason: null };
        if (released.exitCode !== 0) {
          this.setIntent(slotId, null, "live");
          return { stop: false, reason: "seat_pool_returned", notes: { why: "release_refused", exitCode: released.exitCode, error: released.error ?? null } };
        }
        // The lease holds deliveries back, not queue writes: a row may have arrived while the command ran.
        const afterRelease = this.fresh(jobId, run);
        if (!afterRelease) return { stop: true, reason: null };
        if (tookWork(afterRelease)) return returned();
      }
      this.setIntent(slotId, "stop");
      const stopped = await this.deps.effects.stopSlot({ sessionName, expected });
      if (!stopped.ok && !STOPPED_CODES.has(stopped.code)) {
        // A seat relaunched since the read is someone's seat now: it goes back to admission, untouched.
        if (stopped.code === "selection_changed") this.setIntent(slotId, null, "live");
        return { stop: false, failed: true, reason: "seat_pool_stop_failed", notes: { code: stopped.code, error: stopped.error } };
      }
      this.setIntent(slotId, "remove");
      const removed = await this.deps.effects.removeSlot({ rigId: inLease.model.rigId, nodeId, expected });
      if (!removed.ok && removed.code !== "node_not_found") {
        return { stop: false, failed: true, reason: "seat_pool_remove_failed", notes: { code: removed.code, error: removed.error } };
      }
      return { stop: false, reason: "seat_pool_removed", notes: {}, removed: true };
    }));
    if (outcome === TIMED_OUT) return this.gaveUp(jobId, run, "seat_pool_drain_failed", { slot: slot.logicalId });
    if (outcome.reason) this.record(jobId, outcome.failed === true, outcome.reason, { slot: slot.logicalId, ...outcome.notes });
    return outcome.removed ? this.forget(jobId, run, slotId) : !outcome.stop;
  }

  /** Removes a seat that never took work, or forgets one whose node is gone. */
  private async removeUnstarted(jobId: string, run: Run, slotId: number): Promise<boolean> {
    const slot = this.deps.slots.get(slotId);
    if (!slot || slot.retiredAt) return true;
    const ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    const nodeId = slot.nodeId ?? (slot.opId ? this.deps.effects.operationNodeId(slot.opId) : null);
    const view = ctx.state.slots.find((s) => s.slotId === slotId);
    if (!nodeId) {
      this.deps.slots.retire(slotId, this.at());
      this.record(jobId, false, "seat_pool_removed", { slot: slot.logicalId, why: "never_created" });
      return true;
    }
    if (slot.nodeId && view && !view.nodeExists) {
      if (!(await this.forget(jobId, run, slotId))) return false;
      if (this.deps.slots.get(slotId)?.retiredAt) this.record(jobId, false, "seat_pool_removed", { slot: slot.logicalId, why: "node_gone" });
      return true;
    }
    // An admitted seat may hold work: it leaves only by draining.
    if (slot.phase === "live" || slot.phase === "draining") return true;
    if (!slot.nodeId) this.deps.slots.update(slotId, { nodeId });
    if (this.deps.effects.sessionRunning(nodeId)) {
      // Launched after all: it is a live seat now, and leaves only by draining.
      this.setIntent(slotId, null, "live");
      this.record(jobId, false, "seat_pool_launched", { slot: slot.logicalId, nodeId });
      return true;
    }
    this.setIntent(slotId, "remove");
    const removed = await this.within(run, () => this.deps.effects.removeSlot({ rigId: ctx.model.rigId, nodeId, expected: this.deps.effects.stopPrecondition(nodeId) }));
    if (removed === TIMED_OUT) return this.gaveUp(jobId, run, "seat_pool_remove_failed", { slot: slot.logicalId });
    if (!removed.ok && removed.code !== "node_not_found") {
      this.record(jobId, true, "seat_pool_remove_failed", { slot: slot.logicalId, code: removed.code, error: removed.error });
      return true;
    }
    this.record(jobId, false, "seat_pool_removed", { slot: slot.logicalId });
    return this.forget(jobId, run, slotId);
  }

  /**
   * Forgets a seat whose node is gone. For a seat the pool added, `remove_cmd` runs first, so what `prepare_cmd` set up
   * can be undone; while it refuses, the seat stays on record, counted against `max` but taking no row, and the next
   * plan, or the next start, runs it again. False stops the run.
   */
  private async forget(jobId: string, run: Run, slotId: number): Promise<boolean> {
    const slot = this.deps.slots.get(slotId);
    if (!slot || slot.retiredAt) return true;
    const ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    if (slot.opId !== null && ctx.spec.removeCmd) {
      const sessionName = seatSessionName(slot.logicalId, ctx.model.rigName);
      const result = await this.deps.effects.runCommand(ctx.spec.removeCmd, this.commandEnv(ctx, { OPENRIG_SEAT_POOL_SLOT: sessionName }), run.controller.signal);
      if (run.controller.signal.aborted) return false;
      if (result.exitCode !== 0) {
        this.record(jobId, true, "seat_pool_remove_cmd_failed", { slot: slot.logicalId, exitCode: result.exitCode, error: result.error ?? null });
        return true;
      }
    }
    this.deps.slots.retire(slotId, this.at());
    return true;
  }

  /**
   * Reaps a seat whose session is gone, under its lifecycle lease, after checking again that it is gone: a seat that
   * came back since the plan is left alone.
   *
   * The release command runs first, as for a drained seat: the session is gone, but its work may not be saved. When it
   * refuses, the seat is launched again instead (its session resumes where it can), and reaped without asking again
   * only if that launch fails. Then the seat is cleaned, which checks in tmux that the session is gone before anything
   * else changes; then `reap_cmd` runs; then each in-progress row is unclaimed and every open row is moved back to the
   * model (a pending one with a wake), each only if it is still addressed to the seat, with the reason `reap:<seat>`. A
   * row that a reap already moved once goes to the model's escalation owner instead, so a row that keeps losing its
   * seat stops cycling through the pool. Then the node is removed with the `expected` read at the start, and last the
   * seat is forgotten (forget). A failed step stops the reap there; the next plan picks it up again.
   */
  private async reap(jobId: string, run: Run, slotId: number, released = false): Promise<boolean> {
    const slot = this.deps.slots.get(slotId);
    if (!slot?.nodeId || slot.retiredAt || slot.phase !== "live") return true;
    const ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    const nodeId = slot.nodeId;
    const sessionName = seatSessionName(slot.logicalId, ctx.model.rigName);
    const outcome = await this.within(run, () => this.deps.effects.lifecycle(nodeId, async (): Promise<DrainOutcome> => {
      const inLease = this.fresh(jobId, run);
      if (!inLease) return { stop: true, reason: null };
      const view = inLease.state.slots.find((s) => s.slotId === slotId);
      if (!view || !seatPoolReapable(view)) return { stop: false, reason: null };
      const expected = this.deps.effects.stopPrecondition(nodeId);
      const failed = (step: string, notes: Record<string, unknown>): DrainOutcome => {
        this.setIntent(slotId, null);
        return { stop: false, failed: true, reason: "seat_pool_reap_failed", notes: { step, ...notes } };
      };

      if (!released && inLease.spec.releaseCmd) {
        this.setIntent(slotId, "release");
        const release = await this.deps.effects.runCommand(inLease.spec.releaseCmd, this.commandEnv(inLease, { OPENRIG_SEAT_POOL_SLOT: sessionName }), run.controller.signal);
        if (run.controller.signal.aborted) return { stop: true, reason: null };
        if (release.exitCode !== 0) {
          this.setIntent(slotId, null);
          return { stop: false, reason: null, refused: { exitCode: release.exitCode, error: release.error ?? null } };
        }
      }
      this.setIntent(slotId, "reap");
      const cleaned = await this.deps.effects.cleanSlot({ sessionName });
      if (!cleaned.ok && cleaned.code !== "nothing_to_clean") return failed("clean", { code: cleaned.code, error: cleaned.error });
      if (inLease.spec.reapCmd) {
        const reaped = await this.deps.effects.runCommand(inLease.spec.reapCmd, this.commandEnv(inLease, { OPENRIG_SEAT_POOL_SLOT: sessionName }), run.controller.signal);
        if (run.controller.signal.aborted) return { stop: true, reason: null };
        if (reaped.exitCode !== 0) return failed("reap_cmd", { exitCode: reaped.exitCode, error: reaped.error ?? null });
      }
      const reason = `${SEAT_POOL_REAP_REASON}${sessionName}`;
      const rows: Array<{ qitemId: string; to: string; escalated?: true }> = [];
      for (const row of this.deps.effects.openRows(sessionName)) {
        if (row.state === "in-progress") {
          const unclaimed = this.deps.effects.unclaimRow({ qitemId: row.qitemId, seat: sessionName, reason });
          if (!unclaimed.ok && !UNCLAIM_MOVED_ON.has(unclaimed.code)) return failed("unclaim", { qitemId: row.qitemId, code: unclaimed.code, error: unclaimed.error });
        }
        let to = inLease.model.sessionName;
        let escalated = false;
        if (this.deps.effects.reapCount(row.qitemId) > 0) {
          const route = this.deps.effects.escalationOwner(inLease.model.sessionName);
          if (route.owner) [to, escalated] = [route.owner, true];
          else this.record(jobId, true, "seat_pool_escalation_failed", { seat: sessionName, qitemId: row.qitemId, code: "no_escalation_owner", error: route.reason });
        }
        const moved = this.deps.effects.moveRow({ qitemId: row.qitemId, from: sessionName, to, reason, state: row.state === "blocked" ? "blocked" : "pending" });
        if (moved.ok) rows.push({ qitemId: row.qitemId, to, ...(escalated ? { escalated: true as const } : {}) });
        else if (!ROW_MOVED_ON.has(moved.code)) return failed("move", { qitemId: row.qitemId, code: moved.code, error: moved.error });
      }
      this.setIntent(slotId, "remove");
      const removed = await this.deps.effects.removeSlot({ rigId: inLease.model.rigId, nodeId, expected });
      if (!removed.ok && removed.code !== "node_not_found") return failed("remove", { code: removed.code, error: removed.error, rows });
      return { stop: false, reason: "seat_pool_reaped", notes: { rows }, removed: true };
    }));
    if (outcome === TIMED_OUT) return this.gaveUp(jobId, run, "seat_pool_reap_failed", { slot: slot.logicalId });
    if (outcome.refused) {
      const inRun = this.fresh(jobId, run);
      if (!inRun) return false;
      const launched = await this.relaunch(jobId, run, inRun, sessionName, slot.logicalId, { why: "release_refused", ...outcome.refused });
      if (launched === "failed") return this.reap(jobId, run, slotId, true);
      return launched !== "timed_out";
    }
    if (outcome.reason) this.record(jobId, outcome.failed === true, outcome.reason, { slot: slot.logicalId, ...outcome.notes });
    return outcome.removed ? this.forget(jobId, run, slotId) : !outcome.stop;
  }

  /**
   * Moves a waiting row to a seat that is still free. A seat that took work since the plan is left to the next plan, and
   * a pool set to drain since the plan moves nothing.
   */
  private dispatch(jobId: string, run: Run, intent: Extract<SeatPoolIntent, { kind: "dispatch" }>): boolean {
    const ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    if (ctx.spec.drain) return true;
    const slot = ctx.state.slots.find((s) => s.slotId === intent.slotId);
    if (!slot || slot.phase !== "live" || !slot.nodeExists || slot.cause !== "idle" || holdsWork(slot)) return true;
    const notes = { qitemId: intent.qitemId, from: intent.from, to: slot.sessionName };
    const moved = this.deps.effects.moveRow({ ...notes, reason: `seat pool ${jobId}: ${intent.from} had not taken it; ${slot.sessionName} was free` });
    if (moved.ok) this.record(jobId, false, "seat_pool_dispatched", notes);
    else if (ROW_MOVED_ON.has(moved.code)) this.record(jobId, false, "seat_pool_dispatch_skipped", { ...notes, code: moved.code });
    else this.record(jobId, true, "seat_pool_dispatch_failed", { ...notes, code: moved.code, error: moved.error });
    return true;
  }

  /**
   * Wakes a seat holding a waiting row, by why it isn't taking it: launches it when it stopped, escalates when it needs
   * input. Nothing happens when the seat's cause changed since the plan, or the pool was set to drain. False stops the
   * run.
   */
  private async wake(jobId: string, run: Run, intent: Extract<SeatPoolIntent, { kind: "relaunch" | "escalate" }>): Promise<boolean> {
    const ctx = this.fresh(jobId, run);
    if (!ctx) return false;
    if (ctx.spec.drain) return true;
    const slot = ctx.state.slots.find((s) => s.sessionName === intent.seat && s.phase === "live" && s.nodeExists);
    const seat = intent.seat === ctx.model.sessionName
      ? { logicalId: ctx.model.logicalId, nodeId: ctx.model.nodeId, cause: ctx.state.modelCause }
      : slot ? { logicalId: slot.logicalId, nodeId: slot.nodeId!, cause: slot.cause } : null;
    if (!seat) return true;

    if (intent.kind === "relaunch") {
      if (seat.cause !== "stopped" || seat.nodeId !== intent.nodeId) return true;
      return await this.relaunch(jobId, run, ctx, intent.seat, seat.logicalId) !== "timed_out";
    }

    if (seat.cause !== "needs_input") return true;
    const escalated = await this.within(run, () => this.deps.effects.escalate({ jobId, seat: intent.seat, model: ctx.model.sessionName, qitemIds: intent.qitemIds }));
    if (escalated === TIMED_OUT) return this.gaveUp(jobId, run, "seat_pool_escalation_failed", { seat: intent.seat });
    if (!escalated.ok) this.record(jobId, true, "seat_pool_escalation_failed", { seat: intent.seat, code: escalated.code, error: escalated.error });
    // An escalation about the seat is already open: it is the one, and nothing new is recorded.
    else if (!escalated.existing) {
      this.record(jobId, false, "seat_pool_escalated", { seat: intent.seat, to: escalated.destination, qitemId: escalated.qitemId, rows: intent.qitemIds });
    }
    return true;
  }

  /**
   * Launches a stopped seat again, as `rig launch` does, within the launch budget and only while no seat of the pool is
   * out of usage. `held` started nothing; the next plan tries again.
   */
  private async relaunch(jobId: string, run: Run, ctx: Context, seat: string, logicalId: string, notes: Record<string, unknown> = {}):
    Promise<"launched" | "failed" | "held" | "timed_out"> {
    if (await this.outOfUsage(ctx)) {
      this.record(jobId, false, "seat_pool_held", { reason: "usage_limit", seat, ...notes });
      return "held";
    }
    const launched = await this.withLaunchRoom(jobId, run, { seat, ...notes },
      () => this.within(run, () => this.deps.effects.relaunchSeat({ rigId: ctx.model.rigId, logicalId })));
    if (launched === NO_ROOM) return "held";
    if (launched === TIMED_OUT) {
      this.gaveUp(jobId, run, "seat_pool_relaunch_failed", { seat, ...notes });
      return "timed_out";
    }
    if (!launched.ok) {
      this.record(jobId, true, "seat_pool_relaunch_failed", { seat, code: launched.code, error: launched.error, ...notes });
      return "failed";
    }
    this.record(jobId, false, "seat_pool_relaunched", { seat, ...notes });
    return "launched";
  }

  /** Asks the demand command for the next plan. A failure keeps the previous answer, which ages until it holds the pool. */
  private async refreshDemand(jobId: string, run: Run): Promise<void> {
    const ctx = this.fresh(jobId, run);
    if (!ctx?.spec.demandCmd) return;
    const slots = ctx.state.slots.map((slot) => ({ session: seatSessionName(slot.logicalId, ctx.model.rigName), phase: slot.phase }));
    const result = await this.deps.effects.runCommand(ctx.spec.demandCmd, this.commandEnv(ctx, { OPENRIG_SEAT_POOL_SLOTS: JSON.stringify(slots) }), run.controller.signal);
    if (run.controller.signal.aborted) return;
    let desired: unknown;
    try { desired = result.exitCode === 0 ? (JSON.parse(result.stdout) as { desired?: unknown }).desired : undefined; } catch { desired = undefined; }
    if (Number.isInteger(desired) && (desired as number) >= 0) {
      this.samples.set(jobId, { desired: desired as number, sampledAt: this.at() });
      return;
    }
    this.record(jobId, true, "seat_pool_demand_failed", {
      exitCode: result.exitCode, error: result.error ?? (result.exitCode === 0 ? "stdout is not {\"desired\": <whole number>}" : null),
    });
  }
}
