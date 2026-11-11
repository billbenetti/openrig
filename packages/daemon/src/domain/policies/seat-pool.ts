// The seat-pool policy: elastic seats cloned from one model member.
//
// A pool job targets its model seat (`<pod>-<member>@<rig>`) and keeps between `min` and `max` extra seats beside it,
// named `<member>-s<n>`. Evaluation is pure planning: it reads the pool's state through an injected reader, decides
// what should change (add N seats, finish starting a seat, drain a free seat, remove a seat, move a waiting row to a
// free seat, wake a seat holding a waiting row, reap a seat whose session is gone, give an adopted seat back) and
// hands those intents to the executor, which applies them outside the scheduler tick (seat-pool-executor.ts). The
// policy itself never creates, launches, stops, removes, moves or sends anything, so one slow launch can't hold every
// other job's evaluation.

import type { Policy, PolicyEvaluation, PolicyJob } from "./types.js";

export const SEAT_POOL_POLICY_NAME = "seat-pool";

/** A command the executor runs: an argv (never a shell string) and how long it may take. */
export interface SeatPoolCommand {
  argv: string[];
  timeoutSeconds: number;
}

export interface SeatPoolSpec {
  min: number;
  max: number;
  cooldownSeconds: number;
  idleGraceSeconds: number;
  /** How long a pending row waits for the seat it is addressed to before the pool moves it or wakes that seat. */
  dispatchAfterSeconds: number;
  /** Only a row to the model carrying this tag is moved to a seat of the pool; null: every row to the model. */
  dispatchTag: string | null;
  /** True empties the pool as its seats become free, and adds none: the kill switch. */
  drain: boolean;
  /** How long a drain waits for the pool's seats to become free before the executor takes the rest out (the forced
   *  pass): their open rows go back to the model, their sessions stop and their nodes are removed. 0: no deadline. */
  drainDeadlineSeconds: number;
  /** Existing seats (logical ids) this pool counts and gives rows to like its own, but never stops or removes. */
  adopt: string[];
  /** Absolute working directory for a new seat; `{slot}` (required) and `{rig}` are replaced. */
  cwdTemplate: string;
  /** Prints `{"desired": N}`: how many extra seats the pool should have. Absent: the rows waiting for a seat. */
  demandCmd: SeatPoolCommand | null;
  /** Must exit 0 before a free seat is stopped and removed. */
  releaseCmd: SeatPoolCommand | null;
  /** Runs after a seat is added and before its first launch, so it can prepare the seat's working directory. */
  prepareCmd: SeatPoolCommand | null;
  /** Runs before the rows of a seat whose session is gone are moved, so the work they track can be released. */
  reapCmd: SeatPoolCommand | null;
  /** Runs once the node of a seat the pool added is gone, so what `prepareCmd` set up can be undone; the seat stays on
   *  record until it exits 0. */
  removeCmd: SeatPoolCommand | null;
  /** Where an escalation about a seat of the pool goes (a needs-input escalation, and a row a reap moves a second
   *  time), in place of the model's escalation owner. Null: the model's escalation owner, as without the field. */
  escalationOwner: string | null;
  /** A seat whose only open row it parked itself on a wait of its own may receive one row more (seatPoolCanReceive).
   *  Absent (no such property) when the key is not in the context: the rule is off, as before the key. */
  dispatchToParked?: SeatPoolDispatchToParked;
}

/** `dispatch_to_parked` in the job's context: when a seat parked on its own wait counts as a receiver of one more row. */
export interface SeatPoolDispatchToParked {
  /** `max_wake_s`: the longest timer of the park that still counts as the seat's own short wait (default 3600). */
  maxWakeSeconds: number;
  /** `max_claim_age_s`: how long ago the seat may have taken the parked row (default 21600, 6 h). */
  maxClaimAgeSeconds: number;
  /** `allow_background_work`: false keeps out a seat with a background shell (default true). */
  allowBackgroundWork: boolean;
  /** `parked_max_load_per_cpu_percent`: above this 1-minute load per CPU (in percent) the executor gives a parked seat
   *  no row; 0 (the default) checks no load. A free seat is never checked. */
  parkedMaxLoadPerCpuPercent: number;
}

const SPEC_KEYS = new Set([
  "min", "max", "cooldown_s", "idle_grace_s", "dispatch_after_s", "dispatch_tag", "drain", "drain_deadline_s", "adopt", "cwd_template",
  "demand_cmd", "release_cmd", "prepare_cmd", "reap_cmd", "remove_cmd", "escalation_owner", "dispatch_to_parked",
]);
const DEFAULT_COOLDOWN_SECONDS = 60;
const DEFAULT_IDLE_GRACE_SECONDS = 300;
const DEFAULT_DISPATCH_AFTER_SECONDS = 60;
const DEFAULT_COMMAND_TIMEOUT_SECONDS = 60;
/** 15 minutes: the window a working seat has to finish on its own once the pool is set to drain. */
export const DEFAULT_DRAIN_DEADLINE_SECONDS = 900;
/**
 * 1 day: the shortest history retention the settings allow. The drain's clock lives in the job's watchdog history,
 * which is pruned (queue-retention.ts, pruneWatchdogHistory): a deadline no longer than the shortest retention keeps
 * `seat_pool_drain_started` until the deadline passes, in any configuration. A drain left open for longer than the
 * retention window can still lose its entries; it then starts a new deadline (a new `started`), and a stop refuses
 * again until the new forced pass is recorded: the safe side, never a stop without the proof of the pass.
 */
export const MAX_DRAIN_DEADLINE_SECONDS = 86_400;

type SpecProblem = { field: string; message: string };

function wholeNumber(value: unknown, field: string, fallback?: number): number | SpecProblem {
  if (value === undefined && fallback !== undefined) return fallback;
  return Number.isInteger(value) && (value as number) >= 0
    ? value as number
    : { field, message: `seat-pool: ${field} must be a whole number of 0 or more` };
}

function command(value: unknown, field: string): SeatPoolCommand | null | SpecProblem {
  if (value === undefined) return null;
  const problem = { field, message: `seat-pool: ${field} must be { argv: [program, ...args], timeout_s: <seconds> }` };
  if (!value || typeof value !== "object" || Array.isArray(value)) return problem;
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some((key) => key !== "argv" && key !== "timeout_s")) return problem;
  const argv = record["argv"];
  if (!Array.isArray(argv) || argv.length === 0 || argv.some((part) => typeof part !== "string") || !(argv[0] as string).trim()) return problem;
  const timeout = record["timeout_s"] ?? DEFAULT_COMMAND_TIMEOUT_SECONDS;
  if (!Number.isInteger(timeout) || (timeout as number) <= 0) {
    return { field: `${field}.timeout_s`, message: `seat-pool: ${field}.timeout_s must be a positive whole number of seconds` };
  }
  return { argv: argv as string[], timeoutSeconds: timeout as number };
}

const DISPATCH_TO_PARKED = "dispatch_to_parked";
/** The fields of `dispatch_to_parked`: the bounds of each whole number, and its default. */
const DISPATCH_TO_PARKED_NUMBERS = {
  max_wake_s: { min: 60, max: 86_400, fallback: 3600 },
  max_claim_age_s: { min: 600, max: 172_800, fallback: 21_600 },
  parked_max_load_per_cpu_percent: { min: 0, max: 10_000, fallback: 0 },
} as const;
const DISPATCH_TO_PARKED_KEYS = new Set([...Object.keys(DISPATCH_TO_PARKED_NUMBERS), "allow_background_work"]);

/** `dispatch_to_parked`: an object (`{}` takes every default). A field given as null is wrong, as for the other keys:
 *  only a field left out takes its default. */
function dispatchToParked(value: unknown): SeatPoolDispatchToParked | SpecProblem {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { field: DISPATCH_TO_PARKED, message: `seat-pool: ${DISPATCH_TO_PARKED} must be an object ({} turns it on with its defaults)` };
  }
  const record = value as Record<string, unknown>;
  const unknown = Object.keys(record).filter((key) => !DISPATCH_TO_PARKED_KEYS.has(key));
  if (unknown.length > 0) {
    return {
      field: DISPATCH_TO_PARKED,
      message: `seat-pool: ${DISPATCH_TO_PARKED} has unknown field ${unknown.join(", ")}; known fields are ${[...DISPATCH_TO_PARKED_KEYS].join(", ")}`,
    };
  }
  const numbers: Record<string, number> = {};
  for (const [key, { min, max, fallback }] of Object.entries(DISPATCH_TO_PARKED_NUMBERS)) {
    const field = record[key];
    if (field === undefined) { numbers[key] = fallback; continue; }
    if (!Number.isInteger(field) || (field as number) < min || (field as number) > max) {
      return { field: `${DISPATCH_TO_PARKED}.${key}`, message: `seat-pool: ${DISPATCH_TO_PARKED}.${key} must be a whole number from ${min} to ${max}` };
    }
    numbers[key] = field as number;
  }
  const allow = record["allow_background_work"];
  if (allow !== undefined && typeof allow !== "boolean") {
    return { field: `${DISPATCH_TO_PARKED}.allow_background_work`, message: `seat-pool: ${DISPATCH_TO_PARKED}.allow_background_work must be true or false` };
  }
  return {
    maxWakeSeconds: numbers["max_wake_s"]!, maxClaimAgeSeconds: numbers["max_claim_age_s"]!, allowBackgroundWork: allow ?? true,
    parkedMaxLoadPerCpuPercent: numbers["parked_max_load_per_cpu_percent"]!,
  };
}

const isProblem = (value: unknown): value is SpecProblem =>
  !!value && typeof value === "object" && "message" in value && "field" in value;

/** The spec in the job's `context:` block, or what is wrong with it. Registration and update check it the same way. */
export function parseSeatPoolSpec(context: Record<string, unknown>): { spec: SeatPoolSpec } | { problem: SpecProblem } {
  const unknown = Object.keys(context).filter((key) => !SPEC_KEYS.has(key));
  if (unknown.length > 0) {
    return { problem: { field: unknown[0]!, message: `seat-pool: unknown field ${unknown.join(", ")}; known fields are ${[...SPEC_KEYS].join(", ")}` } };
  }
  const min = wholeNumber(context["min"], "min", 0);
  const max = wholeNumber(context["max"], "max");
  const cooldownSeconds = wholeNumber(context["cooldown_s"], "cooldown_s", DEFAULT_COOLDOWN_SECONDS);
  const idleGraceSeconds = wholeNumber(context["idle_grace_s"], "idle_grace_s", DEFAULT_IDLE_GRACE_SECONDS);
  const dispatchAfterSeconds = wholeNumber(context["dispatch_after_s"], "dispatch_after_s", DEFAULT_DISPATCH_AFTER_SECONDS);
  const drainDeadlineSeconds = wholeNumber(context["drain_deadline_s"], "drain_deadline_s", DEFAULT_DRAIN_DEADLINE_SECONDS);
  for (const value of [min, max, cooldownSeconds, idleGraceSeconds, dispatchAfterSeconds, drainDeadlineSeconds]) if (isProblem(value)) return { problem: value };
  if ((drainDeadlineSeconds as number) > MAX_DRAIN_DEADLINE_SECONDS) {
    return { problem: { field: "drain_deadline_s", message: `seat-pool: drain_deadline_s must be at most ${MAX_DRAIN_DEADLINE_SECONDS} (1 day, the shortest history retention); 0 means no deadline` } };
  }
  if ((max as number) < (min as number)) return { problem: { field: "max", message: "seat-pool: max must be at least min" } };
  const dispatchTag = context["dispatch_tag"] ?? null;
  if (dispatchTag !== null && (typeof dispatchTag !== "string" || !dispatchTag.trim())) {
    return { problem: { field: "dispatch_tag", message: "seat-pool: dispatch_tag must be a tag" } };
  }
  const drain = context["drain"] ?? false;
  if (typeof drain !== "boolean") return { problem: { field: "drain", message: "seat-pool: drain must be true or false" } };
  const adopt = context["adopt"] ?? [];
  if (!Array.isArray(adopt) || adopt.some((id) => typeof id !== "string" || !id.trim())) {
    return { problem: { field: "adopt", message: "seat-pool: adopt must be a list of logical ids" } };
  }
  const cwdTemplate = context["cwd_template"];
  if (typeof cwdTemplate !== "string" || !cwdTemplate.startsWith("/") || !cwdTemplate.includes("{slot}")) {
    return { problem: { field: "cwd_template", message: "seat-pool: cwd_template must be an absolute path containing {slot}, so no two seats share a working directory" } };
  }
  const placeholder = cwdTemplate.match(/\{(?!slot\}|rig\})[^}]*\}/);
  if (placeholder) return { problem: { field: "cwd_template", message: `seat-pool: cwd_template may use only {slot} and {rig} (found ${placeholder[0]})` } };
  const escalationOwner = context["escalation_owner"] ?? null;
  if (escalationOwner !== null && (typeof escalationOwner !== "string" || !/^[^\s@]+@[^\s@]+$/.test(escalationOwner))) {
    return { problem: { field: "escalation_owner", message: "seat-pool: escalation_owner must be a seat address (<session>@<rig>)" } };
  }
  const commands: Record<string, SeatPoolCommand | null> = {};
  for (const field of ["demand_cmd", "release_cmd", "prepare_cmd", "reap_cmd", "remove_cmd"]) {
    const parsed = command(context[field], field);
    if (isProblem(parsed)) return { problem: parsed };
    commands[field] = parsed;
  }
  let parkedRule: SeatPoolDispatchToParked | undefined;
  if (context[DISPATCH_TO_PARKED] !== undefined) {
    const parsed = dispatchToParked(context[DISPATCH_TO_PARKED]);
    if (isProblem(parsed)) return { problem: parsed };
    parkedRule = parsed;
  }
  return {
    spec: {
      min: min as number, max: max as number, cooldownSeconds: cooldownSeconds as number, idleGraceSeconds: idleGraceSeconds as number,
      dispatchAfterSeconds: dispatchAfterSeconds as number, dispatchTag: dispatchTag as string | null, drain,
      drainDeadlineSeconds: drainDeadlineSeconds as number, adopt: [...new Set(adopt as string[])], cwdTemplate,
      demandCmd: commands["demand_cmd"]!, releaseCmd: commands["release_cmd"]!, prepareCmd: commands["prepare_cmd"]!,
      reapCmd: commands["reap_cmd"]!, removeCmd: commands["remove_cmd"]!, escalationOwner: escalationOwner as string | null,
      ...(parkedRule ? { dispatchToParked: parkedRule } : {}),
    },
  };
}

/** What the spec lacks to be evaluated, or null. A job update checks a new spec with this too. */
export function seatPoolSpecProblem(spec: Pick<PolicyJob, "context">): SpecProblem | null {
  const parsed = parseSeatPoolSpec(spec.context);
  return "problem" in parsed ? parsed.problem : null;
}

export function slotWorkingDirectory(template: string, rigName: string, slotMemberId: string): string {
  return template.replaceAll("{slot}", slotMemberId).replaceAll("{rig}", rigName);
}

/** The history entries that time a drain. Recorded by the executor (started, ended, expired) and by the job's stop
 *  (left); the drain's clock is read back from them, so a daemon restart doesn't reset it. */
export const SEAT_POOL_DRAIN_STARTED = "seat_pool_drain_started";
export const SEAT_POOL_DRAIN_ENDED = "seat_pool_drain_ended";
export const SEAT_POOL_DRAIN_EXPIRED = "seat_pool_drain_expired";
export const SEAT_POOL_LEFT = "seat_pool_left";
/** The forced pass is still waiting on a seat whose release command did not answer (exit 3 or 5, or no exit code: a
 *  timeout), or that worked on its rows while the command ran (work_arrived): what the pass did so far, so the next
 *  tick, or the next daemon, carries on with it. */
export const SEAT_POOL_DRAIN_RETRYING = "seat_pool_drain_retrying";
export const SEAT_POOL_DRAIN_EVENTS = [SEAT_POOL_DRAIN_STARTED, SEAT_POOL_DRAIN_ENDED, SEAT_POOL_DRAIN_EXPIRED, SEAT_POOL_DRAIN_RETRYING] as const;

/** One of the drain's history entries, oldest first. */
export interface SeatPoolDrainEvent {
  reason: string;
  notes: Record<string, unknown> | null;
}

/** Why a seat the forced pass could not take out is still there. `release_unavailable`: its release command did not
 *  answer (exit 3 or 5, or a timeout) on any try within SEAT_POOL_RELEASE_RETRY_SECONDS of the deadline.
 *  `work_arrived`: the seat worked on its rows while its release command ran (a claim, a block, a close, a handoff) on
 *  every try within that window, so it is working and keeps its rows. */
export type SeatPoolLeftWhy =
  | "release_refused" | "release_unavailable" | "work_arrived" | "delivery_guard" | "stop_failed" | "move_failed" | "remove_failed"
  | "remove_cmd_failed";

/** A seat the forced pass left, with the release command's exit code in that pass (null when it did not run).
 *  `stopped`: left after its stop (draining, never back in admission), as against left running. */
export interface SeatPoolLeftEntry { slot: string; why: string; exitCode?: number | null; stopped?: true }

/** A seat the forced pass left after its stop. A move or a removal only fails after the stop, so those count too when
 *  `stopped` is absent (an entry recorded before the field). */
export const seatPoolLeftStopped = (entry: SeatPoolLeftEntry): boolean =>
  entry.stopped === true || entry.why === "move_failed" || entry.why === "remove_failed";

/** A seat the forced pass asks again, with the release command's last exit code: one whose command did not answer,
 *  or one that worked on its rows while it ran (`why: work_arrived`; left as `release_unavailable` without it). */
export interface SeatPoolRetryWaiting { slot: string; exitCode: number | null; why?: "work_arrived" }

/** How long after a drain's deadline the forced pass keeps asking again a release command that did not answer, or a
 *  seat that worked on its rows while it ran. */
export const SEAT_POOL_RELEASE_RETRY_SECONDS = 600;

/**
 * Why the release command is asked, in OPENRIG_SEAT_POOL_RELEASE_REASON (release_cmd only): `idle` for a free seat the
 * pool drains (by `drain` or by `idle_grace_s`), `drain-deadline` for the forced pass at a drain's deadline (the seat's
 * open rows go to the model after its stop), `reap` for a seat whose session is gone.
 */
export const SEAT_POOL_RELEASE_REASON_ENV = "OPENRIG_SEAT_POOL_RELEASE_REASON";
export type SeatPoolReleaseReason = "idle" | "drain-deadline" | "reap";

/** The current drain: since its latest `started` with no `ended` after it. */
export interface SeatPoolDrainEpisode {
  startedAt: string;
  /** Null when the drain has no deadline (`drain_deadline_s: 0`). */
  deadlineAt: string | null;
  /** The forced pass's result, once it ran for this drain. */
  expired: { at: string; left: SeatPoolLeftEntry[]; moved: Array<{ qitemId: string; to: string }> } | null;
  /**
   * A forced pass still under way: the seats it left for good, the rows it moved, and the seats to ask again (their
   * release command did not answer, or they worked on their rows while it ran). Absent when no pass is waiting.
   */
  retry?: {
    left: SeatPoolLeftEntry[]; moved: Array<{ qitemId: string; to: string }>; waiting: SeatPoolRetryWaiting[];
    /** Until when the seats `waiting` are asked again (null in an entry recorded before the field). */
    retryUntil: string | null;
  };
}

const text = (value: unknown): string | null => (typeof value === "string" ? value : null);

/** Pure: folds the drain's history entries, oldest first, into the current drain, or null when none is on. */
export function seatPoolDrainEpisode(events: SeatPoolDrainEvent[]): SeatPoolDrainEpisode | null {
  let episode: SeatPoolDrainEpisode | null = null;
  for (const event of events) {
    const notes = event.notes ?? {};
    if (event.reason === SEAT_POOL_DRAIN_STARTED) {
      const startedAt = text(notes["startedAt"]);
      if (startedAt) episode = { startedAt, deadlineAt: text(notes["deadlineAt"]), expired: null };
    } else if (event.reason === SEAT_POOL_DRAIN_ENDED) {
      episode = null;
    } else if (event.reason === SEAT_POOL_DRAIN_EXPIRED && episode && text(notes["startedAt"]) === episode.startedAt) {
      episode.expired = {
        at: text(notes["at"]) ?? episode.deadlineAt ?? episode.startedAt,
        left: Array.isArray(notes["left"]) ? notes["left"] as SeatPoolLeftEntry[] : [],
        moved: Array.isArray(notes["moved"]) ? notes["moved"] as Array<{ qitemId: string; to: string }> : [],
      };
      delete episode.retry;
    } else if (event.reason === SEAT_POOL_DRAIN_RETRYING && episode && !episode.expired && text(notes["startedAt"]) === episode.startedAt) {
      episode.retry = {
        left: Array.isArray(notes["left"]) ? notes["left"] as SeatPoolLeftEntry[] : [],
        moved: Array.isArray(notes["moved"]) ? notes["moved"] as Array<{ qitemId: string; to: string }> : [],
        waiting: Array.isArray(notes["waiting"]) ? notes["waiting"] as SeatPoolRetryWaiting[] : [],
        retryUntil: text(notes["retryUntil"]),
      };
    }
  }
  return episode;
}

/** `adding`: recorded, the add not yet confirmed. `added`: the node exists without a session. `live`: launched and
 *  admitted. `draining`: out of admission, on its way to removal. `retired`: removed, kept so its name is never reused. */
export type SeatPoolSlotPhase = "adding" | "added" | "live" | "draining" | "retired";

export interface SeatPoolModel {
  rigId: string;
  rigName: string;
  podNamespace: string;
  memberId: string;
  logicalId: string;
  nodeId: string;
  sessionName: string;
}

/** Why a seat isn't taking a row, read before the pool moves the row or wakes the seat. `stopped`: no live session
 *  (for a seat of the pool, whatever its account's usage). `usage_limit`: its provider account is out of usage. `needs_input`: a prompt waits for an answer. `idle`: at its
 *  prompt with nothing running in the background. `working`: anything else, including not known. */
export type SeatPoolSeatCause = "stopped" | "usage_limit" | "needs_input" | "idle" | "working";

export interface SeatPoolSlotView {
  slotId: number;
  logicalId: string;
  sessionName: string;
  nodeId: string | null;
  phase: Exclude<SeatPoolSlotPhase, "retired">;
  /** The seat existed before any pool: the pool counts it and gives it rows, but never stops or removes it. */
  adopted: boolean;
  /** False once a linked node no longer exists (someone removed it). */
  nodeExists: boolean;
  /** Pending, in-progress and blocked rows addressed to the seat. */
  openRows: number;
  /** Messages the typing guard holds for the seat, not yet delivered. */
  heldMessages: number;
  /** When the seat last became idle at a prompt, with no needs-input and no background shell; null when it isn't. */
  idleSince: string | null;
  /** Meaningful for a live seat whose node exists. */
  cause: SeatPoolSeatCause;
  /** Only with `dispatch_to_parked` in the spec and the node present: idle at its prompt with no needs-input, whatever
   *  runs in the background. */
  atPrompt?: boolean;
  /** Only with `dispatch_to_parked` in the spec and the node present: a background shell runs in the seat. */
  backgroundWork?: boolean;
  /** Only with `dispatch_to_parked` in the spec, the row facts read and exactly one open row, blocked: that row's park. */
  parked?: ParkFacts;
}

/** The park of a seat's only open row, a blocked one: what seatPoolCanReceive checks to call it the seat's own wait. */
export interface ParkFacts {
  qitemId: string;
  /** The row's blocked_on. */
  blockedOn: string;
  /** The interval of the job behind the row's current wake when that wake is a timer, whatever the job's state now;
   *  null with no wake, or a watchdog or blocker wake. */
  timerIntervalS: number | null;
  /** The current wake is a timer, armed, live and not consumed: it has not fired yet (a park timer fires once). */
  timerArmedLive: boolean;
  /** Who made the transition that armed the current wake, as recorded; null with no wake. */
  parkActor: string | null;
  /** When that transition was made; null with no wake. */
  parkedAt: string | null;
  /** When the row was taken: its claim, or else its first move to in-progress by one of its destination's addresses. */
  takenAt: string | null;
}

/** How a seat may receive a waiting row: `free` (no open row, idle at its prompt) or `parked` (dispatch_to_parked). */
export type SeatPoolReceiveMode = "free" | "parked";

/** A pending row addressed to the model, to one of the pool's seats or to a seat the pool removed. */
export interface SeatPoolPendingRow {
  qitemId: string;
  /** The session name it is addressed to, as the pool names that seat. */
  destination: string;
  /** When it reached `destination`: its latest reroute, or its creation. */
  waitingSince: string;
  tags: string[];
}

export interface SeatPoolState {
  model: SeatPoolModel | null;
  /** Why the model isn't taking a row. */
  modelCause: SeatPoolSeatCause;
  restoring: boolean;
  slots: SeatPoolSlotView[];
  /** The spec's adopt entries that exist in the rig and no pool has linked yet. */
  adoptable: Array<{ logicalId: string; nodeId: string }>;
  /** Pending rows addressed to the model, to the pool's seats and to the seats it removed, longest waiting first. */
  pending: SeatPoolPendingRow[];
  /** The demand command's last good answer, when the spec has one. */
  demand: { desired: number; sampledAt: string } | null;
  /** The latest add or removal in this pool. */
  lastScaleAt: string | null;
  /** The host's load per CPU (load1 / CPUs × 100), read for a spec whose `dispatch_to_parked` sets
   *  `parked_max_load_per_cpu_percent`; null when the host gives no reading. Absent: not read, and no seat is held back
   *  by the load in the plan. */
  hostLoadPerCpuPercent?: number | null;
}

export type SeatPoolIntent =
  | { kind: "adopt"; logicalId: string; nodeId: string }
  | { kind: "add"; count: number }
  /** Finish bringing up a seat that was added but not launched (also after a restart interrupted it). */
  | { kind: "launch"; slotId: number }
  /** Drain a seat that is still idle since `idleSince`, as the plan read it: one that was woken since is left. */
  | { kind: "drain"; slotId: number; idleSince: string | null }
  /** Remove a seat that never took work, or forget one whose node is gone. */
  | { kind: "remove"; slotId: number }
  /** Give back an adopted seat under drain: the pool's link ends, and the node and its session stay as they are. */
  | { kind: "unlink"; slotId: number }
  /** Move a waiting row to a free seat, only while it is still pending and addressed to `from`, and wake that seat.
   *  `mode: "parked"` (absent for a free seat): the seat was planned as parked on its own wait (seatPoolCanReceive). */
  | { kind: "dispatch"; qitemId: string; from: string; slotId: number; mode?: "parked" }
  /** Launch a stopped seat (the model or one of the pool's) that holds a waiting row. */
  | { kind: "relaunch"; seat: string; nodeId: string }
  /** Tell the model's escalation owner that a seat holding waiting rows needs input. */
  | { kind: "escalate"; seat: string; qitemIds: string[] }
  /** Take out a seat the pool added whose session is gone (see seatPoolReapable). */
  | { kind: "reap"; slotId: number };

export interface SeatPoolPlan {
  intents: SeatPoolIntent[];
  /** Why the pool holds, or what it is doing. */
  reason: string;
  /** Extra seats wanted, or null when the pool holds as it is. */
  target: number | null;
  current: number;
}

/** Whether a seat is to be reaped: the pool added it, it was live, and its session is gone. An adopted seat is never
 *  reaped: it was someone else's before the pool, so it is only launched again. The planner and, just before acting,
 *  the executor both ask this. */
export function seatPoolReapable(slot: SeatPoolSlotView): boolean {
  return !slot.adopted && slot.phase === "live" && slot.nodeExists && slot.cause === "stopped";
}

/**
 * Whether a seat may receive a waiting row now, and how; null when it may not. The planner, the executor's check just
 * before the move and the slots the demand command is given (`receives`) all ask this one function.
 *
 * Both ways need the pool not draining, the seat live, its node present, not to be reaped and no held message.
 * `free`: no open row and idle at its prompt (the rule from before `dispatch_to_parked`). `parked`, only with
 * `dispatch_to_parked`: a seat the pool added (not adopted), at its prompt (idle, or working only through a background
 * shell, which `allow_background_work: false` refuses), whose only open row is blocked on an `external:` gate with a
 * timer of at most `max_wake_s` still armed and live, armed by the seat itself, after it took the row (by claim or by
 * update) no more than `max_claim_age_s` ago. A seat with two open rows never receives: one row more is the ceiling.
 */
export function seatPoolCanReceive(spec: SeatPoolSpec, slot: SeatPoolSlotView, nowMs: number): SeatPoolReceiveMode | null {
  if (spec.drain || slot.phase !== "live" || !slot.nodeExists || seatPoolReapable(slot) || slot.heldMessages !== 0) return null;
  if (slot.openRows === 0 && slot.idleSince !== null && slot.cause === "idle") return "free";
  const rule = spec.dispatchToParked;
  if (!rule || slot.adopted) return null;
  if (slot.atPrompt !== true || (slot.cause !== "idle" && slot.cause !== "working")) return null;
  if (!rule.allowBackgroundWork && slot.backgroundWork === true) return null;
  const park = slot.parked;
  if (slot.openRows !== 1 || !park) return null;
  if (!park.blockedOn.startsWith("external:")) return null;
  if (!park.timerArmedLive || park.timerIntervalS === null || park.timerIntervalS > rule.maxWakeSeconds) return null;
  if (park.parkActor !== slot.sessionName) return null;
  if (park.takenAt === null || park.parkedAt === null) return null;
  const takenMs = Date.parse(park.takenAt);
  const parkedMs = Date.parse(park.parkedAt);
  // Written so that an unreadable time (NaN) refuses.
  if (!(parkedMs >= takenMs) || !(nowMs - takenMs <= rule.maxClaimAgeSeconds * 1000)) return null;
  return "parked";
}

/** Whether the host's load keeps a seat parked on its own wait from a 2nd row: `parked_max_load_per_cpu_percent` is set
 *  and the load per CPU is above it. An unknown load (null) holds nothing back. */
export function seatPoolParkedLoadOver(spec: SeatPoolSpec, loadPerCpuPercent: number | null): boolean {
  const limit = spec.dispatchToParked?.parkedMaxLoadPerCpuPercent ?? 0;
  return limit > 0 && loadPerCpuPercent !== null && loadPerCpuPercent > limit;
}

/**
 * How a seat may take a row now, with what the plan reads besides the seat's view: a seat whose provider account is out
 * of usage takes none (as its cause reads "usage_limit" in the plan's state), and a seat parked on its own wait takes
 * none while the host's load is above `parked_max_load_per_cpu_percent` (seatPoolParkedLoadOver). The planner, the
 * executor's recheck and the demand command's `receives` ask this, so a seat one of them would not give a row to is
 * never counted by another.
 */
export function seatPoolReceives(
  spec: SeatPoolSpec, slot: SeatPoolSlotView, nowMs: number, facts: { usageLimited?: boolean; loadPerCpuPercent?: number | null } = {},
): SeatPoolReceiveMode | null {
  const mode = seatPoolCanReceive(spec, facts.usageLimited ? { ...slot, cause: "usage_limit" } : slot, nowMs);
  return mode === "parked" && seatPoolParkedLoadOver(spec, facts.loadPerCpuPercent ?? null) ? null : mode;
}

/**
 * How many more seats the pool has room for. `max` counts every seat on record: an adopted seat, running or stopped,
 * and a seat the pool added, starting, live or draining (until it is retired). The planner sizes an add with this and
 * the executor checks it again before each seat, so every seat an add asks for fits, and the executor never adds one
 * the plan would not ask for. `adopting`: seats the same plan adopts before its add (the executor applies intents in
 * order), which take room as soon as they are linked.
 */
export function seatPoolRoom(spec: Pick<SeatPoolSpec, "max">, slots: readonly SeatPoolSlotView[], adopting = 0): number {
  return Math.max(0, spec.max - slots.length - adopting);
}

/** Whether the model or a seat of the pool is out of provider usage: the pool then adds and launches nothing. */
export function seatPoolOutOfUsage(state: Pick<SeatPoolState, "modelCause" | "slots">): boolean {
  return state.modelCause === "usage_limit" || state.slots.some((slot) => slot.nodeExists && slot.cause === "usage_limit");
}

/**
 * Pure: the same state and clock always give the same plan.
 *
 * A pending row waits once it has been at its seat for `dispatch_after_s` (since its creation or its latest reroute).
 * A waiting row addressed to the model (with `dispatch_tag`, only one carrying that tag) or to a seat the pool removed
 * goes to a free seat (live, idle at a prompt, no open row, no held message), the longest waiting first; one that finds
 * no free seat is demand. A waiting row the pool
 * doesn't move wakes the seat holding it by why that seat isn't taking it: a stopped seat is launched and one that needs
 * input is escalated; an idle one is left to the queue's own wakes, and one out of usage waits. Only pending rows are read,
 * so a blocked or claimed row is never moved or counted. A seat the pool added whose session is gone is reaped rather
 * than launched, even under drain or a usage limit; an adopted one is launched when it holds a waiting row, and while
 * it is stopped it is not counted among the seats taking work (`current`), though it still counts against `max`.
 *
 * The target is the demand (the demand command's last answer, or the waiting rows no free seat takes plus the seats
 * holding work), never below the seats holding work plus the seats still starting, clamped to min..max; with `drain`
 * it is 0, and nothing is moved or woken. An add asks for no more seats than the room `max` leaves (seatPoolRoom: every
 * seat on record counts, adopted or added, running, stopped or draining, but not one the same plan forgets or reaps);
 * with no room it is not asked for at all, and a plan left with nothing to do says so (`max_reached`, held). A demand answer older than `sampleMaxAgeMs` holds the pool's size as it is.
 * While a seat of the pool is out of usage, the pool adds no seat and launches none: neither a stopped seat nor one it
 * added and has not started, which waits as it is (and may still be removed). Only a free seat the pool added, idle
 * for `idle_grace_s`, is drained, the newest first; an adopted seat is never drained, and under `drain` it is unlinked.
 * Adds and new drains wait for `cooldown_s` after the last add or removal, except under `drain`. A seat that never
 * becomes free under `drain` is the executor's: once `drain_deadline_s` passes it takes the seat out (the forced pass).
 */
export function planSeatPool(spec: SeatPoolSpec, state: SeatPoolState, nowMs: number, sampleMaxAgeMs: number): SeatPoolPlan {
  const hold = (reason: string): SeatPoolPlan => ({ intents: [], reason, target: null, current: state.slots.length });
  if (!state.model) return hold("model_not_found");
  if (state.restoring) return hold("restore_in_progress");
  const model = state.model;

  const intents: SeatPoolIntent[] = [];
  if (!spec.drain) for (const seat of state.adoptable) intents.push({ kind: "adopt", ...seat });

  const present = state.slots.filter((slot) => slot.nodeId === null || slot.nodeExists);
  for (const gone of state.slots.filter((slot) => !present.includes(slot))) intents.push({ kind: "remove", slotId: gone.slotId });
  for (const slot of present.filter((s) => s.phase === "draining")) intents.push({ kind: "drain", slotId: slot.slotId, idleSince: slot.idleSince });
  for (const slot of present.filter(seatPoolReapable)) intents.push({ kind: "reap", slotId: slot.slotId });
  if (spec.drain) for (const slot of present.filter((s) => s.adopted)) intents.push({ kind: "unlink", slotId: slot.slotId });

  const starting = present.filter((slot) => slot.phase === "adding" || slot.phase === "added");
  // A stopped adopted seat takes no row and is not reaped: it isn't one of the pool's seats until it runs again. A seat
  // this plan reaps is on its way out: it isn't counted as current either, so the same plan can add its replacement
  // (the executor checks the room again after the reap, so the add never takes the pool past `max` if the reap launched the
  // seat instead).
  const live = present.filter((slot) => slot.phase === "live" && !(slot.adopted && slot.cause === "stopped") && !seatPoolReapable(slot));
  const idle = (slot: SeatPoolSlotView) => slot.openRows === 0 && slot.heldMessages === 0 && slot.idleSince !== null;
  const usageLimited = seatPoolOutOfUsage(state);

  const waiting = state.pending.filter((row) => nowMs - Date.parse(row.waitingSince) >= spec.dispatchAfterSeconds * 1000);
  const seatOf = new Map(present.map((slot) => [slot.sessionName, slot]));
  // A row to the model is the model's own unless the spec's dispatch tag marks it as the pool's to hand out.
  const movable = waiting.filter((row) => !seatOf.has(row.destination)
    && (row.destination !== model.sessionName || spec.dispatchTag === null || row.tags.includes(spec.dispatchTag)));
  // The free seats first, in today's order, then the seats parked on their own wait, by id: a parked seat never goes
  // before a seat that is really free. A parked seat stays out of `idle`, so it is neither free nor drained. While the
  // host's load is above the parked limit no parked seat receives: the row is demand, as with no seat to take it.
  const loadPerCpuPercent = state.hostLoadPerCpuPercent ?? null;
  const modes = new Map(live.map((slot) => [slot, spec.drain ? null : seatPoolReceives(spec, slot, nowMs, { loadPerCpuPercent })]));
  const receivers = [
    ...live.filter((slot) => modes.get(slot) === "free"),
    ...live.filter((slot) => modes.get(slot) === "parked").sort((a, b) => a.slotId - b.slotId),
  ];
  const dispatched = movable.slice(0, receivers.length);
  dispatched.forEach((row, i) => intents.push({
    kind: "dispatch", qitemId: row.qitemId, from: row.destination, slotId: receivers[i]!.slotId,
    ...(modes.get(receivers[i]!) === "parked" ? { mode: "parked" as const } : {}),
  }));
  const receiving = new Set(receivers.slice(0, dispatched.length));

  if (!spec.drain) {
    const holders = new Map<string, { nodeId: string; cause: SeatPoolSeatCause; rows: SeatPoolPendingRow[] }>();
    for (const row of waiting) {
      if (dispatched.includes(row)) continue;
      const slot = seatOf.get(row.destination);
      // A seat the pool added whose session is gone is reaped, not launched: its rows go back to the model.
      const holder = slot
        ? (slot.phase === "live" && slot.nodeId && !seatPoolReapable(slot) ? { nodeId: slot.nodeId, cause: slot.cause } : null)
        : row.destination === model.sessionName ? { nodeId: model.nodeId, cause: state.modelCause } : null;
      if (!holder) continue; // a seat the pool removed: the row waits for a free seat
      const entry = holders.get(row.destination) ?? { ...holder, rows: [] };
      entry.rows.push(row);
      holders.set(row.destination, entry);
    }
    for (const [seat, holder] of holders) {
      if (holder.cause === "stopped" && !usageLimited) intents.push({ kind: "relaunch", seat, nodeId: holder.nodeId });
      else if (holder.cause === "needs_input") {
        // With dispatch_tag, the model and an adopted seat are the pool's only for the rows that carry it: their other
        // rows are their own, so a seat waiting on its own work (a background workflow, a row of its own) is no alert.
        const ownsOnlyTagged = spec.dispatchTag !== null && (seat === model.sessionName || seatOf.get(seat)?.adopted === true);
        const rows = ownsOnlyTagged ? holder.rows.filter((r) => r.tags.includes(spec.dispatchTag!)) : holder.rows;
        if (rows.length > 0) intents.push({ kind: "escalate", seat, qitemIds: rows.map((r) => r.qitemId) });
      }
    }
  }

  const occupied = live.filter((slot) => !idle(slot) || receiving.has(slot));
  const free = live.filter((slot) => idle(slot) && !receiving.has(slot) && nowMs - Date.parse(slot.idleSince!) >= spec.idleGraceSeconds * 1000);
  const current = live.length + starting.length;
  const cooling = !spec.drain && state.lastScaleAt !== null && nowMs - Date.parse(state.lastScaleAt) < spec.cooldownSeconds * 1000;

  let target: number;
  let reason: string;
  if (spec.drain) {
    target = 0;
    reason = "draining";
  } else {
    let desired: number | null;
    if (spec.demandCmd) {
      const fresh = state.demand !== null && nowMs - Date.parse(state.demand.sampledAt) <= sampleMaxAgeMs;
      desired = fresh ? state.demand!.desired : null;
    } else {
      desired = movable.length - dispatched.length + occupied.length;
    }
    if (desired === null) {
      // No current answer from the demand command: keep what exists and finish what was started.
      if (!usageLimited) for (const slot of starting) intents.push({ kind: "launch", slotId: slot.slotId });
      return { intents, reason: "demand_sample_stale", target: null, current };
    }
    target = Math.min(spec.max, Math.max(spec.min, desired, occupied.length + starting.length));
    reason = target > current ? "scale_up" : target < current ? "scale_down" : "steady";
    if (usageLimited && target > current) {
      target = current;
      reason = "usage_limit";
    }
  }

  const surplus = Math.max(0, current - target);
  // An adopted seat was someone else's before the pool: the pool never stops or removes it.
  const drainOrder = free.filter((slot) => !slot.adopted).sort((a, b) => b.slotId - a.slotId);
  if (!cooling) for (const slot of drainOrder.slice(0, surplus)) intents.push({ kind: "drain", slotId: slot.slotId, idleSince: slot.idleSince });
  // A seat that never started holds no work. It goes only for the surplus free seats can't cover (a lowered max, or drain).
  const removeStarting = [...starting].sort((a, b) => b.slotId - a.slotId).slice(0, Math.max(0, surplus - drainOrder.length));
  for (const slot of starting) {
    if (removeStarting.includes(slot)) intents.push({ kind: "remove", slotId: slot.slotId });
    else if (!usageLimited) intents.push({ kind: "launch", slotId: slot.slotId });
  }
  if (usageLimited && starting.length > removeStarting.length) reason = "usage_limit";
  if (target > current && !cooling) {
    // The same room the executor checks before each seat: an add it would refuse is never asked for. A seat the same
    // plan forgets (its node is gone) or reaps is out of the count: the executor applies those intents before the add,
    // and checks the room again before each seat, so a reap that ends up launching its seat instead never takes the pool
    // past `max` (below it, the add may still start one seat more than the target, idle until idle_grace_s).
    const leaving = new Set([...state.slots.filter((slot) => !present.includes(slot)), ...present.filter(seatPoolReapable)]);
    const count = Math.min(target - current, seatPoolRoom(spec, state.slots.filter((slot) => !leaving.has(slot)), state.adoptable.length));
    if (count > 0) intents.push({ kind: "add", count });
    else reason = "max_reached";
  }
  if (cooling && target !== current) reason = "cooldown";
  return { intents, reason, target, current };
}

/** A plan with nothing to do for one of these says so in the job's history (`seat_pool_held`); any other is quiet.
 *  `max_reached`: the pool wants more seats than `max` leaves room for (an adopted seat, stopped or not, and a seat
 *  draining both take room), so a pool that doesn't grow says why. */
const HELD_REASONS = new Set(["model_not_found", "restore_in_progress", "demand_sample_stale", "usage_limit", "max_reached"]);

export interface SeatPoolPolicyDeps {
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState | Promise<SeatPoolState>;
  /** Hands the plan to the executor; false when the pool's previous plan is still being applied. */
  submit(jobId: string, intents: SeatPoolIntent[]): boolean;
  now?: () => number;
}

export function makeSeatPoolPolicy(deps: SeatPoolPolicyDeps): Policy {
  return {
    name: SEAT_POOL_POLICY_NAME,
    async evaluate(job: PolicyJob): Promise<PolicyEvaluation> {
      const parsed = parseSeatPoolSpec(job.context);
      // Registration and update refuse an invalid spec; one that is invalid here holds the pool rather than act on it.
      if ("problem" in parsed) return { action: "skip", reason: "seat_pool_spec_invalid", notes: { ...parsed.problem } };
      const state = await deps.readState({ jobId: job.jobId, targetSession: job.target.session, spec: parsed.spec });
      // A demand answer counts while it is no older than two evaluation intervals.
      const plan = planSeatPool(parsed.spec, state, deps.now?.() ?? Date.now(), 2 * job.intervalSeconds * 1000);
      // The executor also refreshes the demand sample, so a plan with nothing to do is still submitted.
      if (!deps.submit(job.jobId, plan.intents)) return { action: "skip", reason: "seat_pool_busy" };
      const notes = { target: plan.target, current: plan.current, reason: plan.reason, intents: plan.intents };
      if (plan.intents.length > 0) return { action: "skip", reason: "seat_pool_planned", notes };
      // A pool held by something it can't fix says so in its history; a pool with nothing to change is quiet.
      return HELD_REASONS.has(plan.reason)
        ? { action: "skip", reason: "seat_pool_held", notes }
        : { action: "skip", reason: "seat_pool_steady", notes };
    },
  };
}
