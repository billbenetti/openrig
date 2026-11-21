// The seat-pool policy: elastic seats cloned from one model member.
//
// A pool job targets its model seat (`<pod>-<member>@<rig>`) and keeps between `min` and `max` extra seats beside it,
// named `<member>-s<n>`. Evaluation is pure planning: it reads the pool's state through an injected reader, decides
// what should change (add N seats, finish starting a seat, drain a free seat, remove a seat, move a waiting row to a
// free seat, wake a seat holding a waiting row, reap a seat whose session is gone, give an adopted seat back) and
// hands those intents to the executor, which applies them outside the scheduler tick (seat-pool-executor.ts). The
// policy itself never creates, launches, stops, removes, moves or sends anything, so one slow launch can't hold every
// other job's evaluation.

import { SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S, seatPoolBusyFromProcess, seatPoolBusyProgram, seatPoolBusyWrapper } from "../seat-pool-busy.js";
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
  /** `no_seat_grace_s`: how long the pool may go without a live seat while the stuck sweep still leaves its queue (the
   *  model's rows carrying dispatch_tag) to it (T-007029). Absent (no property) when not given: 1800 s
   *  (SEAT_POOL_NO_SEAT_GRACE_DEFAULT_SECONDS). */
  noSeatGraceSeconds?: number;
  /** `priority_aging_s` (T-010078): a low row pending at least this long is handed out as routine, so it is not left
   *  waiting forever behind newer routine rows; the aging never goes past routine (it never makes an urgent row).
   *  Absent (no property) when not given: SEAT_POOL_PRIORITY_AGING_DEFAULT_SECONDS (2 h); 0 turns it off. */
  priorityAgingSeconds?: number;
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
  /** A seat whose open rows (one, or up to `max_open_rows`) it parked itself on waits of its own may receive one row
   *  more (seatPoolCanReceive).
   *  Absent (no such property) when the key is not in the context: the rule is off, as before the key. */
  dispatchToParked?: SeatPoolDispatchToParked;
  /** `context_limit_percent` (T-012383): a seat the pool added whose context usage, as its status-line sample reads it
   *  (SeatPoolSlotView.context), is at this percent or above, sampled less than SEAT_POOL_CONTEXT_MAX_AGE_SECONDS ago,
   *  takes no row, free or parked (seatPoolContextOver). Absent (no property) when not given: read as
   *  SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT (40); 0 turns it off. */
  contextLimitPercent?: number;
  /** `return_long_gates` (T-012673): a seat the pool added whose open rows are all parked on one of these long waits (a
   *  PR waiting for a person's review) for at least `min_blocked_s` hands them to a fixed seat and is free again
   *  (seatPoolLongGateRows). Absent (no such property) when the key is not in the context: the rule is off. */
  returnLongGates?: SeatPoolReturnLongGates;
}

/** `return_long_gates` in the job's context (T-012673): where a seat's rows parked on a long wait go. */
export interface SeatPoolReturnLongGates {
  /** `gates`: the waits that count, each an `external:` gate; a row's blocked_on matches one when it is that gate, or
   *  that gate followed by `-`, `:` or `/` and more (`external:github-review-<T>`). */
  gates: string[];
  /** `min_blocked_s` (default 1800): how long each row must have been blocked, without leaving `blocked`. */
  minBlockedSeconds: number;
  /** `to`: the fixed seat the rows go to (`<session>@<rig>`). */
  to: string;
  /** `max_rows` (default 4): the most open rows a seat may hold and still hand them all back. */
  maxRows: number;
  /** `ignore_busy_processes` (T-016495): programs of `dispatch_to_parked.busy_processes` that do not keep a seat's rows
   *  from going back when nothing else holds it (a holder that only waits for the seat's own release, as the setup's
   *  `sair-do-pool.py --segurar`). They still keep the seat from a row. Absent when not given: every busy program holds. */
  ignoreBusyProcesses?: string[];
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
  /** `refuse_gates`: a parked row whose gate (its blocked_on) contains one of these strings is not a wait the seat can
   *  take a 2nd row through (a heavy local run, such as an eval being frozen or a suite queued for the host's lock).
   *  Absent (no property) when the field is not given: every `external:` gate counts. */
  refuseGates?: string[];
  /** `busy_processes`: program names; a seat with a live process under its pane whose program (after the usual
   *  wrappers, seatPoolBusyCommand) is one of them takes no 2nd row (SeatPoolSlotView.busyProcess). Absent (no
   *  property) when the field is not given: processes are not read. With it, a free seat (no open row) is held back
   *  too (T-006251, B1): a process the seat's runtime does not track (reparented, nohup) gives the row back as well. */
  busyProcesses?: string[];
  /** `max_open_rows`: a seat may hold up to this many open rows and still receive, when ALL of them are blocked on its
   *  own `external:` waits, each passing the checks the single park passes (T-007036). Absent (no property) when the
   *  field is not given, which reads as 1: exactly one open row, the rule from before the field. */
  maxOpenRows?: number;
  /** `busy_orphan_max_age_s`, only with `busy_processes` (default 43200, 12 h): the oldest a matching process that left
   *  the pane's tree (found by the TMUX_PANE it inherited) may be and still hold the seat; an older one, or one started
   *  before the pane's current occupant (its runtime, started anew by a fresh launch: seatPoolPaneOccupantMs), holds nothing
   *  (T-006251, S2). A process still under the pane holds it whatever its age. Absent without `busy_processes`. */
  busyOrphanMaxAgeSeconds?: number;
  /** `refused_cooldown_s` (absent: no such property, read as DISPATCH_TO_PARKED_REFUSED_COOLDOWN_DEFAULT, 1800; 0 turns
   *  it off): a seat that gave a row the pool moved to it as parked straight back to the model, without taking it (no
   *  move to in-progress), is out of the parked receivers for this long, or until its open rows are no longer the ones
   *  it held when the row was moved; and no row it gave back that way (nor a successor of one) is moved to it again
   *  while it is parked. Once the seat is free (no open row) it takes any row, that one included. */
  refusedCooldownSeconds?: number;
}

const SPEC_KEYS = new Set([
  "min", "max", "cooldown_s", "idle_grace_s", "dispatch_after_s", "dispatch_tag", "drain", "drain_deadline_s", "no_seat_grace_s", "adopt", "cwd_template",
  "demand_cmd", "release_cmd", "prepare_cmd", "reap_cmd", "remove_cmd", "escalation_owner", "dispatch_to_parked",
  "priority_aging_s", "context_limit_percent", "return_long_gates",
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

/** `no_seat_grace_s`: its bounds, and the value when it is not given (T-007029). */
const NO_SEAT_GRACE = { min: 60, max: 86_400 } as const;
export const SEAT_POOL_NO_SEAT_GRACE_DEFAULT_SECONDS = 1800;

/** `priority_aging_s`: its bounds (0 turns the aging off; at most 7 days) and the value when it is not given (T-010078). */
const PRIORITY_AGING = { min: 0, max: 604_800 } as const;
export const SEAT_POOL_PRIORITY_AGING_DEFAULT_SECONDS = 7200;

/** `context_limit_percent`: its bounds (0 turns it off) and the value when it is not given (T-012383). */
const CONTEXT_LIMIT = { min: 0, max: 100 } as const;
export const SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT = 40;
/** A context sample at least this old says nothing about the seat (the freshness of the status-line sidecar, 10 min). */
export const SEAT_POOL_CONTEXT_MAX_AGE_SECONDS = 600;

/** The context usage at which a seat the pool added takes no row (T-012383); 0 when the limit is off. */
export function seatPoolContextLimit(spec: Pick<SeatPoolSpec, "contextLimitPercent">): number {
  return spec.contextLimitPercent ?? SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT;
}

/** How long a low row waits before it is handed out as routine (T-010078); 0 when the aging is off. */
export function seatPoolPriorityAging(spec: Pick<SeatPoolSpec, "priorityAgingSeconds">): number {
  return spec.priorityAgingSeconds ?? SEAT_POOL_PRIORITY_AGING_DEFAULT_SECONDS;
}

const DISPATCH_TO_PARKED = "dispatch_to_parked";
/** The fields of `dispatch_to_parked`: the bounds of each whole number, and its default. */
const DISPATCH_TO_PARKED_NUMBERS = {
  max_wake_s: { min: 60, max: 86_400, fallback: 3600 },
  max_claim_age_s: { min: 600, max: 172_800, fallback: 21_600 },
  parked_max_load_per_cpu_percent: { min: 0, max: 10_000, fallback: 0 },
} as const;
/** `max_open_rows`: its bounds; absent reads as 1 (DISPATCH_TO_PARKED_MAX_OPEN_ROWS_DEFAULT). */
const DISPATCH_TO_PARKED_MAX_OPEN_ROWS = { min: 1, max: 20 } as const;
export const DISPATCH_TO_PARKED_MAX_OPEN_ROWS_DEFAULT = 1;
/** `refused_cooldown_s`: its bounds; absent reads as DISPATCH_TO_PARKED_REFUSED_COOLDOWN_DEFAULT (seatPoolRefusedCooldown). */
const DISPATCH_TO_PARKED_REFUSED_COOLDOWN = { key: "refused_cooldown_s", min: 0, max: 86_400 } as const;
export const DISPATCH_TO_PARKED_REFUSED_COOLDOWN_DEFAULT = 1800;

/** The refusal memory's cooldown in seconds for a spec (0: off, as without dispatch_to_parked). */
export function seatPoolRefusedCooldown(spec: Pick<SeatPoolSpec, "dispatchToParked">): number {
  return spec.dispatchToParked ? spec.dispatchToParked.refusedCooldownSeconds ?? DISPATCH_TO_PARKED_REFUSED_COOLDOWN_DEFAULT : 0;
}
const DISPATCH_TO_PARKED_LISTS = ["refuse_gates", "busy_processes"] as const;
/** `busy_orphan_max_age_s`: read only with `busy_processes` (it means nothing without it). */
const BUSY_ORPHAN_MAX_AGE = { key: "busy_orphan_max_age_s", min: 60, max: 172_800, fallback: SEAT_POOL_BUSY_ORPHAN_MAX_AGE_S } as const;
const DISPATCH_TO_PARKED_KEYS = new Set([
  ...Object.keys(DISPATCH_TO_PARKED_NUMBERS), "allow_background_work", ...DISPATCH_TO_PARKED_LISTS, BUSY_ORPHAN_MAX_AGE.key, "max_open_rows",
  DISPATCH_TO_PARKED_REFUSED_COOLDOWN.key,
]);
/** The most entries a `dispatch_to_parked` list may hold, and the longest entry. */
const DISPATCH_TO_PARKED_LIST_MAX = 32;
const DISPATCH_TO_PARKED_ENTRY_MAX = 128;

/** A `dispatch_to_parked` list: strings, each non-empty, without whitespace, at most DISPATCH_TO_PARKED_ENTRY_MAX
 *  characters, at most DISPATCH_TO_PARKED_LIST_MAX of them; a `busy_processes` entry is a program name, so no `/`. */
function parkedList(value: unknown, key: (typeof DISPATCH_TO_PARKED_LISTS)[number]): string[] | SpecProblem {
  const field = `${DISPATCH_TO_PARKED}.${key}`;
  const what = key === "busy_processes"
    ? "program names (no whitespace, no /, none of the wrappers it looks past: bash, sh, env, nice, timeout, python3, node…)"
    : "strings without whitespace";
  const problem = {
    field,
    message: `seat-pool: ${field} must be a list of at most ${DISPATCH_TO_PARKED_LIST_MAX} ${what}, each 1 to ${DISPATCH_TO_PARKED_ENTRY_MAX} characters`,
  };
  if (!Array.isArray(value) || value.length > DISPATCH_TO_PARKED_LIST_MAX) return problem;
  for (const entry of value) {
    if (typeof entry !== "string" || entry.length === 0 || entry.length > DISPATCH_TO_PARKED_ENTRY_MAX || /\s/.test(entry)) return problem;
    if (key === "busy_processes" && (entry.includes("/") || seatPoolBusyWrapper(entry))) return problem;
  }
  return [...value] as string[];
}

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
  const maxOpenRows = record["max_open_rows"];
  if (maxOpenRows !== undefined) {
    const { min, max } = DISPATCH_TO_PARKED_MAX_OPEN_ROWS;
    if (!Number.isInteger(maxOpenRows) || (maxOpenRows as number) < min || (maxOpenRows as number) > max) {
      return { field: `${DISPATCH_TO_PARKED}.max_open_rows`, message: `seat-pool: ${DISPATCH_TO_PARKED}.max_open_rows must be a whole number from ${min} to ${max}` };
    }
  }
  const refusedCooldown = record[DISPATCH_TO_PARKED_REFUSED_COOLDOWN.key];
  if (refusedCooldown !== undefined) {
    const { key, min, max } = DISPATCH_TO_PARKED_REFUSED_COOLDOWN;
    if (!Number.isInteger(refusedCooldown) || (refusedCooldown as number) < min || (refusedCooldown as number) > max) {
      return { field: `${DISPATCH_TO_PARKED}.${key}`, message: `seat-pool: ${DISPATCH_TO_PARKED}.${key} must be a whole number from ${min} to ${max}` };
    }
  }
  const lists: Partial<Record<(typeof DISPATCH_TO_PARKED_LISTS)[number], string[]>> = {};
  for (const key of DISPATCH_TO_PARKED_LISTS) {
    if (record[key] === undefined) continue;
    const list = parkedList(record[key], key);
    if (isProblem(list)) return list;
    lists[key] = list;
  }
  const orphanAge = record[BUSY_ORPHAN_MAX_AGE.key];
  const orphanField = `${DISPATCH_TO_PARKED}.${BUSY_ORPHAN_MAX_AGE.key}`;
  if (orphanAge !== undefined && !lists.busy_processes) {
    return { field: orphanField, message: `seat-pool: ${orphanField} is read only with ${DISPATCH_TO_PARKED}.busy_processes` };
  }
  if (orphanAge !== undefined
    && (!Number.isInteger(orphanAge) || (orphanAge as number) < BUSY_ORPHAN_MAX_AGE.min || (orphanAge as number) > BUSY_ORPHAN_MAX_AGE.max)) {
    return { field: orphanField, message: `seat-pool: ${orphanField} must be a whole number from ${BUSY_ORPHAN_MAX_AGE.min} to ${BUSY_ORPHAN_MAX_AGE.max}` };
  }
  return {
    maxWakeSeconds: numbers["max_wake_s"]!, maxClaimAgeSeconds: numbers["max_claim_age_s"]!, allowBackgroundWork: allow ?? true,
    parkedMaxLoadPerCpuPercent: numbers["parked_max_load_per_cpu_percent"]!,
    // Only a list that was given becomes a property, so `{}` reads as it did before the lists.
    ...(lists.refuse_gates ? { refuseGates: lists.refuse_gates } : {}),
    ...(lists.busy_processes
      ? { busyProcesses: lists.busy_processes, busyOrphanMaxAgeSeconds: (orphanAge as number | undefined) ?? BUSY_ORPHAN_MAX_AGE.fallback }
      : {}),
    ...(maxOpenRows !== undefined ? { maxOpenRows: maxOpenRows as number } : {}),
    ...(refusedCooldown !== undefined ? { refusedCooldownSeconds: refusedCooldown as number } : {}),
  };
}

const RETURN_LONG_GATES = "return_long_gates";
/** The fields of `return_long_gates`: the bounds of each whole number, and its default (T-012673). */
const RETURN_LONG_GATES_NUMBERS = {
  min_blocked_s: { min: 0, max: 604_800, fallback: 1800 },
  max_rows: { min: 1, max: 32, fallback: 4 },
} as const;
const RETURN_LONG_GATES_KEYS = new Set([...Object.keys(RETURN_LONG_GATES_NUMBERS), "gates", "to", "ignore_busy_processes"]);
/** The most gates `return_long_gates.gates` may list, and the longest one. */
const RETURN_LONG_GATES_LIST_MAX = 32;
const RETURN_LONG_GATES_ENTRY_MAX = 128;

/** `return_long_gates`: an object with `gates` and `to` (T-012673). Only a field left out takes its default. */
function returnLongGates(value: unknown): SeatPoolReturnLongGates | SpecProblem {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { field: RETURN_LONG_GATES, message: `seat-pool: ${RETURN_LONG_GATES} must be an object with gates and to` };
  }
  const record = value as Record<string, unknown>;
  const unknown = Object.keys(record).filter((key) => !RETURN_LONG_GATES_KEYS.has(key));
  if (unknown.length > 0) {
    return {
      field: RETURN_LONG_GATES,
      message: `seat-pool: ${RETURN_LONG_GATES} has unknown field ${unknown.join(", ")}; known fields are ${[...RETURN_LONG_GATES_KEYS].join(", ")}`,
    };
  }
  const gates = record["gates"];
  const gatesField = `${RETURN_LONG_GATES}.gates`;
  if (!Array.isArray(gates) || gates.length === 0 || gates.length > RETURN_LONG_GATES_LIST_MAX
    || gates.some((gate) => typeof gate !== "string" || gate.length > RETURN_LONG_GATES_ENTRY_MAX || /\s/.test(gate)
      || !gate.startsWith("external:") || gate.length === "external:".length)) {
    return {
      field: gatesField,
      message: `seat-pool: ${gatesField} must be a list of 1 to ${RETURN_LONG_GATES_LIST_MAX} gates, each external:<name> without whitespace, at most ${RETURN_LONG_GATES_ENTRY_MAX} characters`,
    };
  }
  const to = record["to"];
  if (typeof to !== "string" || !/^[^\s@]+@[^\s@]+$/.test(to)) {
    return { field: `${RETURN_LONG_GATES}.to`, message: `seat-pool: ${RETURN_LONG_GATES}.to must be a seat address (<session>@<rig>)` };
  }
  const numbers: Record<string, number> = {};
  for (const [key, { min, max, fallback }] of Object.entries(RETURN_LONG_GATES_NUMBERS)) {
    const field = record[key];
    if (field === undefined) { numbers[key] = fallback; continue; }
    if (!Number.isInteger(field) || (field as number) < min || (field as number) > max) {
      return { field: `${RETURN_LONG_GATES}.${key}`, message: `seat-pool: ${RETURN_LONG_GATES}.${key} must be a whole number from ${min} to ${max}` };
    }
    numbers[key] = field as number;
  }
  // T-016495: program names, as busy_processes (each must also be one of them: parseSeatPoolSpec checks it)
  const ignore = record["ignore_busy_processes"];
  const ignoreField = `${RETURN_LONG_GATES}.ignore_busy_processes`;
  if (ignore !== undefined && (!Array.isArray(ignore) || ignore.length === 0 || ignore.length > DISPATCH_TO_PARKED_LIST_MAX
    || ignore.some((entry) => typeof entry !== "string" || entry.length === 0 || entry.length > DISPATCH_TO_PARKED_ENTRY_MAX
      || /\s/.test(entry) || entry.includes("/") || seatPoolBusyWrapper(entry)))) {
    return {
      field: ignoreField,
      message: `seat-pool: ${ignoreField} must be a list of 1 to ${DISPATCH_TO_PARKED_LIST_MAX} program names of ${DISPATCH_TO_PARKED}.busy_processes`,
    };
  }
  return {
    gates: [...new Set(gates as string[])], minBlockedSeconds: numbers["min_blocked_s"]!, to, maxRows: numbers["max_rows"]!,
    ...(ignore !== undefined ? { ignoreBusyProcesses: [...new Set(ignore as string[])] } : {}),
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
  const noSeatGrace = context["no_seat_grace_s"];
  if (noSeatGrace !== undefined
    && (!Number.isInteger(noSeatGrace) || (noSeatGrace as number) < NO_SEAT_GRACE.min || (noSeatGrace as number) > NO_SEAT_GRACE.max)) {
    return { problem: { field: "no_seat_grace_s", message: `seat-pool: no_seat_grace_s must be a whole number from ${NO_SEAT_GRACE.min} to ${NO_SEAT_GRACE.max}` } };
  }
  const priorityAging = context["priority_aging_s"];
  if (priorityAging !== undefined
    && (!Number.isInteger(priorityAging) || (priorityAging as number) < PRIORITY_AGING.min || (priorityAging as number) > PRIORITY_AGING.max)) {
    return { problem: { field: "priority_aging_s", message: `seat-pool: priority_aging_s must be a whole number of seconds from ${PRIORITY_AGING.min} (off) to ${PRIORITY_AGING.max}` } };
  }
  const contextLimit = context["context_limit_percent"];
  if (contextLimit !== undefined
    && (!Number.isInteger(contextLimit) || (contextLimit as number) < CONTEXT_LIMIT.min || (contextLimit as number) > CONTEXT_LIMIT.max)) {
    return { problem: { field: "context_limit_percent", message: `seat-pool: context_limit_percent must be a whole number from ${CONTEXT_LIMIT.min} (off) to ${CONTEXT_LIMIT.max}` } };
  }
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
  let longGates: SeatPoolReturnLongGates | undefined;
  if (context[RETURN_LONG_GATES] !== undefined) {
    const parsed = returnLongGates(context[RETURN_LONG_GATES]);
    if (isProblem(parsed)) return { problem: parsed };
    longGates = parsed;
  }
  // T-016495: an ignored program that busy_processes does not list would ignore nothing, so it is a mistake in the spec
  const notBusy = longGates?.ignoreBusyProcesses?.filter((program) => !parkedRule?.busyProcesses?.includes(program)) ?? [];
  if (notBusy.length > 0) {
    const field = `${RETURN_LONG_GATES}.ignore_busy_processes`;
    return { problem: { field, message: `seat-pool: ${field} lists ${notBusy.join(", ")}, not in ${DISPATCH_TO_PARKED}.busy_processes` } };
  }
  return {
    spec: {
      min: min as number, max: max as number, cooldownSeconds: cooldownSeconds as number, idleGraceSeconds: idleGraceSeconds as number,
      dispatchAfterSeconds: dispatchAfterSeconds as number, dispatchTag: dispatchTag as string | null, drain,
      drainDeadlineSeconds: drainDeadlineSeconds as number, adopt: [...new Set(adopt as string[])], cwdTemplate,
      demandCmd: commands["demand_cmd"]!, releaseCmd: commands["release_cmd"]!, prepareCmd: commands["prepare_cmd"]!,
      reapCmd: commands["reap_cmd"]!, removeCmd: commands["remove_cmd"]!, escalationOwner: escalationOwner as string | null,
      ...(parkedRule ? { dispatchToParked: parkedRule } : {}),
      ...(noSeatGrace !== undefined ? { noSeatGraceSeconds: noSeatGrace as number } : {}),
      ...(priorityAging !== undefined ? { priorityAgingSeconds: priorityAging as number } : {}),
      ...(contextLimit !== undefined ? { contextLimitPercent: contextLimit as number } : {}),
      // Only a pool that sets the key carries it, so the spec of one that doesn't reads as before it (T-012673).
      ...(longGates ? { returnLongGates: longGates } : {}),
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
  /** Only with `dispatch_to_parked` or `return_long_gates` in the spec and the node present: idle at its prompt with no
   *  needs-input, whatever runs in the background. */
  atPrompt?: boolean;
  /** Only with `dispatch_to_parked` or `return_long_gates` in the spec and the node present: a background shell runs in
   *  the seat. */
  backgroundWork?: boolean;
  /** Only with `dispatch_to_parked` or `return_long_gates` in the spec, the row facts read and exactly one open row,
   *  blocked: that row's park. */
  parked?: ParkFacts;
  /** Only with `dispatch_to_parked.max_open_rows` or `return_long_gates.max_rows` above 1, the row facts read and 2 or
   *  more open rows, all blocked (at most the larger of the two): the park of each (T-007036, T-012673). A seat with one
   *  open row carries `parked` instead. */
  parkedRows?: ParkFacts[];
  /** Only with `dispatch_to_parked.busy_processes` in the spec, for a seat the process read looked at: why the seat
   *  counts as busy for a row (the matching process, or that its processes could not be read), or null when no
   *  process of the seat matches. It holds back a parked seat from a 2nd row and, since T-006251 (B1), a free seat from
   *  its 1st. Absent: not read, which holds nothing back. */
  busyProcess?: string | null;
  /** Only with `dispatch_to_parked.refused_cooldown_s` above 0: the latest row the pool moved to the seat as parked,
   *  within the cooldown, that the seat gave straight back without taking it (seatPoolCanReceive). `at`: when it gave it
   *  back. `openRowIds`: the seat's parked rows when the row was moved to it (null when the record does not say). */
  parkedRefusal?: { at: string; openRowIds: string[] | null };
  /** Only for a seat the pool added (not adopted), its node present and `context_limit_percent` above 0: its context
   *  usage as the status-line sidecar of its current occupant reads it (a sample of a previous occupant, or one with no
   *  percentage, is none). Absent: no reading, which holds nothing back (seatPoolContextOver). */
  context?: SeatPoolContextReading;
}

/** A seat's context usage as its status-line sample reads it: `used_percentage` and `sampled_at` (T-012383). */
export interface SeatPoolContextReading {
  usedPercentage: number;
  sampledAt: string;
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
  /** Only when the reader asks for it (`return_long_gates` in the spec, T-012673): since when the row has been blocked
   *  on one of the rule's `gates` without leaving `blocked` (ms), T-012748: the unbroken run of its latest blocked
   *  transitions whose recorded gate (queue_transitions.blocked_on, migration 174) is listed. A re-park on a listed gate,
   *  a note and a wake timer that fired keep it; a row parked on another gate for hours, then on a listed one, counts
   *  from its move to the listed gate. A transition recorded before the column counts only as the row's newest (its gate
   *  is the row's current one), so the age is never longer than the truth. Null when nothing counts or the record does
   *  not say. `parkedAt` can't tell it: it is null with no wake and moves with each re-park. Absent (no property)
   *  otherwise, so the facts read as before the field. */
  blockedSince?: number | null;
  /** Only with `blockedSince`: when a wake of the row last fired (ms), null when none did. A wake that fired a moment
   *  ago was typed into the seat's pane, which may not read as working yet (T-012673). */
  wakeFiredAt?: number | null;
  /** Only with `blockedSince`: why the row can't be handed to the fixed seat as it is (QueueRepository.
   *  handoffParkedRows refuses the same): `wake_kind_unsupported` (woken by a watchdog attached to it, a repeating
   *  wait or a blocker row, not a park timer of its own or nothing) or `dependent_at_target` (a row of the fixed seat
   *  waits on it); null when it can go. */
  returnRefusal?: string | null;
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
  /** Only with `dispatch_to_parked.refused_cooldown_s` above 0: the pool's seats (session names) that gave this row, or
   *  a row it succeeds by handoff, straight back to the model after the pool moved it to them as parked, without taking
   *  it: every such move of the row, not only the latest (a reroute keeps the row's id, so two parked seats could
   *  otherwise hand it back and forth, T-008466 SH-A). It is never moved to one of them while that seat is parked.
   *  Absent: none. */
  refusedBy?: string[];
}

export interface SeatPoolState {
  model: SeatPoolModel | null;
  /** Why the model isn't taking a row. */
  modelCause: SeatPoolSeatCause;
  restoring: boolean;
  slots: SeatPoolSlotView[];
  /** The spec's adopt entries that exist in the rig and no pool has linked yet. */
  adoptable: Array<{ logicalId: string; nodeId: string }>;
  /** Pending rows addressed to the model, to the pool's seats and to the seats it removed, by priority rank, then
   *  longest waiting first (readPendingRows, T-009034). */
  pending: SeatPoolPendingRow[];
  /** The demand command's last good answer, when the spec has one. */
  demand: { desired: number; sampledAt: string } | null;
  /** The latest add or removal in this pool. */
  lastScaleAt: string | null;
  /** The host's load per CPU (load1 / CPUs × 100), read for a spec whose `dispatch_to_parked` sets
   *  `parked_max_load_per_cpu_percent`; null when the host gives no reading. Absent: not read, and no seat is held back
   *  by the load in the plan. */
  hostLoadPerCpuPercent?: number | null;
  /** T-012673: present (true) only when the spec has `return_long_gates` and the queue does not take its `to` as a
   *  destination (a wrong address, a seat gone from its rig): no seat's rows are planned back. Absent otherwise. */
  returnLongGatesRefused?: true;
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
  | { kind: "reap"; slotId: number }
  /** Hand the rows a seat holds parked on a long wait to the fixed seat `to`, each parked again there on the same gate,
   *  so the seat is free (seatPoolLongGateRows, T-012673). */
  | { kind: "return_long_gate"; slotId: number; qitemIds: string[]; to: string };

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

/** A seat the plan counts among the pool's running seats: live, its node present, not an adopted seat that stopped and
 *  not one to be reaped (seatPoolPlan's `live`). */
function seatPoolLive(slot: SeatPoolSlotView): boolean {
  return slot.phase === "live" && (slot.nodeId === null || slot.nodeExists) && !(slot.adopted && slot.cause === "stopped") && !seatPoolReapable(slot);
}

/** No open row, no held message and idle since a known time: nothing but local work could keep the seat from a row. */
function seatPoolQuiet(slot: SeatPoolSlotView): boolean {
  return slot.openRows === 0 && slot.heldMessages === 0 && slot.idleSince !== null;
}

/** With `busy_processes`, the seat's process read names a program or failed (T-006251): it takes no row either way. */
function seatPoolHeldBusy(spec: SeatPoolSpec, slot: SeatPoolSlotView): boolean {
  return !!spec.dispatchToParked?.busyProcesses?.length && !!slot.busyProcess;
}

/**
 * Whether a seat the pool added is too full of context to take a row (T-012383): `context_limit_percent` above 0 (absent:
 * 40), and its reading (SeatPoolSlotView.context) at that percent or above, sampled less than 10 minutes ago
 * (SEAT_POOL_CONTEXT_MAX_AGE_SECONDS; a sample stamped ahead of the clock counts as just taken), as the Billverso's
 * orchestration gate reads the same sidecar (portao._contexto: used_percentage >= 40, sampled_at under 10 min). Such a
 * seat would take the row and give it straight back (its own rule: no new work past the limit), and the pool would hand
 * the row to the next full seat. No reading, an unreadable or stale one, an adopted seat and a limit of 0 hold nothing
 * back, as before the field.
 *
 * A quiet seat (no open row, no held message, idle since a known time) keeps its reading, however old (T-017384): it
 * runs no turn, so its context does not move and nothing rewrites the sample, which is its live occupant's (the reader
 * refuses a previous occupant's, and /clear writes none). Its idle time says nothing about the sample: it restarts with
 * the daemon and with a change of activity that ran no turn. Without this, the sample went stale 10 min after the
 * seat's last turn, which is when idle_grace_s (600 s in the pools that run it) lets a seat be drained, or at once after
 * a restart: the full seat read as free again, took a row, woke, read its fresh sample and gave the row back.
 */
export function seatPoolContextOver(spec: Pick<SeatPoolSpec, "contextLimitPercent">, slot: SeatPoolSlotView, nowMs: number): boolean {
  const limit = seatPoolContextLimit(spec);
  const reading = slot.context;
  if (limit <= 0 || slot.adopted || !reading) return false;
  if (typeof reading.usedPercentage !== "number" || !Number.isFinite(reading.usedPercentage) || reading.usedPercentage < limit) return false;
  const sampledMs = Date.parse(reading.sampledAt);
  // Written so that an unreadable time (NaN) holds nothing back.
  if (!Number.isFinite(sampledMs)) return false;
  if (Math.max(0, nowMs - sampledMs) < SEAT_POOL_CONTEXT_MAX_AGE_SECONDS * 1000) return true;
  // A quiet seat keeps its reading, however old (REVISAO-seat-pool S5): it runs no turn, so nothing rewrote the sample,
  // and the reading is its live occupant's (the reader refuses another occupant's, and /clear writes none).
  return slot.openRows === 0 && slot.heldMessages === 0 && slot.idleSince !== null;
}

/**
 * Whether a quiet seat is held only by its context (seatPoolContextOver): live, in usage, no open row, no held message,
 * idle since a known time, and past the limit. The plan counts these as occupied while they are inside idle_grace_s or
 * busy with a process, and the demand command is told so as `held: "context"`; past that (seatPoolContextDrainable,
 * T-017384) they are neither occupied nor free: they don't hold the target up, the demand command is not told they are
 * held, they are drained after the free seats and for the rows `max` leaves no room for, and the rows they can't take
 * get seats of their own. A seat with an open row
 * is occupied already; one out of provider usage says nothing, as for `held: "busy"` (seatPoolHeldByProcess).
 */
export function seatPoolHeldByContext(spec: SeatPoolSpec, slot: SeatPoolSlotView, nowMs: number, facts: { usageLimited?: boolean } = {}): boolean {
  if (facts.usageLimited || slot.cause === "usage_limit") return false;
  return seatPoolLive(slot) && seatPoolQuiet(slot) && seatPoolContextOver(spec, slot, nowMs);
}


/**
 * Whether the plan drains a seat held only by its context (T-017384): seatPoolHeldByContext, not adopted, no busy
 * program in it (seatPoolHeldBusy), idle for `idle_grace_s`. Such a seat is neither occupied nor free in the plan, and
 * the demand command is not told it is held (`held: "context"`): it is on its way out, and counted as held it would hold
 * the target up for ever (REVISAO-seat-pool B1).
 */
export function seatPoolContextDrainable(spec: SeatPoolSpec, slot: SeatPoolSlotView, nowMs: number, facts: { usageLimited?: boolean } = {}): boolean {
  return !slot.adopted && !seatPoolHeldBusy(spec, slot) && seatPoolHeldByContext(spec, slot, nowMs, facts)
    && nowMs - Date.parse(slot.idleSince!) >= spec.idleGraceSeconds * 1000;
}

/**
 * Whether a free seat is held only by a `busy_processes` program the read found under its pane (an orphaned suite-flock
 * or congelar-eval): quiet, live, in usage, and its `busyProcess` a process (seatPoolBusyFromProcess), not a failed read.
 * These are the quiet seats the plan counts as occupied (heldBusy && !heldUnread), and the demand command is told so as
 * `held: "busy"` (T-007527). A seat out of provider usage (`usageLimited`, or its cause) says nothing, as one with a held
 * message, one to be reaped or one whose read failed: while the limit lasts the pool adds no seat for it anyway.
 */
export function seatPoolHeldByProcess(spec: SeatPoolSpec, slot: SeatPoolSlotView, facts: { usageLimited?: boolean } = {}): boolean {
  if (facts.usageLimited || slot.cause === "usage_limit") return false;
  return seatPoolLive(slot) && seatPoolQuiet(slot) && seatPoolHeldBusy(spec, slot) && seatPoolBusyFromProcess(slot.busyProcess);
}

/**
 * Whether a seat may receive a waiting row now, and how; null when it may not. The planner, the executor's check just
 * before the move and the slots the demand command is given (`receives`) all ask this one function.
 *
 * Both ways need the pool not draining, the seat live, its node present, not to be reaped, no held message and, for a
 * seat the pool added, its context under `context_limit_percent` (seatPoolContextOver, T-012383).
 * `free`: no open row and idle at its prompt (the rule from before `dispatch_to_parked`), and, with
 * `dispatch_to_parked.busy_processes`, no such program of the seat's (`busyProcess`, T-006251). `parked`, only with
 * `dispatch_to_parked`: a seat the pool added (not adopted), at its prompt (idle, or working only through a background
 * shell, which `allow_background_work: false` refuses), whose open rows (one, or up to `max_open_rows`) are each blocked
 * on an `external:` gate with a
 * timer of at most `max_wake_s` still armed and live, armed by the seat itself, after it took the row (by claim or by
 * update) no more than `max_claim_age_s` ago, on a gate that contains none of `refuse_gates`, and with no
 * `busy_processes` program under its pane (`busyProcess`). A seat with more open rows than `max_open_rows` (default 1),
 * or with one that is pending or in progress, never receives: one row more is the ceiling (T-007036).
 */
export function seatPoolCanReceive(spec: SeatPoolSpec, slot: SeatPoolSlotView, nowMs: number): SeatPoolReceiveMode | null {
  if (spec.drain || slot.phase !== "live" || !slot.nodeExists || seatPoolReapable(slot) || slot.heldMessages !== 0) return null;
  // A seat past its context limit takes no row, free or parked: it would give it straight back (T-012383).
  if (seatPoolContextOver(spec, slot, nowMs)) return null;
  const rule = spec.dispatchToParked;
  if (slot.openRows === 0 && slot.idleSince !== null && slot.cause === "idle") {
    // A free seat with heavy local work its runtime does not track (a suite-flock or congelar-eval orphaned by the shell
    // that started it) gives the row back too, as a parked one would (T-006251, B1). Only with busy_processes.
    return rule?.busyProcesses?.length && slot.busyProcess ? null : "free";
  }
  if (!rule || slot.adopted) return null;
  if (slot.atPrompt !== true || (slot.cause !== "idle" && slot.cause !== "working")) return null;
  if (!rule.allowBackgroundWork && slot.backgroundWork === true) return null;
  // Every open row must be one of the seat's own parks: exactly one (`parked`), or with max_open_rows above 1 up to that
  // many (`parkedRows`, T-007036). A pending or in-progress row has no park, so the counts then differ and it refuses.
  const maxOpenRows = rule.maxOpenRows ?? DISPATCH_TO_PARKED_MAX_OPEN_ROWS_DEFAULT;
  const parks = slot.openRows === 1 ? (slot.parked ? [slot.parked] : []) : (slot.parkedRows ?? []);
  if (slot.openRows < 1 || slot.openRows > maxOpenRows || parks.length !== slot.openRows) return null;
  if (slot.busyProcess) return null;
  if (seatPoolRefusedRecently(rule, slot, parks, nowMs)) return null;
  return parks.every((park) => seatPoolOwnPark(rule, slot.sessionName, park, nowMs)) ? "parked" : null;
}

/** A seat that gave a row it got as parked straight back, within `refused_cooldown_s`, still holding the same parks:
 *  the pool's last row was refused, and a new one would be too (the seat gives rows back while its waits stand). A change
 *  in its open rows (a park closed or a new one) ends it, as does the cooldown; `refused_cooldown_s: 0` turns it off. */
function seatPoolRefusedRecently(rule: SeatPoolDispatchToParked, slot: SeatPoolSlotView, parks: readonly ParkFacts[], nowMs: number): boolean {
  const refusal = slot.parkedRefusal;
  const cooldown = rule.refusedCooldownSeconds ?? DISPATCH_TO_PARKED_REFUSED_COOLDOWN_DEFAULT;
  if (!refusal || cooldown <= 0) return false;
  const atMs = Date.parse(refusal.at);
  // Written so that an unreadable time (NaN) holds the seat back for nothing.
  if (!(nowMs - atMs < cooldown * 1000)) return false;
  if (refusal.openRowIds === null) return true;
  const now = parks.map((park) => park.qitemId).sort();
  const then = [...refusal.openRowIds].sort();
  return now.length === then.length && now.every((id, i) => id === then[i]);
}

/** Whether one blocked row is the seat's own short wait: an `external:` gate not refused, a live timer it armed within
 *  max_wake_s, taken within max_claim_age_s and parked after it was taken. */
function seatPoolOwnPark(rule: SeatPoolDispatchToParked, sessionName: string, park: ParkFacts, nowMs: number): boolean {
  if (!park.blockedOn.startsWith("external:")) return false;
  // A wait that is itself heavy local work (an eval being frozen, a suite queued for the host's lock) is not one the seat
  // can take a 2nd row through: it gives the row back and the row goes round (T-006090).
  if (rule.refuseGates?.some((part) => park.blockedOn.includes(part))) return false;
  if (!park.timerArmedLive || park.timerIntervalS === null || park.timerIntervalS > rule.maxWakeSeconds) return false;
  if (park.parkActor !== sessionName) return false;
  if (park.takenAt === null || park.parkedAt === null) return false;
  const takenMs = Date.parse(park.takenAt);
  const parkedMs = Date.parse(park.parkedAt);
  // Written so that an unreadable time (NaN) refuses.
  return parkedMs >= takenMs && nowMs - takenMs <= rule.maxClaimAgeSeconds * 1000;
}

/** The gate of `return_long_gates.gates` a row's blocked_on matches, or null: the gate itself, or the gate followed by
 *  `-`, `:` or `/` and more (a gate named per task, `external:github-review-T-012673`), never a longer name
 *  (`external:github-reviewer`). */
export function seatPoolLongGateOf(rule: Pick<SeatPoolReturnLongGates, "gates">, blockedOn: string): string | null {
  return rule.gates.find((gate) => blockedOn === gate
    || (blockedOn.length > gate.length + 1 && blockedOn.startsWith(gate) && "-:/".includes(blockedOn[gate.length]!))) ?? null;
}

/**
 * The rows a seat hands back to the fixed seat of `return_long_gates` (T-012673), or null when it hands none. The case
 * (17/11 23:06Z): 6 of the 10 seats of the reviewer pool each held one row parked on `external:github-review` (a PR
 * waiting for a person's review, for hours), so they counted as occupied, and the pool dispatched nothing for half an
 * hour. Such a seat's rows go to the fixed seat, still parked on the same gate, and the seat is free.
 *
 * Every condition must hold: the rule on and the pool not draining; a live seat the pool added (an adopted seat is a
 * fixed seat already), not to be reaped, its node present; no held message; idle at its prompt (`cause` idle and
 * `atPrompt`) for at least SEAT_POOL_LONG_GATE_QUIET_MS, with no background shell and no `busy_processes` program under
 * its pane, so nothing of the seat's is running; between 1 and `max_rows` open rows, ALL blocked (a pending or
 * in-progress row means the seat is working); and each of them on one of `gates`, blocked on those gates (unbroken) for
 * at least `min_blocked_s` (`blockedSince`, T-012748; unknown refuses), with no wake of it fired within SEAT_POOL_LONG_GATE_QUIET_MS and
 * nothing that keeps it from going as it is (`returnRefusal` null; unknown refuses). The planner and the executor's
 * recheck just before the move both ask this.
 */
export function seatPoolLongGateRows(spec: SeatPoolSpec, slot: SeatPoolSlotView, nowMs: number): ParkFacts[] | null {
  const rule = spec.returnLongGates;
  if (!rule || spec.drain || slot.adopted || !seatPoolLive(slot) || slot.nodeId === null || !slot.nodeExists) return null;
  if (slot.heldMessages !== 0 || slot.cause !== "idle" || slot.atPrompt !== true || slot.backgroundWork === true
    || seatPoolLongGateBusy(rule, slot.busyProcess)) return null;
  // The seat's state is observed, so it lags: a seat a wake was just typed into (its own row's timer firing) can still
  // read as idle since hours. A quiet seat has been idle a while and no wake of its rows fired a moment ago, so the
  // move never lands under a turn the seat just began on one of them. Written so that an unknown time refuses.
  const idleMs = slot.idleSince !== null ? Date.parse(slot.idleSince) : NaN;
  if (!(nowMs - idleMs >= SEAT_POOL_LONG_GATE_QUIET_MS)) return null;
  const parks = slot.openRows === 1 ? (slot.parked ? [slot.parked] : []) : (slot.parkedRows ?? []);
  if (slot.openRows < 1 || slot.openRows > rule.maxRows || parks.length !== slot.openRows) return null;
  // Written so that an unknown or unreadable time (null, NaN) refuses.
  const old = (park: ParkFacts) => typeof park.blockedSince === "number" && nowMs - park.blockedSince >= rule.minBlockedSeconds * 1000;
  const unwoken = (park: ParkFacts) => park.wakeFiredAt === undefined || park.wakeFiredAt === null
    || nowMs - park.wakeFiredAt >= SEAT_POOL_LONG_GATE_QUIET_MS;
  // A row the queue would refuse to move (a watchdog or a repeating wait wakes it, a row of the fixed seat waits on it)
  // is never planned: the plan would be refused on every pass, and the seat would lose its turn as a parked receiver.
  return parks.every((park) => seatPoolLongGateOf(rule, park.blockedOn) !== null && old(park) && unwoken(park) && park.returnRefusal === null)
    ? parks : null;
}

/** Whether the seat's busy read keeps its rows from going back (seatPoolLongGateRows): any reason does, except one that
 *  names a program of `ignore_busy_processes` (T-016495). The read says such a program only when nothing else holds the
 *  seat (readSeatPoolBusySessions, lastPrograms), so a failed read or another busy program still keeps the rows. The case
 *  (21/11 02:03Z): 14 of the 28 seats of the dev pool were held by their `sair-do-pool.py --segurar` (it waits for the
 *  seat's own execution in the ledger to end), and one of them kept its row parked on a listed gate for over
 *  `min_blocked_s` instead of handing it to the fixed seat, while 22 rows waited at the model. */
export function seatPoolLongGateBusy(rule: Pick<SeatPoolReturnLongGates, "ignoreBusyProcesses">, busy: string | null | undefined): boolean {
  if (!busy) return false;
  const program = seatPoolBusyProgram(busy);
  return !(program !== null && (rule.ignoreBusyProcesses ?? []).includes(program));
}

/** How long a seat must have been idle, and since a wake of its rows last fired, before return_long_gates moves its rows
 *  (seatPoolLongGateRows, T-012673): well past the lag of the observed seat state (a hook, a 1 s pane sample). */
export const SEAT_POOL_LONG_GATE_QUIET_MS = 120_000;

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
 * goes to a free seat (live, idle at a prompt, no open row, no held message), by priority rank, then the longest
 * waiting first (readPendingRows, T-009034); one that finds no free seat is demand. A waiting row the pool
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
 * added and has not started, which waits as it is (and may still be removed). Only a seat the pool added, idle for
 * `idle_grace_s`, is drained, the newest first: a free one for the surplus, then one held only by its context (T-017384)
 * for the rest of the surplus and for each waiting row no seat takes beyond the room `max` leaves; an adopted seat is
 * never drained, and under `drain` it is unlinked.
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
  // T-012673: a seat holding only rows parked on a long wait hands them to the fixed seat. In this plan it still counts
  // as occupied (its rows are open) and receives nothing, also as a parked seat (a github-review wait with a short timer
  // of its own would read as one under dispatch_to_parked); once its rows are gone the next plan finds it free.
  // A fixed seat the queue does not take (state.returnLongGatesRefused) gets no plan at all: every move would be refused,
  // and the seats would lose their turn as parked receivers; the executor says so once.
  const returning = new Set<SeatPoolSlotView>();
  if (!state.returnLongGatesRefused) for (const slot of present) {
    const parks = seatPoolLongGateRows(spec, slot, nowMs);
    if (!parks) continue;
    returning.add(slot);
    intents.push({ kind: "return_long_gate", slotId: slot.slotId, qitemIds: parks.map((park) => park.qitemId), to: spec.returnLongGates!.to });
  }

  const starting = present.filter((slot) => slot.phase === "adding" || slot.phase === "added");
  // A stopped adopted seat takes no row and is not reaped: it isn't one of the pool's seats until it runs again. A seat
  // this plan reaps is on its way out: it isn't counted as current either, so the same plan can add its replacement
  // (the executor checks the room again after the reap, so the add never takes the pool past `max` if the reap launched the
  // seat instead).
  const live = present.filter(seatPoolLive);
  // A seat with no row but heavy local work its runtime does not track (busyProcess, with busy_processes: T-006251)
  // takes no row, so it is not idle: it counts as occupied (the pool grows for the row it can't take, as for a parked
  // seat) and is never drained as free while its suite runs (REVISAO lente de lógica, wf_fff3f5f2).
  const heldBusy = (slot: SeatPoolSlotView) => seatPoolHeldBusy(spec, slot);
  const quiet = seatPoolQuiet;
  // A quiet seat past its context limit (seatPoolContextOver, T-012383) takes no row either, so it is not idle: the pool
  // grows for the row it can't take instead of leaving the row at the model. Past idle_grace_s and with no busy process
  // it leaves the pool (`full`, T-017384): its context only grows, so a seat in its place is what takes the row.
  const heldContext = (slot: SeatPoolSlotView) => seatPoolContextOver(spec, slot, nowMs);
  const idle = (slot: SeatPoolSlotView) => quiet(slot) && !heldBusy(slot) && !heldContext(slot);
  // T-006251 S6: a quiet seat held only because its processes could not be read (a timeout under load, an unreadable
  // environment, its pane not found) takes no row and is never drained, but it is no occupant either: counting it grew
  // the pool to `max` with no row dispatched (every seat added failed the same read). It still holds its place in the
  // target (no seat starting is removed for it), and while rows wait undispatched the pool asks ONE seat more than the
  // healthy plan (the rows waiting plus the occupied seats), which takes a row when the failure was that seat's own (lens
  // review of the volta 1). Without demand_cmd it grows to the rows waiting + the occupied seats + 1 (R + 1 when no seat
  // holds work; up to `max`), never by one per failed read: a read failing for every free seat, with more rows waiting
  // than seats, still reaches that with nothing dispatched (T-007205 S14).
  const heldUnread = (slot: SeatPoolSlotView) => quiet(slot) && heldBusy(slot) && !seatPoolBusyFromProcess(slot.busyProcess) && !heldContext(slot);
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
  const modes = new Map(live.map((slot) => [slot, spec.drain || returning.has(slot) ? null : seatPoolReceives(spec, slot, nowMs, { loadPerCpuPercent })]));
  const receivers = [
    ...live.filter((slot) => modes.get(slot) === "free"),
    ...live.filter((slot) => modes.get(slot) === "parked").sort((a, b) => a.slotId - b.slotId),
  ];
  // In order, each waiting row goes to the first receiver not taken yet, except a PARKED receiver that refused it (or
  // a row it succeeds: refusedBy): a row a parked seat gave back waits for another seat, or for that seat once it is
  // free (no open row: what made it give the row back is gone, and the row never waits with no seat able to take it,
  // whatever the demand command counts: T-008296 B1).
  const receiving = new Set<SeatPoolSlotView>();
  const dispatched = new Set<SeatPoolPendingRow>();
  for (const row of movable) {
    if (receiving.size === receivers.length) break;
    const receiver = receivers.find((slot) => !receiving.has(slot)
      && !(modes.get(slot) === "parked" && row.refusedBy?.includes(slot.sessionName)));
    if (!receiver) continue;
    receiving.add(receiver);
    dispatched.add(row);
    intents.push({
      kind: "dispatch", qitemId: row.qitemId, from: row.destination, slotId: receiver.slotId,
      ...(modes.get(receiver) === "parked" ? { mode: "parked" as const } : {}),
    });
  }

  if (!spec.drain) {
    const holders = new Map<string, { nodeId: string; cause: SeatPoolSeatCause; rows: SeatPoolPendingRow[] }>();
    for (const row of waiting) {
      if (dispatched.has(row)) continue;
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

  // T-017384: the seats held only by their context (seatPoolHeldByContext: live, in usage, no open row, no held message,
  // idle since a known time), not busy with a process, idle for idle_grace_s, and not adopted, the newest first. They
  // take no row and never will (their context only grows), so they are neither occupied nor free: they don't hold the
  // target up, and they leave the pool after the free seats.
  const full = live.filter((slot) => seatPoolContextDrainable(spec, slot, nowMs, { usageLimited })).sort((a, b) => b.slotId - a.slotId);
  const fullSet = new Set(full);
  const occupied = live.filter((slot) => ((!idle(slot) && !heldUnread(slot)) || receiving.has(slot)) && !fullSet.has(slot));
  const unread = live.filter((slot) => heldUnread(slot) && !receiving.has(slot));
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
      desired = movable.length - dispatched.size + occupied.length
        + (unread.length > 0 && movable.length > dispatched.size ? 1 : 0);
    }
    if (desired === null) {
      // No current answer from the demand command: keep what exists and finish what was started.
      if (!usageLimited) for (const slot of starting) intents.push({ kind: "launch", slotId: slot.slotId });
      return { intents, reason: "demand_sample_stale", target: null, current };
    }
    target = Math.min(spec.max, Math.max(spec.min, desired, occupied.length + unread.length + starting.length));
    reason = target > current ? "scale_up" : target < current ? "scale_down" : "steady";
    if (usageLimited && target > current) {
      target = current;
      reason = "usage_limit";
    }
  }

  const surplus = Math.max(0, current - target);
  // An adopted seat was someone else's before the pool: the pool never stops or removes it.
  const drainOrder = free.filter((slot) => !slot.adopted).sort((a, b) => b.slotId - a.slotId);
  // T-017384: the full seats (above) leave after the free ones: for the surplus the free seats don't cover (they don't
  // hold the target up, so with no row waiting they all go, down to `min`), and one for each waiting row that no seat
  // takes and no starting seat will, beyond the room `max` leaves. That room is the add's (seatPoolRoom) before its clamp
  // at 0: a pool over `max` must first drain back to it before a drain makes room for a seat that takes a row. The free
  // seats still go first for the surplus, as before: a full seat whose release command refuses (the executor puts it
  // back, and the next plan picks it again) holds back only the full seats behind it, never a free seat that would go.
  const leavingNow = new Set([...state.slots.filter((slot) => !present.includes(slot)), ...present.filter(seatPoolReapable)]);
  const keptNow = state.slots.filter((slot) => !leavingNow.has(slot));
  const roomNow = seatPoolRoom(spec, keptNow, state.adoptable.length);
  const rowsLeft = movable.length - dispatched.size - starting.length;
  const renew = spec.drain || rowsLeft <= 0 ? 0 : Math.max(0, rowsLeft - (spec.max - keptNow.length - state.adoptable.length));
  const freeOut = drainOrder.slice(0, surplus);
  const fullOut = full.slice(0, Math.max(0, Math.max(surplus, renew) - freeOut.length));
  if (!cooling) for (const slot of [...freeOut, ...fullOut]) intents.push({ kind: "drain", slotId: slot.slotId, idleSince: slot.idleSince });
  // A seat that never started holds no work. It goes only for the surplus free and full seats can't cover (a lowered
  // max, or drain).
  const removeStarting = [...starting].sort((a, b) => b.slotId - a.slotId).slice(0, Math.max(0, surplus - full.length - drainOrder.length));
  for (const slot of starting) {
    if (removeStarting.includes(slot)) intents.push({ kind: "remove", slotId: slot.slotId });
    else if (!usageLimited) intents.push({ kind: "launch", slotId: slot.slotId });
  }
  if (usageLimited && starting.length > removeStarting.length) reason = "usage_limit";
  // Without demand_cmd, the full seats are on their way out (above), so the rows that no seat takes and no starting
  // seat will (rowsLeft) are asked for as if they weren't there: up to `target - current + full`, the seats that would
  // serve the target. Only for those rows: a full seat the target counts anyway is never replaced while no row waits for
  // a seat, so no add and drain go round it (lens review of the volta 1).
  // With demand_cmd the demand command is the target's authority (REVISAO-seat-pool B4): it sizes the rows it wants
  // seats for (a row it leaves out on purpose, as under a downstream brake, gets none), and the full seats it does not
  // count are out of the target already, so they go by the surplus and the next plan adds for them.
  const wanted = spec.demandCmd
    ? target - current
    : Math.max(target - current, Math.min(target - current + full.length, Math.max(0, rowsLeft)));
  if (wanted > 0 && !cooling) {
    // The same room the executor checks before each seat: an add it would refuse is never asked for. A seat the same
    // plan forgets (its node is gone) or reaps is out of the count: the executor applies those intents before the add,
    // and checks the room again before each seat, so a reap that ends up launching its seat instead never takes the pool
    // past `max` (below it, the add may still start one seat more than the target, idle until idle_grace_s).
    const count = Math.min(wanted, roomNow);
    if (count > 0) intents.push({ kind: "add", count });
    else if (target > current) reason = "max_reached";
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
