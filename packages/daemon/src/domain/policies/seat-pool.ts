// The seat-pool policy: elastic seats cloned from one model member.
//
// A pool job targets its model seat (`<pod>-<member>@<rig>`) and keeps between `min` and `max` extra seats beside it,
// named `<member>-s<n>`. Evaluation is pure planning: it reads the pool's state through an injected reader, decides
// what should change (add N seats, finish starting a seat, drain a free seat, remove a seat) and hands those intents
// to the executor, which applies them outside the scheduler tick (seat-pool-executor.ts). The policy itself never
// creates, launches, stops or removes anything, so one slow launch can't hold every other job's evaluation.

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
  /** True empties the pool as its seats become free, and adds none: the kill switch. */
  drain: boolean;
  /** Existing seats (logical ids) this pool takes over, so they are counted and drained like its own. */
  adopt: string[];
  /** Absolute working directory for a new seat; `{slot}` (required) and `{rig}` are replaced. */
  cwdTemplate: string;
  /** Prints `{"desired": N}`: how many extra seats the pool should have. Absent: the pending rows to the model. */
  demandCmd: SeatPoolCommand | null;
  /** Must exit 0 before a free seat is stopped and removed. */
  releaseCmd: SeatPoolCommand | null;
  /** Runs after a seat is added and before its first launch, so it can prepare the seat's working directory. */
  prepareCmd: SeatPoolCommand | null;
}

const SPEC_KEYS = new Set(["min", "max", "cooldown_s", "idle_grace_s", "drain", "adopt", "cwd_template", "demand_cmd", "release_cmd", "prepare_cmd"]);
const DEFAULT_COOLDOWN_SECONDS = 60;
const DEFAULT_IDLE_GRACE_SECONDS = 300;
const DEFAULT_COMMAND_TIMEOUT_SECONDS = 60;

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
  for (const value of [min, max, cooldownSeconds, idleGraceSeconds]) if (isProblem(value)) return { problem: value };
  if ((max as number) < (min as number)) return { problem: { field: "max", message: "seat-pool: max must be at least min" } };
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
  const commands: Record<string, SeatPoolCommand | null> = {};
  for (const field of ["demand_cmd", "release_cmd", "prepare_cmd"]) {
    const parsed = command(context[field], field);
    if (isProblem(parsed)) return { problem: parsed };
    commands[field] = parsed;
  }
  return {
    spec: {
      min: min as number, max: max as number, cooldownSeconds: cooldownSeconds as number, idleGraceSeconds: idleGraceSeconds as number,
      drain, adopt: [...new Set(adopt as string[])], cwdTemplate,
      demandCmd: commands["demand_cmd"]!, releaseCmd: commands["release_cmd"]!, prepareCmd: commands["prepare_cmd"]!,
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

export interface SeatPoolSlotView {
  slotId: number;
  logicalId: string;
  nodeId: string | null;
  phase: Exclude<SeatPoolSlotPhase, "retired">;
  adopted: boolean;
  /** False once a linked node no longer exists (someone removed it). */
  nodeExists: boolean;
  /** Pending, in-progress and blocked rows addressed to the seat. */
  openRows: number;
  /** Messages the typing guard holds for the seat, not yet delivered. */
  heldMessages: number;
  /** When the seat last became idle at a prompt, with no needs-input and no background shell; null when it isn't. */
  idleSince: string | null;
}

export interface SeatPoolState {
  model: SeatPoolModel | null;
  restoring: boolean;
  slots: SeatPoolSlotView[];
  /** The spec's adopt entries that exist in the rig and no pool has linked yet. */
  adoptable: Array<{ logicalId: string; nodeId: string }>;
  /** Pending rows addressed to the model seat. */
  pendingForModel: number;
  /** The demand command's last good answer, when the spec has one. */
  demand: { desired: number; sampledAt: string } | null;
  /** The latest add or removal in this pool. */
  lastScaleAt: string | null;
}

export type SeatPoolIntent =
  | { kind: "adopt"; logicalId: string; nodeId: string }
  | { kind: "add"; count: number }
  /** Finish bringing up a seat that was added but not launched (also after a restart interrupted it). */
  | { kind: "launch"; slotId: number }
  | { kind: "drain"; slotId: number }
  /** Remove a seat that never took work, or forget one whose node is gone. */
  | { kind: "remove"; slotId: number };

export interface SeatPoolPlan {
  intents: SeatPoolIntent[];
  /** Why the pool holds, or what it is doing. */
  reason: string;
  /** Extra seats wanted, or null when the pool holds as it is. */
  target: number | null;
  current: number;
}

/**
 * Pure: the same state and clock always give the same plan.
 *
 * The target is the demand (the demand command's last answer, or the pending rows to the model plus the seats holding
 * work), never below the seats holding work plus the seats still starting, clamped to min..max; with `drain` it is 0.
 * A demand answer older than `sampleMaxAgeMs` holds the pool as it is. Only a free seat (no open rows, no held messages,
 * idle at a prompt for `idle_grace_s`) is drained, the pool's own before adopted ones and the newest first. Adds and new
 * drains wait for `cooldown_s` after the last add or removal, except under `drain`.
 */
export function planSeatPool(spec: SeatPoolSpec, state: SeatPoolState, nowMs: number, sampleMaxAgeMs: number): SeatPoolPlan {
  const hold = (reason: string): SeatPoolPlan => ({ intents: [], reason, target: null, current: state.slots.length });
  if (!state.model) return hold("model_not_found");
  if (state.restoring) return hold("restore_in_progress");

  const intents: SeatPoolIntent[] = [];
  if (!spec.drain) for (const seat of state.adoptable) intents.push({ kind: "adopt", ...seat });

  const present = state.slots.filter((slot) => slot.nodeId === null || slot.nodeExists);
  for (const gone of state.slots.filter((slot) => !present.includes(slot))) intents.push({ kind: "remove", slotId: gone.slotId });
  for (const slot of present.filter((s) => s.phase === "draining")) intents.push({ kind: "drain", slotId: slot.slotId });

  const starting = present.filter((slot) => slot.phase === "adding" || slot.phase === "added");
  const live = present.filter((slot) => slot.phase === "live");
  const idle = (slot: SeatPoolSlotView) => slot.openRows === 0 && slot.heldMessages === 0 && slot.idleSince !== null;
  const occupied = live.filter((slot) => !idle(slot));
  const free = live.filter((slot) => idle(slot) && nowMs - Date.parse(slot.idleSince!) >= spec.idleGraceSeconds * 1000);
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
      desired = state.pendingForModel + occupied.length;
    }
    if (desired === null) {
      // No current answer from the demand command: keep what exists and finish what was started.
      for (const slot of starting) intents.push({ kind: "launch", slotId: slot.slotId });
      return { intents, reason: "demand_sample_stale", target: null, current };
    }
    target = Math.min(spec.max, Math.max(spec.min, desired, occupied.length + starting.length));
    reason = target > current ? "scale_up" : target < current ? "scale_down" : "steady";
  }

  const surplus = Math.max(0, current - target);
  const drainOrder = [...free].sort((a, b) => Number(a.adopted) - Number(b.adopted) || b.slotId - a.slotId);
  if (!cooling) for (const slot of drainOrder.slice(0, surplus)) intents.push({ kind: "drain", slotId: slot.slotId });
  // A seat that never started holds no work. It goes only for the surplus free seats can't cover (a lowered max, or drain).
  const removeStarting = [...starting].sort((a, b) => b.slotId - a.slotId).slice(0, Math.max(0, surplus - drainOrder.length));
  for (const slot of starting) {
    intents.push(removeStarting.includes(slot) ? { kind: "remove", slotId: slot.slotId } : { kind: "launch", slotId: slot.slotId });
  }
  if (target > current && !cooling) intents.push({ kind: "add", count: target - current });
  if (cooling && target !== current) reason = "cooldown";
  return { intents, reason, target, current };
}

const HELD_REASONS = new Set(["model_not_found", "restore_in_progress", "demand_sample_stale"]);

export interface SeatPoolPolicyDeps {
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }): SeatPoolState;
  /** Hands the plan to the executor; false when the pool's previous plan is still being applied. */
  submit(jobId: string, intents: SeatPoolIntent[]): boolean;
  now?: () => number;
}

/** A demand answer counts while it is no older than two evaluation intervals. */
export function seatPoolSampleMaxAgeMs(intervalSeconds: number): number {
  return 2 * intervalSeconds * 1000;
}

export function makeSeatPoolPolicy(deps: SeatPoolPolicyDeps): Policy {
  return {
    name: SEAT_POOL_POLICY_NAME,
    async evaluate(job: PolicyJob): Promise<PolicyEvaluation> {
      const parsed = parseSeatPoolSpec(job.context);
      // Registration and update refuse an invalid spec; one that is invalid here holds the pool rather than act on it.
      if ("problem" in parsed) return { action: "skip", reason: "seat_pool_spec_invalid", notes: { ...parsed.problem } };
      const state = deps.readState({ jobId: job.jobId, targetSession: job.target.session, spec: parsed.spec });
      const plan = planSeatPool(parsed.spec, state, deps.now?.() ?? Date.now(), seatPoolSampleMaxAgeMs(job.intervalSeconds));
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
