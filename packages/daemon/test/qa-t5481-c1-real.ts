// Frozen eval of T-005497 (e-c1 of T-005481): the real capture and the adapter that turns it into the view the reader
// builds (SeatPoolSlotView with ParkFacts). Shared by qa-t5481-c1-policy.test.ts (layer G) and
// qa-t5481-c1-reader.test.ts (layer Q). Not a test file itself.
//
// The capture (T-003440): the production daemon 7433 (902f51ac7), 2026-11-11T05:02:55Z, read-only, by
// qualidade-qa-4@fabrica; the commands are in eval/c1.qa/real/COMANDO.txt. The files under
// fixtures/qa-t5481-c1/real/ are byte-for-byte copies of eval/c1.qa/real/ (cp, never rewritten). Nothing here writes them.
//
// How the adapter reads them (INTERFACE.md section 7):
//   sessionName  = <slot>.ps.json[0].canonicalSessionName (and the parked.json seat must say the same)
//   openRows     = parked.seat.obligations.openCount + heldCount
//   heldMessages = 0 (the capture has no held-message count; declared in the LEIA-ME)
//   atPrompt     = activity.value === "idle-at-prompt" && activity.needsInput.count === 0
//   backgroundWork = activity.backgroundWork !== null
//   cause        = needsInput > 0 ? "needs_input" : atPrompt && !backgroundWork ? "idle" : "working"
//   idleSince    = cause === "idle" ? activity.since : null
//   parked (only with exactly 1 open row, blocked):
//     blockedOn      = show.blockedOn
//     the current arm = the latest transition whose wake has phase "armed": parkActor = its actorSession, parkedAt = its ts
//     timerIntervalS = watchdog.intervalSeconds when the arm is a timer and watchdog.jobId is its ref
//     timerArmedLive = the held row's wake (parked.json): kind timer, phase armed, live, not unconsumed
//                      (for a cut of the history with no parked.json, the same four read from the transitions and the
//                      job's state: fired after the arm with the same ref → phase fired, live false, unconsumed while blocked)
//     takenAt        = show.claimedAt ?? the ts of the first in-progress transition whose actor is the row's destination

import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { ParkFacts, SeatPoolSlotView } from "../src/domain/policies/seat-pool.js";

export const CAPTURE_AT = "2026-11-11T05:02:55Z";
export const CAPTURE_NOW_MS = Date.parse(CAPTURE_AT);

const ROOT = new URL("./fixtures/qa-t5481-c1/real/", import.meta.url);

/** The slots captured, by short name, and their dated folders. */
export const SLOT_DIRS = {
  "build-dev-1-s6": "20261111T0502Z-build-dev-1-s6",
  "build-dev-1-s8": "20261111T0502Z-build-dev-1-s8",
  "build-dev-1-s9": "20261111T0502Z-build-dev-1-s9",
  "build-dev-1-s10": "20261111T0502Z-build-dev-1-s10",
  "build-dev-1-s11": "20261111T0502Z-build-dev-1-s11",
  "qualidade-qa-s4": "20261111T0502Z-qualidade-qa-s4",
  "qualidade-qa-s6": "20261111T0502Z-qualidade-qa-s6",
} as const;
export type CapturedSlot = keyof typeof SLOT_DIRS;

/** The park job of the s9 that fired at 04:44:51Z (park_timer_fired_once). */
export const TERMINAL_S9_FILE = "20261111T0502Z-terminal-s9/01M74BAQ16995V194YC1FPCF4D.watchdog.json";

export interface RealWake { transitionId: number; qitemId: string; phase: "armed" | "fired"; kind: string; ref: string; deliveryStatus: string | null }
export interface RealTransition {
  transitionId: number; qitemId: string; ts: string; state: string; transitionNote: string | null; actorSession: string; wake?: RealWake;
}
export interface RealShow {
  qitemId: string; state: string; blockedOn: string | null; sourceSession: string; destinationSession: string; claimedAt: string | null;
  chainOfRecord: string[] | null; body: string; summary: string | null; tags: string[] | null; resolution: string | null;
}
export interface RealWatchdog {
  jobId: string; policy: string; intervalSeconds: number; state: string; terminalReason: string | null; registeredBySession: string;
  targetSession: string; lastFireAt: string | null;
}
export interface RealHeldWake { kind: string; ref: string; live: boolean; phase: string; deliveryStatus: string | null; unconsumed: boolean }
export interface RealParked {
  ok: boolean;
  seat: {
    seatNodeId: string; sessionName: string;
    activity: { value: string; needsInput: { count: number }; since: string; backgroundWork: unknown | null };
    obligations: {
      openCount: number; heldCount: number;
      items: Array<{ qitemId: string; state: string }>;
      held: Array<{ qitemId: string; state: string; wake: RealHeldWake | null }>;
    };
  };
}
export interface RealPs { canonicalSessionName: string; logicalId: string; sessionStatus: string; seatPoolModel?: string }

export interface Capture {
  slot: CapturedSlot;
  dir: string;
  show: RealShow;
  transitions: RealTransition[];
  watchdog: RealWatchdog;
  parked: RealParked;
  ps: RealPs;
}

const readJson = (relative: string): unknown => JSON.parse(readFileSync(new URL(relative, ROOT), "utf8"));

export function loadCapture(slot: CapturedSlot): Capture {
  const dir = SLOT_DIRS[slot];
  const files = readdirSync(fileURLToPath(new URL(`${dir}/`, ROOT)));
  const one = (suffix: string): unknown => {
    const found = files.filter((name) => name.endsWith(suffix));
    if (found.length !== 1) throw new Error(`${dir}: expected one *${suffix}, found ${found.length}`);
    return readJson(`${dir}/${found[0]}`);
  };
  const ps = one(".ps.json") as RealPs[];
  if (ps.length !== 1) throw new Error(`${dir}: the ps capture must hold the slot's row only`);
  return {
    slot, dir,
    show: one(".show.json") as RealShow,
    transitions: one(".transitions.json") as RealTransition[],
    watchdog: one(".watchdog.json") as RealWatchdog,
    parked: one(".parked.json") as RealParked,
    ps: ps[0]!,
  };
}

export function loadTerminalS9(): RealWatchdog {
  return readJson(TERMINAL_S9_FILE) as RealWatchdog;
}

/** The transition that armed the row's current wake: the latest one whose wake is "armed". */
export function armedTransition(transitions: readonly RealTransition[]): RealTransition | null {
  return transitions.filter((t) => t.wake?.phase === "armed").at(-1) ?? null;
}

/** The wake status read from the transitions and the job (getStatus's rule), for a cut with no parked.json. */
export function wakeFromHistory(transitions: readonly RealTransition[], job: RealWatchdog, rowState: string): RealHeldWake | null {
  const armed = armedTransition(transitions);
  if (!armed) return null;
  const fired = transitions.find((t) => t.transitionId > armed.transitionId && t.wake?.phase === "fired" && t.wake.ref === armed.wake!.ref);
  return {
    kind: armed.wake!.kind,
    ref: armed.wake!.ref,
    phase: fired ? "fired" : "armed",
    live: !fired && job.jobId === armed.wake!.ref && job.state === "active",
    deliveryStatus: fired?.wake?.deliveryStatus ?? null,
    unconsumed: !!fired && rowState === "blocked",
  };
}

export const armedLive = (wake: RealHeldWake | null): boolean =>
  !!wake && wake.kind === "timer" && wake.phase === "armed" && wake.live === true && wake.unconsumed === false;

/** The park facts as the reader gives them, from the capture (or from a cut of its history with another job). */
export function captureFacts(
  cap: Pick<Capture, "show" | "transitions" | "watchdog" | "parked">,
  cut?: { transitions: RealTransition[]; watchdog: RealWatchdog },
): ParkFacts {
  const transitions = cut?.transitions ?? cap.transitions;
  const job = cut?.watchdog ?? cap.watchdog;
  const armed = armedTransition(transitions);
  let timerIntervalS: number | null = null;
  if (armed && armed.wake!.kind === "timer") {
    if (job.jobId !== armed.wake!.ref) throw new Error(`the captured job ${job.jobId} is not the current wake ${armed.wake!.ref}`);
    timerIntervalS = job.intervalSeconds;
  }
  const wake = cut
    ? wakeFromHistory(transitions, job, cap.show.state)
    : cap.parked.seat.obligations.held.find((h) => h.qitemId === cap.show.qitemId)?.wake ?? null;
  const firstTake = transitions.find((t) => t.state === "in-progress" && t.actorSession === cap.show.destinationSession);
  return {
    qitemId: cap.show.qitemId,
    blockedOn: cap.show.blockedOn!,
    timerIntervalS,
    timerArmedLive: armedLive(wake),
    parkActor: armed?.actorSession ?? null,
    parkedAt: armed?.ts ?? null,
    takenAt: cap.show.claimedAt ?? firstTake?.ts ?? null,
  };
}

/** The slot's view as the reader builds it with dispatch_to_parked on, from the capture. */
export function captureView(cap: Capture, slotId: number, facts?: ParkFacts): SeatPoolSlotView {
  const activity = cap.parked.seat.activity;
  const atPrompt = activity.value === "idle-at-prompt" && activity.needsInput.count === 0;
  const backgroundWork = activity.backgroundWork !== null && activity.backgroundWork !== undefined;
  const cause = activity.needsInput.count > 0 ? "needs_input" : atPrompt && !backgroundWork ? "idle" : "working";
  const obligations = cap.parked.seat.obligations;
  const open = [...obligations.items, ...obligations.held];
  const openRows = obligations.openCount + obligations.heldCount;
  const onlyBlocked = openRows === 1 && open.length === 1 && open[0]!.state === "blocked";
  return {
    slotId,
    logicalId: cap.ps.logicalId,
    sessionName: cap.ps.canonicalSessionName,
    nodeId: cap.parked.seat.seatNodeId,
    phase: "live",
    adopted: false,
    nodeExists: true,
    openRows,
    heldMessages: 0,
    idleSince: cause === "idle" ? activity.since : null,
    cause,
    atPrompt,
    backgroundWork,
    ...(onlyBlocked ? { parked: facts ?? captureFacts(cap) } : {}),
  };
}
