import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Hono } from "hono";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { eventsSchema } from "../src/db/migrations/003_events.js";
import { watchdogJobsSchema } from "../src/db/migrations/031_watchdog_jobs.js";
import { watchdogHistorySchema } from "../src/db/migrations/032_watchdog_history.js";
import { seatPoolSlotsSchema } from "../src/db/migrations/158_seat_pool_slots.js";
import { EventBus } from "../src/domain/event-bus.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { WatchdogHistoryLog } from "../src/domain/watchdog-history-log.js";
import { updateWatchdogJob } from "../src/domain/watchdog-job-update.js";
import { watchdogRoutes } from "../src/routes/watchdog.js";
import { SeatPoolSlots } from "../src/domain/seat-pool-executor.js";
import type { PolicyJob } from "../src/domain/policies/types.js";
import {
  makeSeatPoolPolicy,
  parseSeatPoolSpec,
  planSeatPool,
  SEAT_POOL_DRAIN_ENDED,
  SEAT_POOL_DRAIN_EXPIRED,
  SEAT_POOL_DRAIN_RETRYING,
  SEAT_POOL_DRAIN_STARTED,
  SEAT_POOL_POLICY_NAME,
  seatPoolDrainEpisode,
  type SeatPoolIntent,
  type SeatPoolPendingRow,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";

const NOW = Date.parse("2026-11-09T12:00:00.000Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const SAMPLE_MAX_AGE = 120_000;

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", cooldown_s: 60, idle_grace_s: 300, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

let nextSlot = 1;
function slot(overrides: Partial<SeatPoolSlotView> = {}): SeatPoolSlotView {
  const slotId = overrides.slotId ?? nextSlot++;
  return {
    slotId, logicalId: `dev.impl-s${slotId}`, sessionName: `dev-impl-s${slotId}@r`, nodeId: `node-${slotId}`, phase: "live", adopted: false,
    nodeExists: true, openRows: 0, heldMessages: 0, idleSince: ago(600), cause: "idle", ...overrides,
  };
}

let nextRow = 1;
/** Pending rows to `destination` (the model by default), created `ageSeconds` ago: past the default dispatch_after_s. */
function rows(count: number, destination = "dev-impl@r", ageSeconds = 120, tags: string[] = []): SeatPoolPendingRow[] {
  return Array.from({ length: count }, () => ({ qitemId: `q-${nextRow++}`, destination, waitingSince: ago(ageSeconds), tags }));
}

function state(overrides: Partial<SeatPoolState> = {}): SeatPoolState {
  return {
    model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: "dev-impl@r" },
    modelCause: "working", restoring: false, slots: [], adoptable: [], pending: [], demand: null, lastScaleAt: null, ...overrides,
  };
}

const plan = (s: SeatPoolSpec, st: SeatPoolState) => planSeatPool(s, st, NOW, SAMPLE_MAX_AGE);

describe("seat-pool spec", () => {
  it("fills the defaults and keeps commands as argv", () => {
    const parsed = parseSeatPoolSpec({ max: 2, cwd_template: "/w/{slot}", release_cmd: { argv: ["/bin/release", "--now"], timeout_s: 9 } });
    expect(parsed).toEqual({ spec: {
      min: 0, max: 2, cooldownSeconds: 60, idleGraceSeconds: 300, dispatchAfterSeconds: 60, dispatchTag: null, drain: false, drainDeadlineSeconds: 900, adopt: [], cwdTemplate: "/w/{slot}",
      demandCmd: null, prepareCmd: null, releaseCmd: { argv: ["/bin/release", "--now"], timeoutSeconds: 9 }, reapCmd: null, removeCmd: null,
      escalationOwner: null,
    } });
  });

  it.each([
    [{ cwd_template: "/w/{slot}" }, "max"],
    [{ max: 1, min: 2, cwd_template: "/w/{slot}" }, "max"],
    [{ max: 1 }, "cwd_template"],
    [{ max: 1, cwd_template: "w/{slot}" }, "cwd_template"],
    [{ max: 1, cwd_template: "/w/shared" }, "cwd_template"],
    [{ max: 1, cwd_template: "/w/{model}/{slot}" }, "cwd_template"],
    [{ max: 1, cwd_template: "/w/{slot}", demand_cmd: "pool-demand --json" }, "demand_cmd"],
    [{ max: 1, cwd_template: "/w/{slot}", prepare_cmd: { argv: [] } }, "prepare_cmd"],
    [{ max: 1, cwd_template: "/w/{slot}", release_cmd: { argv: ["x"], timeout_s: 0 } }, "release_cmd.timeout_s"],
    [{ max: 1, cwd_template: "/w/{slot}", restart_cmd: { argv: ["x"] } }, "restart_cmd"],
    [{ max: 1, cwd_template: "/w/{slot}", reap_cmd: "pool-reap" }, "reap_cmd"],
    [{ max: 1, cwd_template: "/w/{slot}", drain: "yes" }, "drain"],
    [{ max: 1, cwd_template: "/w/{slot}", dispatch_tag: " " }, "dispatch_tag"],
    [{ max: 1, cwd_template: "/w/{slot}", dispatch_after_s: -5 }, "dispatch_after_s"],
    [{ max: 1, cwd_template: "/w/{slot}", adopt: "dev.impl2" }, "adopt"],
    [{ max: 1, cwd_template: "/w/{slot}", drain_deadline_s: 86_401 }, "drain_deadline_s"],
  ])("refuses %j at %s", (context, field) => {
    const parsed = parseSeatPoolSpec(context);
    expect("problem" in parsed && parsed.problem.field).toBe(field);
  });

  // T-004870 F3, r1: the drain's clock lives in the watchdog history, whose shortest retention is 1 day.
  it("caps drain_deadline_s at 1 day, the shortest history retention, and keeps 0 as no deadline", () => {
    const refused = parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", drain_deadline_s: 86_401 });
    expect("problem" in refused && refused.problem.message).toMatch(/at most 86400/);
    for (const seconds of [0, 86_400]) {
      const parsed = parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", drain_deadline_s: seconds });
      expect("spec" in parsed && parsed.spec.drainDeadlineSeconds).toBe(seconds);
    }
  });
});

describe("seatPoolDrainEpisode", () => {
  const started = (at: string, deadlineAt: string | null) => ({ reason: SEAT_POOL_DRAIN_STARTED, notes: { startedAt: at, deadlineAt } });
  const expired = (startedAt: string) => ({ reason: SEAT_POOL_DRAIN_EXPIRED, notes: { startedAt, at: "x", left: [{ slot: "dev.impl-s1", why: "release_refused" }], moved: [] } });

  it("is the latest started with no ended after it, and its own expired only", () => {
    expect(seatPoolDrainEpisode([])).toBeNull();
    expect(seatPoolDrainEpisode([started("t1", "d1"), { reason: SEAT_POOL_DRAIN_ENDED, notes: {} }])).toBeNull();
    const episode = seatPoolDrainEpisode([started("t1", "d1"), expired("t1"), { reason: SEAT_POOL_DRAIN_ENDED, notes: {} }, started("t2", "d2"), expired("t1")]);
    expect(episode).toEqual({ startedAt: "t2", deadlineAt: "d2", expired: null });
  });

  // r1 rule 2: a pruned start is a new drain (the executor records another started), never a drain with no clock.
  it("has no current drain when the start was pruned, even with the drain's expired still there", () => {
    expect(seatPoolDrainEpisode([expired("t1")])).toBeNull();
  });
});

describe("planSeatPool", () => {
  beforeEach(() => { nextSlot = 1; nextRow = 1; });

  it("holds while the model is missing or the rig is being restored", () => {
    expect(plan(spec(), state({ model: null, pending: rows(5) }))).toMatchObject({ intents: [], reason: "model_not_found" });
    const restoring = plan(spec(), state({ restoring: true, pending: rows(5), modelCause: "stopped", slots: [slot({ phase: "added" }), slot()] }));
    expect(restoring).toMatchObject({ intents: [], reason: "restore_in_progress" });
  });

  it("adds one seat per row waiting past dispatch_after_s for the model, up to max", () => {
    expect(plan(spec(), state({ pending: rows(2) })).intents).toEqual([{ kind: "add", count: 2 }]);
    expect(plan(spec(), state({ pending: rows(9) })).intents).toEqual([{ kind: "add", count: 3 }]);
    expect(plan(spec({ min: 1 }), state()).intents).toEqual([{ kind: "add", count: 1 }]);
    // A row younger than dispatch_after_s is still the model's to take: it is not demand.
    expect(plan(spec(), state({ pending: rows(2, "dev-impl@r", 30) })).intents).toEqual([]);
    expect(plan(spec({ dispatch_after_s: 10 }), state({ pending: rows(2, "dev-impl@r", 30) })).intents).toEqual([{ kind: "add", count: 2 }]);
  });

  it("uses the demand command's answer, and holds when that answer is stale", () => {
    const withCmd = spec({ demand_cmd: { argv: ["/bin/demand"] } });
    expect(plan(withCmd, state({ demand: { desired: 2, sampledAt: ago(30) } })).intents).toEqual([{ kind: "add", count: 2 }]);
    const starting = slot({ phase: "added", idleSince: null });
    const free = slot();
    const stale = plan(withCmd, state({ demand: { desired: 0, sampledAt: ago(600) }, slots: [starting, free] }));
    expect(stale).toMatchObject({ reason: "demand_sample_stale", target: null });
    expect(stale.intents).toEqual([{ kind: "launch", slotId: starting.slotId }]);
    expect(plan(withCmd, state({ demand: null })).intents).toEqual([]);
  });

  it("never plans below the seats holding work plus the seats starting", () => {
    const busy = slot({ openRows: 1 });
    const working = slot({ idleSince: null });
    const starting = slot({ phase: "adding", nodeId: null, nodeExists: false });
    const result = plan(spec({ demand_cmd: { argv: ["/bin/demand"] } }), state({ demand: { desired: 0, sampledAt: ago(1) }, slots: [busy, working, starting] }));
    expect(result.target).toBe(3);
    expect(result.intents).toEqual([{ kind: "launch", slotId: starting.slotId }]);
  });

  it("drains only seats it added that are idle past the grace, newest first, and never an adopted one", () => {
    const adopted = slot({ adopted: true });
    const older = slot();
    const newer = slot();
    const recent = slot({ idleSince: ago(10) });
    const result = plan(spec(), state({ slots: [adopted, older, newer, recent] }));
    expect(result.intents).toEqual([
      { kind: "drain", slotId: newer.slotId, idleSince: newer.idleSince },
      { kind: "drain", slotId: older.slotId, idleSince: older.idleSince },
    ]);
  });

  it("counts a seat holding a message for it as holding work, so it is never drained", () => {
    const holding = slot({ heldMessages: 1 });
    const free = slot();
    const result = plan(spec(), state({ slots: [holding, free] }));
    expect(result.target).toBe(1);
    expect(result.intents).toEqual([{ kind: "drain", slotId: free.slotId, idleSince: free.idleSince }]);
  });

  it("waits for the cooldown before adding or starting a drain, but finishes what it started", () => {
    const starting = slot({ phase: "added", idleSince: null });
    const draining = slot({ phase: "draining" });
    const cooling = plan(spec(), state({ pending: rows(3), lastScaleAt: ago(10), slots: [starting, draining] }));
    expect(cooling.reason).toBe("cooldown");
    expect(cooling.intents).toEqual([{ kind: "drain", slotId: draining.slotId, idleSince: draining.idleSince }, { kind: "launch", slotId: starting.slotId }]);
    expect(plan(spec(), state({ slots: [slot()], lastScaleAt: ago(10) })).intents).toEqual([]);
    // A surplus of free seats waiting out the cooldown never takes a starting seat in their place.
    const waiting = plan(spec(), state({ slots: [slot(), slot(), starting], lastScaleAt: ago(10) }));
    expect(waiting.intents).toEqual([{ kind: "launch", slotId: starting.slotId }]);
  });

  it("drain empties the pool without touching work, gives adopted seats back, adopts nothing and ignores the cooldown", () => {
    const busy = slot({ openRows: 2 });
    const free = slot();
    const starting = slot({ phase: "added", idleSince: null });
    const adoptedBusy = slot({ adopted: true, openRows: 1 });
    const adoptedFree = slot({ adopted: true });
    const result = plan(spec({ drain: true, min: 2 }), state({
      pending: rows(4), modelCause: "stopped", lastScaleAt: ago(1), slots: [busy, free, starting, adoptedBusy, adoptedFree],
      adoptable: [{ logicalId: "dev.impl2", nodeId: "n2" }],
    }));
    expect(result.target).toBe(0);
    expect(result.intents).toEqual<SeatPoolIntent[]>([
      { kind: "unlink", slotId: adoptedBusy.slotId },
      { kind: "unlink", slotId: adoptedFree.slotId },
      { kind: "drain", slotId: free.slotId, idleSince: free.idleSince },
      { kind: "remove", slotId: starting.slotId },
    ]);
  });

  it("removes a seat started past a lowered max, and forgets a seat whose node is gone", () => {
    const busy = slot({ openRows: 1 });
    const starting = slot({ phase: "added", idleSince: null });
    const gone = slot({ nodeExists: false });
    const result = plan(spec({ max: 1 }), state({ slots: [busy, starting, gone] }));
    expect(result.intents).toEqual([{ kind: "remove", slotId: gone.slotId }, { kind: "remove", slotId: starting.slotId }]);
  });

  it("adopts the listed seats no pool holds yet", () => {
    const result = plan(spec(), state({ adoptable: [{ logicalId: "dev.impl2", nodeId: "n2" }] }));
    expect(result.intents).toEqual([{ kind: "adopt", logicalId: "dev.impl2", nodeId: "n2" }]);
  });

  it("counts every adopted seat against max, stopped or adopted in the same plan, and adds only the room left", () => {
    const adopting = [{ logicalId: "dev.impl2", nodeId: "n2" }, { logicalId: "dev.impl3", nodeId: "n3" }];
    // The executor links the two seats before it adds: the add asks only for the room they leave.
    expect(plan(spec(), state({ adoptable: adopting, pending: rows(5) })).intents)
      .toEqual([...adopting.map((seat) => ({ kind: "adopt", ...seat })), { kind: "add", count: 1 }]);
    const full = plan(spec({ max: 2 }), state({ adoptable: adopting, pending: rows(5) }));
    expect(full.intents.filter((intent) => intent.kind === "add")).toEqual([]);
    expect(full.reason).toBe("max_reached");
    // A stopped adopted seat takes no row and isn't counted as current, but it still takes room under max.
    const stopped = slot({ adopted: true, cause: "stopped", idleSince: null });
    expect(plan(spec({ max: 2 }), state({ slots: [stopped], pending: rows(5) })).intents).toEqual([{ kind: "add", count: 1 }]);
  });
});

describe("planSeatPool: dispatch and wake", () => {
  beforeEach(() => { nextSlot = 1; nextRow = 1; });

  it("moves waiting rows to free seats, oldest first, and counts the rest as demand", () => {
    const older = rows(1, "dev-impl@r", 900);
    const newer = rows(2);
    const free = slot();
    const result = plan(spec(), state({ pending: [...older, ...newer], slots: [free] }));
    expect(result.intents).toEqual<SeatPoolIntent[]>([
      { kind: "dispatch", qitemId: older[0]!.qitemId, from: "dev-impl@r", slotId: free.slotId },
      { kind: "add", count: 2 },
    ]);
    // The seat that receives a row holds work: it counts toward the target and is not drained.
    expect(result.target).toBe(3);
  });

  it("moves a row addressed to a seat the pool removed, and never a row addressed to one of its live seats", () => {
    const busy = slot({ openRows: 1, idleSince: null, cause: "working" });
    const free = slot();
    const orphan = rows(1, "dev-impl-s9@r");
    const held = rows(1, busy.sessionName);
    const result = plan(spec(), state({ pending: [...orphan, ...held], slots: [busy, free] }));
    expect(result.intents).toEqual([{ kind: "dispatch", qitemId: orphan[0]!.qitemId, from: "dev-impl-s9@r", slotId: free.slotId }]);
  });

  it("with dispatch_tag, moves and counts only the model's rows carrying it, and still a row to a seat it removed", () => {
    const free = [slot(), slot()];
    const tagged = rows(1, "dev-impl@r", 900, ["role:impl"]);
    const reply = rows(1, "dev-impl@r", 900, ["review"]);
    const orphan = rows(1, "dev-impl-s9@r");
    const result = plan(spec({ dispatch_tag: "role:impl", max: 9 }), state({ pending: [...tagged, ...reply, ...orphan, ...rows(2)], slots: free }));
    // The reply and the untagged rows stay with the model, which is working on them: they are not demand.
    expect(result.intents).toEqual<SeatPoolIntent[]>([
      { kind: "dispatch", qitemId: tagged[0]!.qitemId, from: "dev-impl@r", slotId: free[0]!.slotId },
      { kind: "dispatch", qitemId: orphan[0]!.qitemId, from: "dev-impl-s9@r", slotId: free[1]!.slotId },
    ]);
    expect(result.target).toBe(2);
  });

  it("only gives a row to a seat that is idle at its prompt with nothing open or held", () => {
    const seats = [
      slot({ openRows: 1 }),
      slot({ heldMessages: 1 }),
      slot({ idleSince: null, cause: "needs_input" }),
      slot({ idleSince: null, cause: "working" }),
      slot({ idleSince: null, cause: "stopped" }),
    ];
    const result = plan(spec({ max: 9 }), state({ pending: rows(1), slots: seats }));
    expect(result.intents.filter((i) => i.kind === "dispatch")).toEqual([]);
    expect(result.intents).toContainEqual({ kind: "add", count: 1 });
  });

  it("wakes a seat of the pool holding a waiting row by its cause, and reaps one whose session is gone", () => {
    const stopped = slot({ cause: "stopped", openRows: 2, idleSince: null });
    const idle = slot({ openRows: 2 });
    const asking = slot({ cause: "needs_input", openRows: 1, idleSince: null });
    const askingRows = rows(1, asking.sessionName);
    const young = rows(1, stopped.sessionName, 10);
    const result = plan(spec(), state({
      pending: [...rows(1, stopped.sessionName), ...young, ...rows(1, idle.sessionName), ...askingRows], slots: [stopped, idle, asking],
    }));
    // The idle one is left to the queue's own wakes.
    expect(result.intents).toEqual<SeatPoolIntent[]>([
      { kind: "reap", slotId: stopped.slotId },
      { kind: "escalate", seat: asking.sessionName, qitemIds: [askingRows[0]!.qitemId] },
    ]);
  });

  it("launches an adopted seat whose session is gone when it holds a waiting row, never reaps it, and doesn't count it", () => {
    const asleep = slot({ adopted: true, cause: "stopped", idleSince: null });
    const holding = slot({ adopted: true, cause: "stopped", idleSince: null, openRows: 1 });
    const waiting = rows(1, holding.sessionName);
    const result = plan(spec(), state({ pending: [...waiting, ...rows(1)], slots: [asleep, holding] }));
    // Neither takes the model's row, so it is demand for a seat of the pool's own.
    expect(result.intents).toEqual<SeatPoolIntent[]>([
      { kind: "relaunch", seat: holding.sessionName, nodeId: holding.nodeId! },
      { kind: "add", count: 1 },
    ]);
    expect(result).toMatchObject({ current: 0, target: 1 });
  });

  it("launches a stopped model, escalates one that needs input, and leaves one idle or working", () => {
    const waiting = rows(2);
    expect(plan(spec(), state({ modelCause: "stopped", pending: waiting })).intents).toEqual([
      { kind: "relaunch", seat: "dev-impl@r", nodeId: "model" },
      { kind: "add", count: 2 },
    ]);
    expect(plan(spec(), state({ modelCause: "idle", pending: waiting })).intents).toEqual([{ kind: "add", count: 2 }]);
    expect(plan(spec(), state({ modelCause: "needs_input", pending: waiting })).intents).toEqual([
      { kind: "escalate", seat: "dev-impl@r", qitemIds: waiting.map((r) => r.qitemId) },
      { kind: "add", count: 2 },
    ]);
    expect(plan(spec(), state({ modelCause: "working", pending: waiting })).intents).toEqual([{ kind: "add", count: 2 }]);
  });

  it("adds no seat and launches no stopped seat while a seat of the pool is out of usage", async () => {
    const waiting = rows(2);
    expect(plan(spec(), state({ modelCause: "usage_limit", pending: waiting }))).toMatchObject({ intents: [], reason: "usage_limit", target: 0 });
    const stopped = slot({ cause: "stopped", openRows: 1, idleSince: null });
    const free = slot();
    const result = plan(spec({ max: 5 }), state({ pending: [...rows(1, stopped.sessionName), ...waiting], slots: [stopped, free, slot({ cause: "usage_limit", idleSince: null })] }));
    // The free seat still takes a row, and the stopped seat is reaped; nothing is launched or added.
    expect(result.intents).toEqual([
      { kind: "reap", slotId: stopped.slotId },
      { kind: "dispatch", qitemId: waiting[0]!.qitemId, from: "dev-impl@r", slotId: free.slotId },
    ]);
    expect(result.reason).toBe("usage_limit");
    const evaluated = await makeSeatPoolPolicy({ readState: async () => state({ modelCause: "usage_limit", pending: waiting }), submit: () => true, now: () => NOW })
      .evaluate({ jobId: "pool-1", policy: SEAT_POOL_POLICY_NAME, target: { session: "dev-impl@r" }, intervalSeconds: 60, context: { max: 2, cwd_template: "/w/{slot}" } } as PolicyJob);
    expect(evaluated).toMatchObject({ reason: "seat_pool_held", notes: { reason: "usage_limit" } });
  });

  it("launches no seat it added and hasn't started while a seat is out of usage, and still removes an unwanted one", () => {
    const added = slot({ phase: "added", cause: "stopped", idleSince: null });
    const held = plan(spec(), state({ modelCause: "usage_limit", slots: [added] }));
    expect(held).toMatchObject({ intents: [], reason: "usage_limit" });
    // The same holds while the demand command's answer is stale.
    expect(plan(spec({ demand_cmd: { argv: ["/bin/demand"] } }), state({ modelCause: "usage_limit", slots: [added] })).intents).toEqual([]);
    expect(plan(spec({ max: 0 }), state({ modelCause: "usage_limit", slots: [added] })).intents).toEqual([{ kind: "remove", slotId: added.slotId }]);
    expect(plan(spec(), state({ slots: [added] })).intents).toEqual([{ kind: "launch", slotId: added.slotId }]);
  });

  it("reaps a seat whose session is gone under drain too, and never a seat still starting or whose node is gone", () => {
    const stopped = slot({ cause: "stopped", idleSince: null, openRows: 1 });
    const starting = slot({ phase: "added", cause: "stopped", idleSince: null });
    const gone = slot({ cause: "stopped", idleSince: null, nodeExists: false });
    const result = plan(spec({ drain: true }), state({ slots: [stopped, starting, gone] }));
    expect(result.intents).toEqual<SeatPoolIntent[]>([
      { kind: "remove", slotId: gone.slotId },
      { kind: "reap", slotId: stopped.slotId },
      { kind: "remove", slotId: starting.slotId },
    ]);
  });

  it("moves and wakes nothing under drain", () => {
    const free = slot();
    const result = plan(spec({ drain: true }), state({ modelCause: "stopped", pending: [...rows(2), ...rows(1, "dev-impl-s9@r")], slots: [free] }));
    expect(result.intents).toEqual([{ kind: "drain", slotId: free.slotId, idleSince: free.idleSince }]);
  });

  it("still moves and wakes while the demand command's answer is stale", () => {
    const free = slot();
    const waiting = rows(2);
    const result = plan(spec({ demand_cmd: { argv: ["/bin/demand"] } }), state({ modelCause: "idle", pending: waiting, slots: [free] }));
    expect(result).toMatchObject({ reason: "demand_sample_stale", target: null });
    expect(result.intents).toEqual([
      { kind: "dispatch", qitemId: waiting[0]!.qitemId, from: "dev-impl@r", slotId: free.slotId },
    ]);
  });
});


describe("seat-pool policy evaluation", () => {
  const job = (context: Record<string, unknown>): PolicyJob => ({
    jobId: "pool-1", policy: SEAT_POOL_POLICY_NAME, target: { session: "dev-impl@r" }, intervalSeconds: 60, context,
  } as PolicyJob);

  it("hands the plan to the executor and reports it, quietly when nothing changes", async () => {
    const submitted: SeatPoolIntent[][] = [];
    const policy = makeSeatPoolPolicy({
      readState: () => state({ pending: rows(1) }),
      submit: (_jobId, intents) => { submitted.push(intents); return true; },
      now: () => NOW,
    });
    const planned = await policy.evaluate(job({ max: 2, cwd_template: "/w/{slot}" }));
    expect(planned).toMatchObject({ action: "skip", reason: "seat_pool_planned", notes: { target: 1, intents: [{ kind: "add", count: 1 }] } });
    const steady = await makeSeatPoolPolicy({ readState: () => state(), submit: (_j, i) => { submitted.push(i); return true; }, now: () => NOW })
      .evaluate(job({ max: 2, cwd_template: "/w/{slot}" }));
    expect(steady).toMatchObject({ action: "skip", reason: "seat_pool_steady" });
    // A plan with nothing to do still reaches the executor, which refreshes the demand.
    expect(submitted).toEqual([[{ kind: "add", count: 1 }], []]);
  });

  it("reports a pool held by restore or a missing model, and a pool whose previous plan still runs", async () => {
    const held = await makeSeatPoolPolicy({ readState: () => state({ restoring: true }), submit: () => true, now: () => NOW })
      .evaluate(job({ max: 2, cwd_template: "/w/{slot}" }));
    expect(held).toMatchObject({ reason: "seat_pool_held", notes: { reason: "restore_in_progress" } });
    const busy = await makeSeatPoolPolicy({ readState: () => state({ pending: rows(1) }), submit: () => false, now: () => NOW })
      .evaluate(job({ max: 2, cwd_template: "/w/{slot}" }));
    expect(busy).toEqual({ action: "skip", reason: "seat_pool_busy" });
  });

  it("holds an invalid stored spec without reading state or submitting", async () => {
    let touched = false;
    const result = await makeSeatPoolPolicy({ readState: () => { touched = true; return state(); }, submit: () => { touched = true; return true; } })
      .evaluate(job({ max: 2 }));
    expect(result).toMatchObject({ action: "skip", reason: "seat_pool_spec_invalid", notes: { field: "cwd_template" } });
    expect(touched).toBe(false);
  });
});

describe("seat-pool registration, update and stop", () => {
  let db: Database.Database;
  let jobsRepo: WatchdogJobsRepository;
  let app: Hono;
  const specYaml = "policy: seat-pool\ncontext:\n  max: 2\n  cwd_template: /work/{slot}\n";
  const register = (yaml: string) => app.request("/api/watchdog/register", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ policy: "seat-pool", specYaml: yaml, targetSession: "dev-impl@r", intervalSeconds: 60, registeredBySession: "ops@kernel" }),
  });

  beforeEach(() => {
    db = createDb();
    migrate(db, [coreSchema, eventsSchema, watchdogJobsSchema, watchdogHistorySchema, seatPoolSlotsSchema]);
    jobsRepo = new WatchdogJobsRepository(db);
    const bus = new EventBus(db);
    const log = new WatchdogHistoryLog(db);
    app = new Hono();
    app.use("*", async (c, next) => {
      c.set("eventBus" as never, bus);
      c.set("watchdogJobsRepo" as never, jobsRepo);
      c.set("watchdogHistoryLog" as never, log);
      await next();
    });
    app.route("/api/watchdog", watchdogRoutes());
  });
  afterEach(() => db.close());

  it("refuses an invalid spec at registration and update, before anything is stored", async () => {
    const refused = await register("policy: seat-pool\ncontext:\n  max: 2\n  cwd_template: /work/shared\n");
    expect(refused.status).toBe(400);
    expect(await refused.json()).toMatchObject({ error: "spec_invalid", message: expect.stringContaining("{slot}") });
    expect(jobsRepo.listAll()).toEqual([]);

    const created = await register(specYaml);
    expect(created.status).toBe(201);
    const { jobId } = await created.json() as { jobId: string };
    expect(() => updateWatchdogJob(jobsRepo, jobId, { specYaml: "policy: seat-pool\ncontext:\n  max: 2\n  cwd_template: /w/{slot}\n  release_cmd: release-now\n" }))
      .toThrow(/release_cmd/);
    const drained = updateWatchdogJob(jobsRepo, jobId, { specYaml: `${specYaml}  drain: true\n` });
    expect(drained.changed).toEqual(["specYaml"]);
  });

  it("refuses to stop a pool that still holds seats, and stops it once they are retired", async () => {
    const job = jobsRepo.register({ policy: "seat-pool", specYaml, targetSession: "dev-impl@r", intervalSeconds: 60, registeredBySession: "ops@kernel" });
    const slots = new SeatPoolSlots(db);
    const held = slots.insert({ poolJobId: job.jobId, logicalId: "dev.impl-s1", nodeId: "n1", phase: "live", intent: null, opId: "op-1", at: ago(0) });
    const stop = () => app.request(`/api/watchdog/${job.jobId}/stop`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    const refused = await stop();
    expect(refused.status).toBe(409);
    expect(await refused.json()).toMatchObject({ error: "pool_has_slots", message: expect.stringContaining("drain: true") });
    expect(jobsRepo.getByIdOrThrow(job.jobId).state).toBe("active");
    slots.retire(held.slotId, ago(0));
    expect((await stop()).status).toBe(200);
    expect(jobsRepo.getByIdOrThrow(job.jobId).state).toBe("stopped");
  });

  // T-004870 F3, r1 rule 3: the stop needs the forced pass's proof in the history; once it is pruned, the stop refuses.
  it("refuses to stop a drained pool again once the forced pass's entry is pruned, and says when the deadline passes", async () => {
    const job = jobsRepo.register({ policy: "seat-pool", specYaml: `${specYaml}  drain: true
`, targetSession: "dev-impl@r", intervalSeconds: 60, registeredBySession: "ops@kernel" });
    const slots = new SeatPoolSlots(db);
    slots.insert({ poolJobId: job.jobId, logicalId: "dev.impl-s1", nodeId: "n1", phase: "live", intent: null, opId: "op-1", at: ago(0) });
    const log = new WatchdogHistoryLog(db);
    const entry = (reason: string, notes: Record<string, unknown>) =>
      log.record({ jobId: job.jobId, evaluatedAt: ago(0), outcome: "skipped", skipReason: reason, evaluationNotes: notes });
    const startedAt = "2026-11-09T12:00:00.000Z";
    const deadlineAt = "2026-11-09T12:15:00.000Z";
    entry(SEAT_POOL_DRAIN_STARTED, { startedAt, deadlineAt });
    const pass = entry(SEAT_POOL_DRAIN_EXPIRED, { startedAt, deadlineAt, at: deadlineAt, left: [{ slot: "dev.impl-s1", why: "release_refused" }], moved: [] });
    const stop = () => app.request(`/api/watchdog/${job.jobId}/stop`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });

    db.prepare("DELETE FROM watchdog_history WHERE history_id = ?").run(pass.historyId);
    const refused = await stop();
    expect(refused.status).toBe(409);
    expect(await refused.json()).toMatchObject({ error: "pool_has_slots", message: expect.stringContaining(deadlineAt) });
    expect(jobsRepo.getByIdOrThrow(job.jobId).state).toBe("active");

    db.prepare("DELETE FROM watchdog_history WHERE job_id = ?").run(job.jobId);
    expect((await stop()).status).toBe(409);
    expect(jobsRepo.getByIdOrThrow(job.jobId).state).toBe("active");
  });

  // The 409 tells the current drain, not the spec: drain_deadline_s changed during a drain applies to the next one.
  it("says the current drain has no deadline when it started with 0, even after the spec changed to a deadline", async () => {
    const job = jobsRepo.register({ policy: "seat-pool", specYaml: `${specYaml}  drain: true\n  drain_deadline_s: 600\n`, targetSession: "dev-impl@r", intervalSeconds: 60, registeredBySession: "ops@kernel" });
    new SeatPoolSlots(db).insert({ poolJobId: job.jobId, logicalId: "dev.impl-s1", nodeId: "n1", phase: "live", intent: null, opId: "op-1", at: ago(0) });
    new WatchdogHistoryLog(db).record({ jobId: job.jobId, evaluatedAt: ago(0), outcome: "skipped", skipReason: SEAT_POOL_DRAIN_STARTED, evaluationNotes: { startedAt: ago(0), deadlineAt: null } });
    const refused = await app.request(`/api/watchdog/${job.jobId}/stop`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    expect(refused.status).toBe(409);
    expect(await refused.json()).toMatchObject({ error: "pool_has_slots", message: expect.stringContaining("started with drain_deadline_s: 0") });
  });
});

// T-005245 (F3s), the stop and the show of a drained pool.
describe("F3s: the stop says which seats were left stopped, and the show and the 409 say the forced pass's retry", () => {
  let db: Database.Database;
  let jobsRepo: WatchdogJobsRepository;
  let app: Hono;
  const SPEC = "policy: seat-pool\ncontext:\n  max: 3\n  cwd_template: /w/{slot}\n  drain: true\n";
  beforeEach(() => {
    db = createDb();
    migrate(db, [coreSchema, eventsSchema, watchdogJobsSchema, watchdogHistorySchema, seatPoolSlotsSchema]);
    jobsRepo = new WatchdogJobsRepository(db);
    app = new Hono();
    const log = new WatchdogHistoryLog(db);
    const bus = new EventBus(db);
    app.use("*", async (c, next) => {
      c.set("eventBus" as never, bus);
      c.set("watchdogJobsRepo" as never, jobsRepo);
      c.set("watchdogHistoryLog" as never, log);
      await next();
    });
    app.route("/api/watchdog", watchdogRoutes());
  });
  afterEach(() => db.close());
  const startedAt = "2026-11-09T12:00:00.000Z";
  const deadlineAt = "2026-11-09T12:15:00.000Z";
  const pool = (seats: string[]) => {
    const job = jobsRepo.register({ policy: "seat-pool", specYaml: SPEC, targetSession: "dev-impl@r", intervalSeconds: 60, registeredBySession: "ops@kernel" });
    const slots = new SeatPoolSlots(db);
    seats.forEach((logicalId, i) => slots.insert({ poolJobId: job.jobId, logicalId, nodeId: `n${i}`, phase: "draining", intent: null, opId: `op-${i}`, at: ago(0) }));
    const log = new WatchdogHistoryLog(db);
    const entry = (reason: string, notes: Record<string, unknown>) =>
      log.record({ jobId: job.jobId, evaluatedAt: ago(0), outcome: "skipped", skipReason: reason, evaluationNotes: notes });
    entry(SEAT_POOL_DRAIN_STARTED, { startedAt, deadlineAt });
    return { job, entry };
  };
  const stop = (jobId: string) => app.request(`/api/watchdog/${jobId}/stop`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });

  it("S8: leftRunning and seat_pool_left carry stopped for a seat the pass left stopped (also an older move_failed/remove_failed entry)", async () => {
    const { job, entry } = pool(["dev.impl-s1", "dev.impl-s2", "dev.impl-s3"]);
    entry(SEAT_POOL_DRAIN_EXPIRED, { startedAt, deadlineAt, at: deadlineAt, moved: [], left: [
      { slot: "dev.impl-s1", why: "stop_failed", exitCode: 0, stopped: true },
      { slot: "dev.impl-s2", why: "release_refused", exitCode: 1 },
      { slot: "dev.impl-s3", why: "move_failed", exitCode: 0 },
    ] });
    const stopped = await stop(job.jobId);
    expect(stopped.status).toBe(200);
    expect((await stopped.json() as { leftRunning: unknown[] }).leftRunning).toEqual([
      { slot: "dev.impl-s1", sessionName: "dev-impl-s1@r", why: "stop_failed", stopped: true },
      { slot: "dev.impl-s2", sessionName: "dev-impl-s2@r", why: "release_refused" },
      { slot: "dev.impl-s3", sessionName: "dev-impl-s3@r", why: "move_failed", stopped: true },
    ]);
    const left = db.prepare("SELECT evaluation_notes FROM watchdog_history WHERE job_id = ? AND skip_reason = 'seat_pool_left' ORDER BY rowid").all(job.jobId) as Array<{ evaluation_notes: string }>;
    expect(left.map((row) => JSON.parse(row.evaluation_notes).stopped ?? false)).toEqual([true, false, true]);
  });

  it("S5: while the pass asks again, the show gives retryUntil and the seats waiting, and the 409 says until when", async () => {
    const { job, entry } = pool(["dev.impl-s1", "dev.impl-s2"]);
    const retryUntil = "2026-11-09T12:25:00.000Z";
    entry(SEAT_POOL_DRAIN_RETRYING, { startedAt, deadlineAt, at: deadlineAt, retryUntil, left: [], moved: [], waiting: [{ slot: "dev.impl-s2", exitCode: 3 }] });
    const show = await (await app.request(`/api/watchdog/${job.jobId}`)).json() as { seatPool: { drain: Record<string, unknown> } };
    expect(show.seatPool.drain).toEqual({ startedAt, deadlineAt, expired: false, retryUntil, waiting: ["dev.impl-s2"] });
    const refused = await stop(job.jobId);
    expect(refused.status).toBe(409);
    expect(await refused.json()).toMatchObject({ error: "pool_has_slots", message: expect.stringMatching(new RegExp(`dev\\.impl-s2 until ${retryUntil}`)) });
    // With no pass under way, the show says nothing of a retry.
    entry(SEAT_POOL_DRAIN_EXPIRED, { startedAt, deadlineAt, at: retryUntil, moved: [], left: [{ slot: "dev.impl-s2", why: "release_unavailable", exitCode: 3 }] });
    const after = await (await app.request(`/api/watchdog/${job.jobId}`)).json() as { seatPool: { drain: Record<string, unknown> } };
    expect(after.seatPool.drain).toEqual({ startedAt, deadlineAt, expired: true });
  });
});

describe("F6a: escalation_owner, max_reached in the history, the room a plan frees", () => {
  it("reads escalation_owner as a seat address, and refuses anything else", () => {
    expect(spec({ escalation_owner: "plataforma-supervisor@comando" }).escalationOwner).toBe("plataforma-supervisor@comando");
    expect(spec().escalationOwner).toBeNull();
    for (const bad of ["", "  ", "supervisor", "a b@c", 3, ["x@y"]]) {
      expect(parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", escalation_owner: bad })).toMatchObject({ problem: { field: "escalation_owner" } });
    }
  });

  it("a pool held at max with nothing to change says so in its history", async () => {
    // The REVISAO-f1 S2 shape: a stopped adopted seat takes room under max but isn't one of the seats taking work.
    const full = [slot({ cause: "working", idleSince: null, openRows: 1 }), slot({ adopted: true, cause: "stopped", idleSince: null })];
    const evaluated = await makeSeatPoolPolicy({ readState: () => state({ slots: full, pending: rows(3) }), submit: () => true, now: () => NOW })
      .evaluate({ jobId: "pool-1", policy: SEAT_POOL_POLICY_NAME, target: { session: "dev-impl@r" }, intervalSeconds: 60,
        context: { max: 2, cwd_template: "/w/{slot}" } } as PolicyJob);
    expect(evaluated).toMatchObject({ action: "skip", reason: "seat_pool_held", notes: { reason: "max_reached", intents: [] } });
  });

  it("counts as room the seats the same plan forgets or reaps, so a full pool replaces a gone seat in the same plan", () => {
    const busy = () => slot({ cause: "working", idleSince: null, openRows: 1 });
    const gone = slot({ nodeExists: false, cause: "stopped", idleSince: null });
    const result = plan(spec({ max: 3, cooldown_s: 300 }), state({ slots: [busy(), busy(), gone], pending: rows(3) }));
    expect(result.intents).toEqual([{ kind: "remove", slotId: gone.slotId }, { kind: "add", count: 1 }]);
    const dead = slot({ cause: "stopped", idleSince: null });
    const reaping = plan(spec({ max: 3, demand_cmd: { argv: ["demand"] } }),
      state({ slots: [busy(), dead], demand: { desired: 3, sampledAt: ago(10) } }));
    // The seat being reaped is neither current nor room: demand 3 with one working seat asks for 2.
    expect(reaping.intents).toEqual([{ kind: "reap", slotId: dead.slotId }, { kind: "add", count: 2 }]);
    // RC r1 S1: with max 2, one working seat and one to reap, demand 2 replaces the reaped seat in the same plan.
    const one = busy();
    const gonePlan = plan(spec({ max: 2, demand_cmd: { argv: ["demand"] } }),
      state({ slots: [one, slot({ cause: "stopped", idleSince: null })], demand: { desired: 2, sampledAt: ago(10) } }));
    expect(gonePlan.intents.filter((intent) => intent.kind === "add")).toEqual([{ kind: "add", count: 1 }]);
    expect(gonePlan.current).toBe(1);
  });
});

describe("F6a: a needs-input escalation only for the rows the pool owns", () => {
  const tagged = (destination: string) => rows(1, destination, 120, ["pool:new"]);
  it("with dispatch_tag, an adopted seat or the model waiting on an untagged row of its own is no alert", () => {
    const fixa = slot({ adopted: true, cause: "needs_input", idleSince: null, openRows: 1 });
    const own = rows(1, fixa.sessionName);
    const s = spec({ dispatch_tag: "pool:new" });
    expect(plan(s, state({ slots: [fixa], pending: own })).intents.filter((i) => i.kind === "escalate")).toEqual([]);
    const model = plan(s, state({ modelCause: "needs_input", pending: rows(1) }));
    expect(model.intents.filter((i) => i.kind === "escalate")).toEqual([]);
    // A tagged row is the pool's: it is escalated, and only it is named.
    const work = tagged(fixa.sessionName);
    expect(plan(s, state({ slots: [fixa], pending: [...own, ...work] })).intents.filter((i) => i.kind === "escalate"))
      .toEqual([{ kind: "escalate", seat: fixa.sessionName, qitemIds: [work[0]!.qitemId] }]);
  });

  it("a seat the pool added is escalated for any waiting row, and without dispatch_tag every seat is, as before", () => {
    const added = slot({ cause: "needs_input", idleSince: null, openRows: 1 });
    const fixa = slot({ adopted: true, cause: "needs_input", idleSince: null, openRows: 1 });
    const waiting = [...rows(1, added.sessionName), ...rows(1, fixa.sessionName)];
    const withTag = plan(spec({ dispatch_tag: "pool:new" }), state({ slots: [added, fixa], pending: waiting }));
    expect(withTag.intents.filter((i) => i.kind === "escalate").map((i) => (i as { seat: string }).seat)).toEqual([added.sessionName]);
    const withoutTag = plan(spec(), state({ slots: [added, fixa], pending: waiting }));
    expect(withoutTag.intents.filter((i) => i.kind === "escalate").map((i) => (i as { seat: string }).seat).sort())
      .toEqual([added.sessionName, fixa.sessionName].sort());
  });
});
