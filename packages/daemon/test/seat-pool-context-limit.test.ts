// T-012383: a seat the pool added whose context is at `context_limit_percent` or above (default 40), as the status-line
// sidecar of its current occupant reads it (used_percentage, sampled_at under 10 min: the rule the Billverso's
// orchestration gate, portao._contexto, applies to the same file), takes no row, free or parked.
//
// The case (17/11, the build-dev pool at max): the core kept handing rows to slots past 40%, which by their own rule
// take no new work, so each gave the row straight back to the model and the next plan handed it to the next full slot.
// T-011798 went s28 -> s24 -> model and T-012070 went s31 -> s24 -> model: two dispatches each, no work done, and the row
// back where it started. With the limit the plan never hands those rows to a full slot: it counts the full slot as
// occupied, so the pool grows for the rows instead, and a slot under the limit still takes them.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
import { ContextUsageStore } from "../src/domain/context-usage-store.js";
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
  SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT,
  seatPoolCanReceive,
  seatPoolContextOver,
  seatPoolHeldByContext,
  type ParkFacts,
  type SeatPoolIntent,
  type SeatPoolPendingRow,
  type SeatPoolSlotView,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import type { Policy, PolicyJob } from "../src/domain/policies/types.js";

// ---------------------------------------------------------------------------------------------------------------
// The pure layer: the spec, the predicate and the planner
// ---------------------------------------------------------------------------------------------------------------
const NOW = Date.parse("2026-11-17T19:40:00.000Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const MODEL_SESSION = "build-dev-1@fabrica";
const TAG = "tarefa-nova";

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 6, cwd_template: "/w/{rig}/{slot}", cooldown_s: 0, idle_grace_s: 300, dispatch_tag: TAG, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

/** A slot of the pool, live and free (idle at its prompt for 10 min, no row), named as the real ones. */
function slot(n: number, overrides: Partial<SeatPoolSlotView> = {}): SeatPoolSlotView {
  return {
    slotId: n, logicalId: `build.dev-1-s${n}`, sessionName: `build-dev-1-s${n}@fabrica`, nodeId: `node-s${n}`, phase: "live",
    adopted: false, nodeExists: true, openRows: 0, heldMessages: 0, idleSince: ago(600), cause: "idle", ...overrides,
  };
}

/** The context reading of a full slot, sampled `ageS` ago. */
const full = (usedPercentage: number, ageS = 30) => ({ context: { usedPercentage, sampledAt: ago(ageS) } });

function row(qitemId: string, ageS = 120): SeatPoolPendingRow {
  return { qitemId, destination: MODEL_SESSION, waitingSince: ago(ageS), tags: [TAG, "pool-devolvida"] };
}

function state(slots: SeatPoolSlotView[], pending: SeatPoolPendingRow[]): SeatPoolState {
  return {
    model: { rigId: "rig", rigName: "fabrica", podNamespace: "build", memberId: "dev-1", logicalId: "build.dev-1", nodeId: "node-model", sessionName: MODEL_SESSION },
    modelCause: "working", restoring: false, slots, adoptable: [], pending, demand: null, lastScaleAt: null,
  };
}

const dispatches = (intents: SeatPoolIntent[]) => intents.filter((i): i is Extract<SeatPoolIntent, { kind: "dispatch" }> => i.kind === "dispatch");

describe("T-012383: context_limit_percent (spec)", () => {
  it("absent reads as 40 and adds no property; 0 to 100 are kept", () => {
    expect(spec()).not.toHaveProperty("contextLimitPercent");
    expect(SEAT_POOL_CONTEXT_LIMIT_DEFAULT_PERCENT).toBe(40);
    for (const value of [0, 1, 40, 100]) expect(spec({ context_limit_percent: value }).contextLimitPercent).toBe(value);
  });

  it.each([[-1], [101], [40.5], ["40"], [null], [true]])("refuses %j", (value) => {
    const parsed = parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", context_limit_percent: value });
    expect(parsed).toEqual({ problem: { field: "context_limit_percent", message: expect.stringContaining("context_limit_percent must be a whole number from 0") } });
  });

  it("an unknown field names it among the known", () => {
    const parsed = parseSeatPoolSpec({ max: 1, cwd_template: "/w/{slot}", context_limit: 40 });
    expect("problem" in parsed && parsed.problem.message).toContain("context_limit_percent");
  });
});

describe("T-012383: the ping-pong of T-011798 and T-012070 (planner)", () => {
  // s24, s28 and s31 past 40% (the build-dev pool at 19:4xZ of 17/11), both rows back at the model with the pool's tag.
  const fullSlots = () => [slot(24, full(52)), slot(28, full(47)), slot(31, full(61))];
  const pending = () => [row("qitem-T-011798"), row("qitem-T-012070")];

  it("control, the behavior before the limit (context_limit_percent: 0): both rows go to full slots, which give them back", () => {
    const plan = planSeatPool(spec({ context_limit_percent: 0 }), state(fullSlots(), pending()), NOW, 120_000);
    expect(dispatches(plan.intents).map((i) => [i.qitemId, i.slotId])).toEqual([["qitem-T-011798", 24], ["qitem-T-012070", 28]]);
  });

  it("with the default limit no row goes to a full slot; inside idle_grace_s the full slots count as occupied and the pool grows for the rows", () => {
    const recent = () => [slot(24, { ...full(52), idleSince: ago(60) }), slot(28, { ...full(47), idleSince: ago(60) }), slot(31, { ...full(61), idleSince: ago(60) })];
    const plan = planSeatPool(spec(), state(recent(), pending()), NOW, 120_000);
    expect(dispatches(plan.intents)).toEqual([]);
    // 3 full slots occupied + 2 rows no slot takes: 5 seats wanted, 3 there, 2 added.
    expect(plan).toMatchObject({ target: 5, current: 3, reason: "scale_up" });
    expect(plan.intents).toContainEqual({ kind: "add", count: 2 });
    expect(plan.intents.filter((i) => i.kind === "drain")).toEqual([]);
    // Past idle_grace_s (T-017384) they are replaced: they don't hold the target up (2, for the rows), one goes for the
    // surplus, and 2 seats are added for the rows.
    const past = planSeatPool(spec(), state(fullSlots(), pending()), NOW, 120_000);
    expect(dispatches(past.intents)).toEqual([]);
    expect(past).toMatchObject({ target: 2, current: 3, reason: "scale_down" });
    expect(past.intents).toContainEqual({ kind: "add", count: 2 });
    expect(past.intents.filter((i) => i.kind === "drain").map((i) => (i as { slotId: number }).slotId)).toEqual([31]);
  });

  it("a slot under the limit takes the row; the full one beside it does not", () => {
    const plan = planSeatPool(spec(), state([slot(24, full(52)), slot(30, full(12))], [row("qitem-T-011798")]), NOW, 120_000);
    expect(dispatches(plan.intents)).toEqual([{ kind: "dispatch", qitemId: "qitem-T-011798", from: MODEL_SESSION, slotId: 30 }]);
  });

  it("with no row waiting, a full slot inside idle_grace_s is still occupied; past it, it goes (T-017384)", () => {
    const plan = planSeatPool(spec({ min: 0 }), state([slot(24, { ...full(52), idleSince: ago(60) })], []), NOW, 120_000);
    expect(plan).toMatchObject({ target: 1, current: 1 });
    expect(plan.intents).toEqual([]);
    const past = planSeatPool(spec({ min: 0 }), state([slot(24, full(52))], []), NOW, 120_000);
    expect(past).toMatchObject({ target: 0, current: 1, reason: "scale_down" });
    expect(past.intents).toEqual([{ kind: "drain", slotId: 24, idleSince: ago(600) }]);
  });
});

describe("T-017384: a free slot past the limit is drained, not held for ever as occupied (planner)", () => {
  const drains = (intents: SeatPoolIntent[]) => intents.filter((i) => i.kind === "drain").map((i) => (i as { slotId: number }).slotId);
  const working = (n: number) => slot(n, { openRows: 1, idleSince: null, cause: "working" });
  const rows = (count: number) => Array.from({ length: count }, (_, k) => row(`qitem-qa-${k + 1}`));
  // The VM's times (ops/autoscale/pools/*.json: idle_grace_s 600): the slot went quiet 700 s ago, and its last sample is
  // from the turn it ended, 5 s before that, so older than the 10 min a sample is read for.
  const vm = (overrides: Record<string, unknown> = {}) => spec({ idle_grace_s: 600, ...overrides });
  const fullVm = (n: number) => slot(n, { ...full(47, 705), idleSince: ago(700) });

  // The qa pool at 14:21Z of 21/11: 16 live with max 13, 10 of them free past 40%, 6 with a row in progress, 5 rows
  // waiting. Before, the full slots counted as occupied and were never drained (only an idle slot is), so the plan read
  // scale_down with nothing to drain and nothing to add: the rows waited with no slot able to take them.
  it("the qa pool of 21/11 with the VM's times: no row goes to a full slot, and the full slots go, newest first", () => {
    const slots = [1, 2, 3, 4, 5, 6].map(working).concat([7, 8, 9, 10, 11, 12, 13, 14, 15, 16].map(fullVm));
    const plan = planSeatPool(vm({ max: 13 }), state(slots, rows(5)), NOW, 120_000);
    // 6 slots working + 5 rows: 11 wanted; the full slots hold nothing up
    expect(plan).toMatchObject({ current: 16, target: 11, reason: "scale_down" });
    expect(dispatches(plan.intents)).toEqual([]);
    // The 3 over max, then one per row no slot takes: 8 go, so the next plan has room to add 5 slots that take the rows.
    expect(drains(plan.intents)).toEqual([16, 15, 14, 13, 12, 11, 10, 9]);
    // The next plan: 6 working and 2 full, room 5: the 5 seats for the rows are added (the full ones go when over).
    const next = planSeatPool(vm({ max: 13 }), state([1, 2, 3, 4, 5, 6].map(working).concat([7, 8].map(fullVm)), rows(5)), NOW, 120_000);
    expect(next.intents).toContainEqual({ kind: "add", count: 5 });
    expect(drains(next.intents)).toEqual([]);
    // With no row waiting all the full slots go (scale_down), down to min.
    expect(drains(planSeatPool(vm({ max: 13 }), state(slots, []), NOW, 120_000).intents)).toEqual([16, 15, 14, 13, 12, 11, 10, 9, 8, 7]);
    expect(drains(planSeatPool(vm({ max: 13, min: 8 }), state(slots, []), NOW, 120_000).intents)).toEqual([16, 15, 14, 13, 12, 11, 10, 9]);
    // The same with demand_cmd and a fresh desired 0.
    const asked = { ...state(slots, []), demand: { desired: 0, sampledAt: ago(10) } };
    expect(drains(planSeatPool(vm({ max: 13, demand_cmd: { argv: ["d"] } }), asked, NOW, 120_000).intents)).toEqual([16, 15, 14, 13, 12, 11, 10, 9, 8, 7]);
    // A free slot under the limit still goes first for the surplus, as before.
    expect(drains(planSeatPool(vm({ max: 13 }), state([...slots, slot(17, { ...full(10), idleSince: ago(700) })], []), NOW, 120_000).intents))
      .toEqual([17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7]);
    // Inside the cooldown nothing is drained.
    const cooling = { ...state(slots, rows(5)), lastScaleAt: ago(10) };
    expect(drains(planSeatPool(vm({ max: 13, cooldown_s: 60 }), cooling, NOW, 120_000).intents)).toEqual([]);
  });

  it("the rows a full slot is drained for: only beyond the room max leaves and what a starting seat will take, never below min", () => {
    // room for some of the rows (3 full, max 4, 3 rows): 1 added, and 2 full slots go for the other 2
    const partial = planSeatPool(vm({ max: 4 }), state([fullVm(1), fullVm(2), fullVm(3)], rows(3)), NOW, 120_000);
    expect(drains(partial.intents)).toEqual([3, 2]);
    expect(partial.intents).toContainEqual({ kind: "add", count: 1 });
    // a seat still starting is launched, not removed, while full slots cover the surplus (3 full + 1 starting, max 2)
    const starting = slot(4, { phase: "added", nodeId: null, idleSince: null, cause: "stopped" });
    const withStarting = planSeatPool(vm({ max: 2 }), state([fullVm(1), fullVm(2), fullVm(3), starting], []), NOW, 120_000);
    expect(drains(withStarting.intents)).toEqual([3, 2, 1]);
    expect(withStarting.intents).toContainEqual({ kind: "launch", slotId: 4 });
    expect(withStarting.intents).not.toContainEqual({ kind: "remove", slotId: 4 });
    // 5 seats starting for the 5 rows (min 13 holds the pool at its size): no full slot goes for the rows, none is added
    const added = [9, 10, 11, 12, 13].map((n) => slot(n, { phase: "added", nodeId: null, idleSince: null, cause: "stopped" }));
    const up = planSeatPool(vm({ max: 13, min: 13 }), state([1, 2, 3, 4, 5, 6, 7, 8].map(fullVm).concat(added), rows(5)), NOW, 120_000);
    expect(drains(up.intents)).toEqual([]);
    expect(up.intents.filter((i) => i.kind === "add")).toEqual([]);
    // a full slot the demand counts (desired 1: e.g. its tarefa in the ledger), no row waiting: no add and no drain, so
    // nothing goes round it (the release command would refuse its drain); with 2 rows, 2 seats for them, no more
    const counted = { ...state([fullVm(1)], []), demand: { desired: 1, sampledAt: ago(10) } };
    const withDemand = vm({ max: 13, demand_cmd: { argv: ["d"] } });
    expect(planSeatPool(withDemand, counted, NOW, 120_000).intents).toEqual([]);
    const countedRows = { ...state([fullVm(1)], rows(2)), demand: { desired: 3, sampledAt: ago(10) } };
    const twoRows = planSeatPool(withDemand, countedRows, NOW, 120_000);
    expect(twoRows.intents.filter((i) => i.kind === "add")).toEqual([{ kind: "add", count: 2 }]);
    // with demand_cmd the full slot also goes for the rows (B5); its release command holds it while the ledger does
    expect(drains(twoRows.intents)).toEqual([1]);
    // the room is there but the target already counts the full slots (9 working, 2 full, 2 rows, max 13): 2 added
    const stuck = planSeatPool(vm({ max: 13 }), state([1, 2, 3, 4, 5, 6, 7, 8, 9].map(working).concat([10, 11].map(fullVm)), rows(2)), NOW, 120_000);
    expect(stuck.intents.filter((i) => i.kind === "add")).toEqual([{ kind: "add", count: 2 }]);
    expect(drains(stuck.intents)).toEqual([]);
    // with demand_cmd its target alone decides the add (REVISAO B4): 5 working and 1 full slot the demand counts, 3 rows
    // it leaves out (a downstream brake): desired 6 = current, no add; the next plan, desired 7 = current, none either
    const braked = { ...state([1, 2, 3, 4, 5].map(working).concat([fullVm(6)]), rows(3)), demand: { desired: 6, sampledAt: ago(10) } };
    expect(planSeatPool(withDemand, braked, NOW, 120_000).intents.filter((i) => i.kind === "add")).toEqual([]);
    const braked2 = { ...state([1, 2, 3, 4, 5, 7].map(working).concat([fullVm(6)]), rows(3)), demand: { desired: 7, sampledAt: ago(10) } };
    expect(planSeatPool(withDemand, braked2, NOW, 120_000).intents.filter((i) => i.kind === "add")).toEqual([]);
    // without demand_cmd and no row waiting, the full slots fill min until a row waits for a seat (no add and drain
    // round them): min 5, 2 full, 3 added, none drained
    const warm = planSeatPool(vm({ max: 13, min: 5 }), state([fullVm(1), fullVm(2)], []), NOW, 120_000);
    expect(warm.intents.filter((i) => i.kind === "add")).toEqual([{ kind: "add", count: 3 }]);
    expect(drains(warm.intents)).toEqual([]);
    // with demand_cmd, below max, the demand not counting the full slots (REVISAO B5), in 2 plans: first the full slots go,
    // one per row no slot takes (the add is the demand's alone: desired = current, none), then the add for the rows
    const notCounted = (slots: SeatPoolSlotView[], desired: number) => ({ ...state(slots, rows(2)), demand: { desired, sampledAt: ago(10) } });
    const nine = [1, 2, 3, 4, 5, 6, 7, 8, 9].map(working);
    const p12 = planSeatPool(withDemand, notCounted([...nine, fullVm(10), fullVm(11)], 11), NOW, 120_000);
    expect(drains(p12.intents)).toEqual([11, 10]);
    expect(p12.intents.filter((i) => i.kind === "add")).toEqual([]);
    const p12next = planSeatPool(withDemand, notCounted(nine, 11), NOW, 120_000);
    expect(p12next.intents.filter((i) => i.kind === "add")).toEqual([{ kind: "add", count: 2 }]);
    expect(drains(p12next.intents)).toEqual([]);
    // the qa pool at the VM's max 8: 6 working, 1 full, 1 row, desired 7: the full slot goes, then 1 is added
    const qa8 = vm({ max: 8, demand_cmd: { argv: ["d"] } });
    const six = [1, 2, 3, 4, 5, 6].map(working);
    const one = (slots: SeatPoolSlotView[]) => ({ ...state(slots, rows(1)), demand: { desired: 7, sampledAt: ago(10) } });
    expect(drains(planSeatPool(qa8, one([...six, fullVm(7)]), NOW, 120_000).intents)).toEqual([7]);
    expect(planSeatPool(qa8, one(six), NOW, 120_000).intents.filter((i) => i.kind === "add")).toEqual([{ kind: "add", count: 1 }]);
    // every full slot goes while a row waits, not one per row: a newer one the release command holds (its tarefa in the
    // ledger, counted by the demand: desired 7 = 5 working + s12 + 1 row) never stands in front of an older one, s9
    const mixed = { ...state([1, 2, 3, 4, 5].map(working).concat([fullVm(9), fullVm(12)]), rows(1)), demand: { desired: 7, sampledAt: ago(10) } };
    expect(drains(planSeatPool(withDemand, mixed, NOW, 120_000).intents)).toEqual([12, 9]);
    // no row waiting, min 2, max 2: a slot draining and 2 full ones: only the draining one, the pool stays at min
    const draining = slot(1, { phase: "draining" });
    expect(drains(planSeatPool(vm({ min: 2, max: 2 }), state([draining, fullVm(2), fullVm(3)], []), NOW, 120_000).intents)).toEqual([1]);
  });

  it("a full slot that is not free stays: a busy process, inside idle_grace_s, adopted, out of usage or the limit off", () => {
    const busy = spec({ max: 2, dispatch_to_parked: { busy_processes: ["suite-flock"] } });
    const at = (s: SeatPoolSpec, slots: SeatPoolSlotView[], st: Partial<SeatPoolState> = {}) =>
      drains(planSeatPool(s, { ...state(slots, rows(2)), ...st }, NOW, 120_000).intents);
    // the case: 3 full free slots, max 2, 2 rows waiting: all 3 go (1 over max, then 1 per row)
    expect(at(busy, [slot(1, full(50)), slot(2, full(50)), slot(3, full(50))])).toEqual([3, 2, 1]);
    // a suite still runs under s3 (the busy read: T-017384/the real reader below): it is left
    expect(at(busy, [slot(1, full(50)), slot(2, full(50)), slot(3, { ...full(50), busyProcess: "suite-flock" })])).toEqual([2, 1]);
    // idle for less than idle_grace_s (300 s): left
    expect(at(busy, [slot(1, full(50)), slot(2, full(50)), slot(3, { ...full(50), idleSince: ago(60) })])).toEqual([2, 1]);
    // an adopted seat is never drained: it has no reading, so it takes a row, and both full slots go (1 over max, 1 row)
    expect(at(busy, [slot(1, full(50)), slot(2, full(50)), slot(3, { ...full(50), adopted: true })])).toEqual([2, 1]);
    // the pool out of usage: nothing goes for the rows (a new slot could not start), and a full slot is not free
    expect(at(busy, [slot(1, full(50)), slot(2, full(50)), slot(3, full(50))], { modelCause: "usage_limit" })).toEqual([]);
    // context_limit_percent 0: no slot is full, the rows go to the free slots, and the surplus slot is the free one left
    expect(at(spec({ max: 2, context_limit_percent: 0 }), [slot(1, full(50)), slot(2, full(50)), slot(3, full(50))])).toEqual([3]);
    // after a daemon restart (quiet for 30 s, the sample 1 h old): the full slot takes no row (REVISAO S5)
    const restarted = slot(7, { ...full(47, 3600), idleSince: ago(30) });
    expect(dispatches(planSeatPool(spec({ max: 2 }), state([restarted], rows(1)), NOW, 120_000).intents)).toEqual([]);
    // a slot that is not quiet keeps the 10 min of its sample: the stale one of a working slot reads as no reading
    expect(seatPoolContextOver(busy, slot(5, { ...full(80, 3605), openRows: 1, idleSince: null, cause: "working" }), NOW)).toBe(false);
  });
});

describe("T-012383: seatPoolContextOver and seatPoolCanReceive (the reading)", () => {
  const s = spec();
  it.each([
    ["40% exactly", true, full(40)],
    ["39.9%", false, full(39.9)],
    ["sampled 599 s ago", true, full(80, 599)],
    ["sampled 1 h ago, quiet for 30 s (the daemon restarted, or a change of activity with no turn): a quiet slot keeps it", true, { ...full(80, 3600), idleSince: ago(30) }],
    ["sampled 3605 s ago, 5 s before the slot went quiet (its last turn): a quiet slot keeps it", true, { ...full(80, 3605), idleSince: ago(3600) }],
    ["sampled ahead of the clock (as just taken)", true, { context: { usedPercentage: 80, sampledAt: new Date(NOW + 5_000).toISOString() } }],
    ["an unreadable time", false, { context: { usedPercentage: 80, sampledAt: "not a time" } }],
    ["a percentage that is NaN", false, { context: { usedPercentage: Number.NaN, sampledAt: ago(10) } }],
    ["no reading (sidecar absent, unreadable or of a previous occupant)", false, {}],
    ["an adopted seat at 90% (a fixed seat the pool never replaces)", false, { ...full(90), adopted: true }],
  ])("%s: over=%s", (_name, over, overrides) => {
    const view = slot(5, overrides as Partial<SeatPoolSlotView>);
    expect(seatPoolContextOver(s, view, NOW)).toBe(over);
    expect(seatPoolCanReceive(s, view, NOW)).toBe(over ? null : "free");
  });

  it("the limit follows the spec: 50 lets a 45% slot take a row, 0 turns the rule off", () => {
    expect(seatPoolCanReceive(spec({ context_limit_percent: 50 }), slot(5, full(45)), NOW)).toBe("free");
    expect(seatPoolCanReceive(spec({ context_limit_percent: 50 }), slot(5, full(50)), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec({ context_limit_percent: 0 }), slot(5, full(99)), NOW)).toBe("free");
  });

  it("a slot parked on its own wait gets no 2nd row past the limit, and gets it under", () => {
    const parked = (pct: number) => slot(7, {
      ...full(pct), openRows: 1, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
      parked: {
        qitemId: "q-a", blockedOn: "external:ci-1", timerIntervalS: 2700, timerArmedLive: true, parkActor: "build-dev-1-s7@fabrica",
        parkedAt: ago(1800), takenAt: ago(3600),
      } satisfies ParkFacts,
    });
    const s2 = spec({ dispatch_to_parked: {} });
    expect(seatPoolCanReceive(s2, parked(10), NOW)).toBe("parked");
    expect(seatPoolCanReceive(s2, parked(45), NOW)).toBeNull();
  });

  it("seatPoolHeldByContext: only a quiet slot in usage past the limit", () => {
    expect(seatPoolHeldByContext(s, slot(5, full(45)), NOW)).toBe(true);
    expect(seatPoolHeldByContext(s, slot(5, full(30)), NOW)).toBe(false);
    expect(seatPoolHeldByContext(s, slot(5, { ...full(45), openRows: 1, idleSince: null, cause: "working" }), NOW)).toBe(false);
    expect(seatPoolHeldByContext(s, slot(5, { ...full(45), heldMessages: 1 }), NOW)).toBe(false);
    expect(seatPoolHeldByContext(s, slot(5, full(45)), NOW, { usageLimited: true })).toBe(false);
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The executor: the real queue, the reader and the history
// ---------------------------------------------------------------------------------------------------------------
const RIG = "fabrica";
const MODEL = "build-dev-1@fabrica";
const LEAD = "coordenacao-orquestrador-tech@fabrica";
const T0 = Date.parse("2026-11-17T19:40:00.000Z");
const IDLE_SINCE = new Date(T0 - 7_200_000).toISOString();
const iso = (ms: number) => new Date(ms).toISOString();
const QUIET = new Set(["seat_pool_steady", "seat_pool_busy", "not_due"]);
const BASE: Record<string, unknown> = { min: 0, cooldown_s: 0, idle_grace_s: 86400, dispatch_after_s: 60, cwd_template: "/work/{rig}/{slot}", dispatch_tag: TAG };

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
  /** Each seat's context reading, as the sidecar reader gives it (absent: null, no reading). */
  readonly context = new Map<string, { usedPercentage: number | null; sampledAt: string | null }>();
  readonly contextReads: string[] = [];
  demandSlots: Array<Array<{ session: string; receives?: string | null; held?: string }>> = [];
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

  addSeat(logicalId: string): { logicalId: string; session: string; nodeId: string } {
    const node = this.rigRepo.addNode(this.rigId, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: this.podId });
    const session = seatSessionName(logicalId, RIG);
    this.db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`sess-${logicalId}`, node.id, session);
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

  async row(qitemId: string): Promise<string> {
    return (await this.queue.create({ qitemId, sourceSession: LEAD, destinationSession: MODEL, body: `${qitemId}: work`, tags: [TAG], nudge: false } as never)).qitemId;
  }

  get(qitemId: string): { destinationSession: string; state: string } {
    return this.queue.getById(qitemId)! as never;
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

  /** The reader as startup.ts builds it: the plan's and the executor's own read the context the same way. */
  readState(job: { jobId: string; targetSession: string; spec: SeatPoolSpec }, busy?: ReadonlyMap<string, string | null>): SeatPoolState {
    return readSeatPoolState({
      db: this.db,
      slots: this.slots,
      openRows: (sessionName) => this.queue.listOpenObligations(sessionName, 500),
      rowFacts: (sessionName) => this.queue.listPoolSeatRowFacts(sessionName),
      sessionAddresses: (sessionName) => [sessionName],
      heldMessages: (nodeId) => this.outbox.heldCount(nodeId),
      seatStopped: () => false,
      seatState: () => ({ activity: "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: null, changedAt: IDLE_SINCE }) as never,
      restoring: () => false,
      demand: (jobId) => this.executor.demandSample(jobId),
      // as startup.ts: the busy read's answer, when the test gives one (T-017384)
      busyProcess: busy ? (sessionName) => busy.get(sessionName) : undefined,
      contextUsage: (_nodeId, sessionName) => {
        this.contextReads.push(sessionName);
        return this.context.get(sessionName) ?? null;
      },
    }, job);
  }

  state(): SeatPoolState { return this.readState({ jobId: this.jobId, targetSession: MODEL, spec: this.spec() }); }

  effects(): SeatPoolEffects {
    const queueEffects = seatPoolQueueEffects({ queueRepo: this.queue, escalationOwner: () => ({ owner: LEAD, reason: "eval" }) });
    return {
      ...queueEffects,
      addSlot: async () => ({ ok: false, code: "eval_no_add", error: "the test adds no seat" }),
      operationNodeId: () => null,
      launchSlot: async () => ({ ok: false, code: "eval_no_launch", error: "the test launches no seat" }),
      sessionRunning: () => true,
      lifecycle: async (_nodeId, fn) => fn(),
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: "x" }) as never,
      stopSlot: async () => ({ ok: false, code: "eval_no_stop", error: "the test stops no seat" }),
      removeSlot: async () => ({ ok: false, code: "eval_no_remove", error: "the test removes no seat" }),
      runCommand: async (_command, env) => {
        if (env["OPENRIG_SEAT_POOL_SLOTS"] !== undefined) this.demandSlots.push(JSON.parse(env["OPENRIG_SEAT_POOL_SLOTS"]));
        return { exitCode: 0, stdout: JSON.stringify({ desired: 0 }) };
      },
      cleanSlot: async () => ({ ok: true }),
      relaunchSeat: async () => ({ ok: false, code: "eval_no_relaunch", error: "the test relaunches no seat" }),
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

describe("T-012383: the executor (real queue, reader and history)", () => {
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

  it("T-017384: the real reader, with the VM's times, drains a full quiet slot, asks the busy read for it, and leaves it while a suite runs", async () => {
    h.register({ ...BASE, max: 2, idle_grace_s: 600, dispatch_to_parked: { busy_processes: ["suite-flock"] } });
    const s1 = h.poolSeat("build.dev-1-s1");
    // the sample of its last turn, 5 s before it went quiet 2 h ago: past the 10 min a sample is read for
    h.context.set(s1.session, { usedPercentage: 47, sampledAt: iso(Date.parse(IDLE_SINCE) - 5_000) });
    const spec = h.spec();
    const view = h.state().slots.find((s) => s.sessionName === s1.session)!;
    expect(seatPoolHeldByContext(spec, view, h.nowMs)).toBe(true);
    // the daemon's busy read (startup.ts) asks this slot's processes, though it can't take a row
    const asked: string[] = [];
    await readSeatPoolBusyCandidates(spec, h.state().slots, h.nowMs, {
      processes: async () => [], panes: async () => [], processPane: async () => null,
      paneOf: (session) => { asked.push(session); return { paneId: "%1", tmuxSession: session }; },
    });
    expect(asked).toEqual([s1.session]);
    const job = { jobId: h.jobId, targetSession: MODEL, spec };
    const drainsOf = (st: SeatPoolState) => planSeatPool(spec, st, h.nowMs, 120_000).intents.filter((i) => i.kind === "drain");
    expect(drainsOf(h.readState(job, new Map([[s1.session, null]])))).toEqual([{ kind: "drain", slotId: s1.slotId, idleSince: view.idleSince }]);
    expect(drainsOf(h.readState(job, new Map([[s1.session, "suite-flock (pid 4242)"]])))).toEqual([]);
  });

  it("the ping-pong: the rows of T-011798 and T-012070 stay at the model while s24 and s28 are past 40%, and go once s24 is relaunched", async () => {
    h.register({ ...BASE, max: 2 });
    const s24 = h.poolSeat("build.dev-1-s24");
    const s28 = h.poolSeat("build.dev-1-s28");
    h.context.set(s24.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(s28.session, { usedPercentage: 47, sampledAt: iso(T0 - 30_000) });
    h.nowMs = T0 - 2_000;
    const a = await h.row("qitem-T-011798");
    h.nowMs = T0 - 1_000;
    const b = await h.row("qitem-T-012070");
    h.nowMs = T0;
    await h.tickAt(61);
    await h.tickAt(200);
    expect(h.notes("seat_pool_dispatched")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    expect(h.get(b)).toMatchObject({ destinationSession: MODEL, state: "pending" });
    // s24 relaunched clean: its new occupant's sample reads 8%; the next plan hands it the oldest row.
    h.context.set(s24.session, { usedPercentage: 8, sampledAt: iso(T0 + 250_000) });
    await h.tickAt(260);
    expect(h.notes("seat_pool_dispatched")).toEqual([expect.objectContaining({ qitemId: a, to: s24.session })]);
    expect(h.get(b).destinationSession).toBe(MODEL);
  });

  it("control: with context_limit_percent 0 the same full slots get both rows (the dispatches that came back to the model)", async () => {
    h.register({ ...BASE, max: 2, context_limit_percent: 0 });
    const s24 = h.poolSeat("build.dev-1-s24");
    const s28 = h.poolSeat("build.dev-1-s28");
    h.context.set(s24.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(s28.session, { usedPercentage: 47, sampledAt: iso(T0 - 30_000) });
    await h.row("qitem-T-011798");
    await h.row("qitem-T-012070");
    await h.tickAt(61);
    expect(h.notes("seat_pool_dispatched").map((n) => n["to"]).sort()).toEqual([s24.session, s28.session].sort());
    // With the rule off nothing is read.
    expect(h.contextReads).toEqual([]);
  });

  it("the reader: a slot the pool added carries its reading; an adopted seat, no reading or one with no percentage carries none", async () => {
    h.register({ ...BASE, max: 4 });
    const added = h.poolSeat("build.dev-1-s24");
    const adopted = h.poolSeat("build.dev-2", true);
    const none = h.poolSeat("build.dev-1-s25");
    const noPct = h.poolSeat("build.dev-1-s26");
    h.context.set(added.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(adopted.session, { usedPercentage: 90, sampledAt: iso(T0 - 30_000) });
    h.context.set(noPct.session, { usedPercentage: null, sampledAt: iso(T0 - 30_000) });
    const bySession = new Map(h.state().slots.map((s) => [s.sessionName, s]));
    expect(bySession.get(added.session)!.context).toEqual({ usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    expect(bySession.get(adopted.session)).not.toHaveProperty("context");
    expect(bySession.get(none.session)).not.toHaveProperty("context");
    expect(bySession.get(noPct.session)).not.toHaveProperty("context");
    // An adopted seat is never asked for.
    expect(h.contextReads).not.toContain(adopted.session);
  });

  it("recheck: a slot that passed the limit between the plan and the move skips the row with free_context; the row stays at the model", async () => {
    h.register({ ...BASE, max: 1 });
    const s = h.poolSeat("build.dev-1-s24");
    h.context.set(s.session, { usedPercentage: 35, sampledAt: iso(T0 - 30_000) });
    h.nowMs = T0 - 120_000;
    const a = await h.row("qitem-T-011798");
    h.nowMs = T0;
    const intent = dispatches(planSeatPool(h.spec(), h.state(), h.nowMs, 120_000).intents)[0]!;
    expect(intent).toMatchObject({ qitemId: a, slotId: s.slotId });
    h.context.set(s.session, { usedPercentage: 41, sampledAt: iso(T0 - 5_000) });
    await h.submit([intent]);
    expect(h.notes("seat_pool_dispatch_skipped")).toEqual([{
      qitemId: a, from: MODEL, to: s.session, code: "free_context", usedPercentage: 41, sampledAt: iso(T0 - 5_000), limit: 40,
    }]);
    expect(h.notes("seat_pool_dispatched")).toEqual([]);
    expect(h.get(a)).toMatchObject({ destinationSession: MODEL, state: "pending" });
  });

  it("the demand command is told a full quiet slot is held by its context (receives null, held context); a slot under it receives", async () => {
    h.register({ ...BASE, max: 2, demand_cmd: { argv: ["demand"] } });
    const fullSlot = h.poolSeat("build.dev-1-s24");
    const fresh = h.poolSeat("build.dev-1-s25");
    h.context.set(fullSlot.session, { usedPercentage: 52, sampledAt: iso(T0 - 30_000) });
    h.context.set(fresh.session, { usedPercentage: 3, sampledAt: iso(T0 - 30_000) });
    await h.tickAt(1);
    const last = h.demandSlots.at(-1)!;
    expect(last.find((x) => x.session === fullSlot.session)).toMatchObject({ receives: null, held: "context" });
    const freshEntry = last.find((x) => x.session === fresh.session)!;
    expect(freshEntry.receives).toBe("free");
    expect(freshEntry).not.toHaveProperty("held");
  });

  it("T-017384: a full slot the plan drains (past idle_grace_s) is not told held: the demand would hold the target up for it", async () => {
    h.register({ ...BASE, max: 2, idle_grace_s: 600, demand_cmd: { argv: ["demand"] } });
    const fullSlot = h.poolSeat("build.dev-1-s24");
    h.context.set(fullSlot.session, { usedPercentage: 52, sampledAt: iso(Date.parse(IDLE_SINCE) - 5_000) });
    await h.tickAt(1);
    const entry = h.demandSlots.at(-1)!.find((x) => x.session === fullSlot.session)!;
    expect(entry.receives).toBeNull();
    expect(entry).not.toHaveProperty("held");
  });
});

// ---------------------------------------------------------------------------------------------------------------
// The sidecar the daemon reads (ContextUsageStore.readSidecarForNode)
// ---------------------------------------------------------------------------------------------------------------
describe("T-012383: readSidecarForNode, the live occupant's sidecar only", () => {
  let home: string;
  let db: Database.Database;
  const SESSION = "build-dev-1-s24@fabrica";
  const write = (sample: Record<string, unknown>) => {
    mkdirSync(join(home, "state", "context-usage"), { recursive: true });
    writeFileSync(join(home, "state", "context-usage", `${SESSION}.json`), JSON.stringify(sample));
  };
  const sample = (pct: number | null, sampledAt: string, generation?: string) => ({
    context_window: { context_window_size: 1_000_000, used_percentage: pct, remaining_percentage: pct === null ? null : 100 - pct },
    session_name: SESSION, sampled_at: sampledAt, ...(generation ? { occupant_generation: generation } : {}),
  });

  beforeEach(() => {
    home = mkdtempSync(join(tmpdir(), "ctx-pool-"));
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
  });
  afterEach(() => {
    db.close();
    rmSync(home, { recursive: true, force: true });
  });

  it("reads the percentage and the time of the sample", () => {
    write(sample(52, "2026-11-17T19:39:30.000Z"));
    const store = new ContextUsageStore(db, { stateDir: home });
    expect(store.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "known", usedPercentage: 52, sampledAt: "2026-11-17T19:39:30.000Z" });
  });

  it("a sample taken before the live occupant booted (a previous occupant's, under the same name) reads as unknown", () => {
    write(sample(52, "2026-11-17T19:30:00.000Z"));
    const store = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00" });
    expect(store.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "unknown", reason: "stale_generation" });
  });

  it("a sample recording the live occupant's generation is its own, whatever its time; one after the boot is too", () => {
    write(sample(52, "2026-11-17T19:30:00.000Z", "gen-2"));
    const own = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00", resolveOccupantGeneration: () => "gen-2" });
    expect(own.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "known", usedPercentage: 52 });
    write(sample(52, "2026-11-17T19:36:00.000Z"));
    const after = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00" });
    expect(after.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "known", usedPercentage: 52 });
  });

  it("a sample recording another occupant's generation reads as unknown, even sampled after the live one booted", () => {
    write(sample(52, "2026-11-17T19:36:00.000Z", "gen-1"));
    const store = new ContextUsageStore(db, { stateDir: home, resolveOccupantBootAt: () => "2026-11-17 19:35:00", resolveOccupantGeneration: () => "gen-2" });
    expect(store.readSidecarForNode("node-s24", SESSION)).toMatchObject({ availability: "unknown", reason: "stale_generation" });
  });

  it("no sidecar, an unreadable one or one with no percentage reads as unknown", () => {
    const store = new ContextUsageStore(db, { stateDir: home });
    expect(store.readSidecarForNode("node-s24", SESSION).availability).toBe("unknown");
    mkdirSync(join(home, "state", "context-usage"), { recursive: true });
    writeFileSync(join(home, "state", "context-usage", `${SESSION}.json`), "{not json");
    expect(store.readSidecarForNode("node-s24", SESSION).availability).toBe("unknown");
    write(sample(null, "2026-11-17T19:39:30.000Z"));
    expect(store.readSidecarForNode("node-s24", SESSION).availability).toBe("unknown");
  });
});
