// dispatch_to_parked.max_open_rows (T-007036): a pool seat whose open rows are ALL blocked on its own external: waits,
// each with a live timer it armed itself, may receive one row more. Since T-009130 a row blocked on an external: gate
// (a wait on another seat or an outside event, with no CPU of the seat's) no longer counts toward max_open_rows; a row
// blocked on another row, a pending or an in-progress one still counts, and still keeps the seat from receiving, as does
// a blocked one without a live timer.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import {
  parseSeatPoolSpec,
  planSeatPool,
  seatPoolCanReceive,
  type ParkFacts,
  type SeatPoolSlotView,
  type SeatPoolSpec,
} from "../src/domain/policies/seat-pool.js";

const NOW = Date.parse("2026-11-13T02:00:00.000Z");
const ago = (seconds: number) => new Date(NOW - seconds * 1000).toISOString();
const SLOT = "dev-impl-s1@r";

function spec(dispatchToParked: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", dispatch_to_parked: dispatchToParked });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

function park(n: number, over: Partial<ParkFacts> = {}): ParkFacts {
  return {
    qitemId: `a-${n}`, blockedOn: `external:x${n}`, timerIntervalS: 1800, timerArmedLive: true, parkActor: SLOT,
    parkedAt: ago(1200), takenAt: ago(3600), ...over,
  };
}

/** A pool seat at its prompt holding `parks` (all blocked); `openRows` defaults to their count. */
function seat(parks: ParkFacts[], openRows = parks.length): SeatPoolSlotView {
  return {
    slotId: 1, logicalId: "dev.impl-s1", sessionName: SLOT, nodeId: "node-1", phase: "live", adopted: false, nodeExists: true,
    openRows, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
    ...(openRows === 1 && parks.length === 1 ? { parked: parks[0] } : {}),
    ...(parks.length > 1 ? { parkedRows: parks } : {}),
  };
}

describe("dispatch_to_parked.max_open_rows: the spec", () => {
  it("is absent unless given, so {} reads as before (max 1)", () => {
    expect(spec().dispatchToParked).not.toHaveProperty("maxOpenRows");
    expect(spec({ max_open_rows: 4 }).dispatchToParked).toMatchObject({ maxOpenRows: 4 });
  });

  it.each([0, 21, 1.5, "4", null])("refuses %p", (value) => {
    const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/w/{slot}", dispatch_to_parked: { max_open_rows: value } });
    expect("problem" in parsed && parsed.problem.field).toBe("dispatch_to_parked.max_open_rows");
  });
});

describe("dispatch_to_parked.max_open_rows: seatPoolCanReceive", () => {
  it("one open row, blocked with a live timer: receives, as today (default and max 4)", () => {
    expect(seatPoolCanReceive(spec(), seat([park(1)]), NOW)).toBe("parked");
    expect(seatPoolCanReceive(spec({ max_open_rows: 4 }), seat([park(1)]), NOW)).toBe("parked");
  });

  it("three rows, all blocked on external: with a live timer it armed: receives, the external: waits not counted (T-009130)", () => {
    const three = seat([park(1), park(2), park(3)]);
    expect(seatPoolCanReceive(spec({ max_open_rows: 4 }), three, NOW)).toBe("parked");
    expect(seatPoolCanReceive(spec(), three, NOW)).toBe("parked");
    expect(seatPoolCanReceive(spec({ max_open_rows: 2 }), three, NOW)).toBe("parked");
  });

  it("T-009130: a row blocked on another row still counts toward max_open_rows, and keeps the seat from receiving", () => {
    const onRow = (n: number) => park(n, { blockedOn: `qitem-${n}` });
    // 2 rows blocked on other rows with max 1: over the ceiling; with max 3, under it but not the seat's own wait
    expect(seatPoolCanReceive(spec(), seat([park(1), onRow(2), onRow(3)]), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec({ max_open_rows: 3 }), seat([park(1), onRow(2), onRow(3)]), NOW)).toBeNull();
    // an external: wait that is a refused gate (heavy local work) still refuses, counted or not
    expect(seatPoolCanReceive(spec({ max_open_rows: 3, refuse_gates: ["suite-flock"] }),
      seat([park(1), park(2), park(3, { blockedOn: "external:suite-flock-teto-s1" })]), NOW)).toBeNull();
  });

  it("T-009130: three in-progress (or pending) rows: no parks, so the seat does not receive, whatever the max", () => {
    expect(seatPoolCanReceive(spec({ max_open_rows: 3 }), seat([], 3), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec({ max_open_rows: 20 }), seat([], 3), NOW)).toBeNull();
  });

  it("a pending (or in-progress) row among them: no park for it, so the seat does not receive", () => {
    // 3 open rows, only 2 of them parks: the third is pending or in-progress.
    expect(seatPoolCanReceive(spec({ max_open_rows: 4 }), seat([park(1), park(2)], 3), NOW)).toBeNull();
    // A single pending row: no park at all.
    expect(seatPoolCanReceive(spec({ max_open_rows: 4 }), seat([], 1), NOW)).toBeNull();
  });

  it("a blocked row without a live timer, or one armed by another seat, or on a gate that is not external:: refuses", () => {
    const max4 = spec({ max_open_rows: 4 });
    expect(seatPoolCanReceive(max4, seat([park(1), park(2, { timerArmedLive: false })]), NOW)).toBeNull();
    expect(seatPoolCanReceive(max4, seat([park(1), park(2, { timerIntervalS: null })]), NOW)).toBeNull();
    expect(seatPoolCanReceive(max4, seat([park(1), park(2, { parkActor: "other@r" })]), NOW)).toBeNull();
    expect(seatPoolCanReceive(max4, seat([park(1), park(2, { blockedOn: "fc-other-row" })]), NOW)).toBeNull();
    expect(seatPoolCanReceive(max4, seat([park(1), park(2, { timerIntervalS: 7200 })]), NOW)).toBeNull();
    expect(seatPoolCanReceive(max4, seat([park(1), park(2, { takenAt: ago(30_000) })]), NOW)).toBeNull();
    expect(seatPoolCanReceive(spec({ max_open_rows: 4, refuse_gates: ["congelar"] }),
      seat([park(1), park(2, { blockedOn: "external:congelar-eval" })]), NOW)).toBeNull();
  });
});

describe("dispatch_to_parked.max_open_rows: listPoolSeatRowFacts over the real queue", () => {
  const T0 = Date.parse("2026-11-13T00:00:00.000Z");
  let db: Database.Database;
  let queue: QueueRepository;
  const at = (ms: number) => vi.setSystemTime(ms);

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    at(T0);
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
    queue = new QueueRepository(db, new EventBus(db));
  });
  afterEach(() => {
    db.close();
    vi.useRealTimers();
  });

  async function taken(): Promise<string> {
    const row = await queue.create({ sourceSession: "dev-impl@r", destinationSession: SLOT, body: "A", tags: ["tarefa-nova"], nudge: false } as never);
    queue.claim({ qitemId: row.qitemId, destinationSession: SLOT });
    return row.qitemId;
  }
  function parkRow(qitemId: string, n: number): void {
    queue.update({ qitemId, actorSession: SLOT, state: "blocked", blockedOn: `external:x${n}`, wakeAfterSeconds: 1800, transitionNote: "parked" } as never);
  }

  it("three blocked rows with live timers: no single park; the three parks (external: not counted), and the seat receives", async () => {
    const ids = [await taken(), await taken(), await taken()];
    at(T0 + 60_000);
    ids.forEach((id, i) => parkRow(id, i + 1));
    at(T0 + 120_000);
    const one = queue.listPoolSeatRowFacts(SLOT);
    expect(one.parked).toBeNull();
    expect(one.parkedRows).toHaveLength(3); // T-009130: the external: waits don't count toward the default max (1)
    const four = queue.listPoolSeatRowFacts(SLOT, 4);
    expect(four.parked).toBeNull();
    expect(four.parkedRows!.map((p) => p.qitemId).sort()).toEqual([...ids].sort());
    expect(four.parkedRows!.every((p) => p.timerArmedLive && p.parkActor === SLOT)).toBe(true);
    const view: SeatPoolSlotView = {
      slotId: 1, logicalId: "dev.impl-s1", sessionName: SLOT, nodeId: "node-1", phase: "live", adopted: false, nodeExists: true,
      openRows: four.rows.length, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: true,
      parkedRows: four.parkedRows!,
    };
    expect(seatPoolCanReceive(spec({ max_open_rows: 4 }), view, T0 + 120_000)).toBe("parked");
  });

  it("one of them still in-progress: no parks, whatever the max", async () => {
    const ids = [await taken(), await taken()];
    at(T0 + 60_000);
    parkRow(ids[0]!, 1);
    const facts = queue.listPoolSeatRowFacts(SLOT, 4);
    expect(facts.parked).toBeNull();
    expect(facts).not.toHaveProperty("parkedRows");
  });

  it("more rows blocked on other rows than the max: no parks; blocked on external: they don't count (T-009130)", async () => {
    const ids = [await taken(), await taken(), await taken()];
    at(T0 + 60_000);
    ids.forEach((id, i) => parkRow(id, i + 1));
    expect(queue.listPoolSeatRowFacts(SLOT, 2).parkedRows).toHaveLength(3);
    expect(queue.listPoolSeatRowFacts(SLOT).parkedRows).toHaveLength(3);
    const other = [await taken(), await taken()];
    at(T0 + 90_000);
    other.forEach((id) => queue.update({ qitemId: id, actorSession: SLOT, state: "blocked", blockedOn: ids[0], transitionNote: "on a row" } as never));
    expect(queue.listPoolSeatRowFacts(SLOT, 1)).not.toHaveProperty("parkedRows");
    expect(queue.listPoolSeatRowFacts(SLOT, 2).parkedRows).toHaveLength(5);
  });

  it("one blocked row: the single park, as before, with or without a max", async () => {
    const id = await taken();
    at(T0 + 60_000);
    parkRow(id, 1);
    expect(queue.listPoolSeatRowFacts(SLOT).parked?.qitemId).toBe(id);
    const four = queue.listPoolSeatRowFacts(SLOT, 4);
    expect(four.parked?.qitemId).toBe(id);
    expect(four).not.toHaveProperty("parkedRows");
  });
});

describe("T-009130: the dispatch counts no external: wait toward max_open_rows", () => {
  const T0 = Date.parse("2026-11-13T00:00:00.000Z");
  let db: Database.Database;
  let queue: QueueRepository;
  const at = (ms: number) => vi.setSystemTime(ms);
  const S2 = "dev-impl-s2@r";

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    at(T0);
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
    queue = new QueueRepository(db, new EventBus(db));
  });
  afterEach(() => {
    db.close();
    vi.useRealTimers();
  });

  async function taken(dest: string): Promise<string> {
    const row = await queue.create({ sourceSession: "dev-impl@r", destinationSession: dest, body: "A", tags: ["tarefa-nova"], nudge: false } as never);
    queue.claim({ qitemId: row.qitemId, destinationSession: dest });
    return row.qitemId;
  }

  function view(slotId: number, sessionName: string, maxOpenRows: number): SeatPoolSlotView {
    const facts = queue.listPoolSeatRowFacts(sessionName, maxOpenRows);
    return {
      slotId, logicalId: `dev.impl-s${slotId}`, sessionName, nodeId: `node-${slotId}`, phase: "live", adopted: false, nodeExists: true,
      openRows: facts.rows.length, heldMessages: 0, idleSince: null, cause: "working", atPrompt: true, backgroundWork: false,
      ...(facts.parked ? { parked: facts.parked } : {}),
      ...(facts.parkedRows ? { parkedRows: facts.parkedRows } : {}),
    };
  }

  it("a seat with 3 rows blocked on external: receives the waiting row; a seat with 3 in progress does not", async () => {
    const parked = [await taken(SLOT), await taken(SLOT), await taken(SLOT)];
    await taken(S2); await taken(S2); await taken(S2);
    at(T0 + 60_000);
    parked.forEach((id, i) => queue.update({ qitemId: id, actorSession: SLOT, state: "blocked", blockedOn: `external:sre-push-${i}`,
      wakeAfterSeconds: 1800, transitionNote: "parked" } as never));
    at(T0 + 120_000);
    const sp = spec({ max_open_rows: 2 });
    const slots = [view(2, S2, 2), view(1, SLOT, 2)];
    const state = {
      model: { rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model", sessionName: "dev-impl@r" },
      modelCause: "working" as const, restoring: false, slots, adoptable: [], demand: null, lastScaleAt: null,
      pending: [{ qitemId: "q-wait", destination: "dev-impl@r", waitingSince: new Date(T0 - 600_000).toISOString(), tags: [] }],
    };
    const dispatch = planSeatPool(sp, state, T0 + 120_000, 120_000).intents.filter((i) => i.kind === "dispatch");
    expect(dispatch).toEqual([{ kind: "dispatch", qitemId: "q-wait", from: "dev-impl@r", slotId: 1, mode: "parked" }]);
    expect(seatPoolCanReceive(sp, slots[0]!, T0 + 120_000)).toBeNull();
  });
});
