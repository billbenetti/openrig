// Eval F1 (T-004860 / T-004868), frozen by qualidade-qa-6@fabrica.
//
// The pool's `max` counts the adopted seats the same way in the planner (policies/seat-pool.ts) and in the executor
// (seat-pool-executor.ts, allocate): every `add` the plan asks for becomes a seat, the executor never makes a seat the
// planner would not ask for, and an add the pool has no room for is neither a silent no-op nor a spent launch of the
// tick. `max` counts every seat on record: the adopted ones, running or stopped, and the ones the pool added, draining
// included; with more demand than max the pool fills exactly up to max, even when only part of an add fits. The pool's shape is the real one: the nodes of `rig ps --nodes --rig fabrica --json` (captured outside a seat,
// 2026-11-10) and the adapter's spec ops/autoscale/pools/build-dev.vm.json (billverso origin/main 573377be), read from
// the fixtures below, never written by hand.
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import {
  SeatPoolExecutor,
  SeatPoolSlots,
  seatSessionName,
  type SeatPoolEffects,
} from "../src/domain/seat-pool-executor.js";
import {
  parseSeatPoolSpec,
  planSeatPool,
  type SeatPoolIntent,
  type SeatPoolModel,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import { LaunchBudget } from "../src/domain/launch-budget.js";

interface PsNode {
  logicalId: string;
  canonicalSessionName: string;
  rigName: string;
  sessionStatus: string;
  agentActivity?: { state?: string };
}
const fixture = (name: string) => readFileSync(new URL(`./fixtures/seat-pool-max-adopted/${name}`, import.meta.url), "utf8");
const PS = JSON.parse(fixture("rig-ps-nodes-fabrica.json")) as PsNode[];
const POOL = JSON.parse(fixture("build-dev.vm.json")) as { policy: string; context: Record<string, unknown> };

// The adapter's model for the build-dev pool (ops/autoscale/config.json: "modelo": "build-dev-1@fabrica").
const MODEL_LOGICAL_ID = "build.dev-1";
const byId = new Map(PS.map((node) => [node.logicalId, node]));
const modelNode = byId.get(MODEL_LOGICAL_ID);
const SPEC_ADOPT = (POOL.context["adopt"] ?? []) as string[];
/** The spec's adopt entries that exist in the rig: the seats the pool adopts. */
const ADOPTED = SPEC_ADOPT.filter((logicalId) => byId.has(logicalId));
const N = ADOPTED.length;
const SPEC_MAX = POOL.context["max"] as number;
const RIG = modelNode?.rigName ?? "fabrica";
const MODEL: SeatPoolModel = {
  rigId: "rig-fabrica", rigName: RIG, podNamespace: "build", memberId: "dev-1", logicalId: MODEL_LOGICAL_ID,
  nodeId: `node-${MODEL_LOGICAL_ID}`, sessionName: modelNode?.canonicalSessionName ?? "build-dev-1@fabrica",
};
const JOB = "pool-build-dev";
const NOW = Date.parse("2026-11-10T02:00:00.000Z");
const IDLE_SINCE = new Date(NOW - 3_600_000).toISOString();
/** More demand than any max here: the planner asks for every seat it thinks the pool has room for. */
const DEMAND = 100;

/** How the adopted seats are running: as captured, or at night (only the first two of the spec still run). */
type Mix = "captured" | "night";
const MIXES: Mix[] = ["captured", "night"];
const CASES: Array<{ name: string; max: number }> = [
  { name: "adopted > max (the runbook's observation max=3)", max: 3 },
  { name: "adopted = max", max: N },
  { name: "adopted < max (max = adopted + 3)", max: N + 3 },
  { name: "the spec's real max", max: SPEC_MAX },
];

interface Seat { logicalId: string; running: boolean; idle: boolean }

function poolSpec(max: number): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ ...POOL.context, max });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

describe("F1: the pool's max counts adopted seats the same way in the planner and the executor (real build-dev shape)", () => {
  let db: Database.Database;
  let slots: SeatPoolSlots;
  let seats: Map<string, Seat>;
  let jobSpec: SeatPoolSpec;
  let history: Array<{ failed: boolean; reason: string; notes: Record<string, unknown> }>;

  const effects = (): SeatPoolEffects => ({
    addSlot: async ({ memberId, operationId }) => {
      const nodeId = `node-${operationId}`;
      seats.set(nodeId, { logicalId: `build.${memberId}`, running: false, idle: false });
      return { ok: true, nodeId };
    },
    operationNodeId: (operationId) => (seats.has(`node-${operationId}`) ? `node-${operationId}` : null),
    launchSlot: async ({ nodeId }) => {
      Object.assign(seats.get(nodeId)!, { running: true, idle: true });
      return { ok: true };
    },
    sessionRunning: (nodeId) => seats.get(nodeId)?.running === true,
    lifecycle: async (_nodeId, fn) => fn(),
    stopPrecondition: (nodeId) => ({ nodeId, sessionId: `sess-${nodeId}` }),
    stopSlot: async () => ({ ok: true }),
    removeSlot: async ({ nodeId }) => (seats.delete(nodeId) ? { ok: true } : { ok: false, code: "node_not_found", error: "gone" }),
    runCommand: async (command) => ({
      exitCode: 0,
      stdout: command.argv.some((part) => part.endsWith("demanda.py")) ? JSON.stringify({ desired: DEMAND }) : "",
    }),
    cleanSlot: async () => ({ ok: true }),
    openRows: () => [],
    unclaimRow: () => ({ ok: true }),
    reapCount: () => 0,
    moveRow: () => ({ ok: true }),
    relaunchSeat: async () => ({ ok: true }),
    escalationOwner: () => ({ owner: null, reason: "none" }),
    escalate: async () => ({ ok: false, code: "no_escalation_owner", error: "none" }),
  } as SeatPoolEffects);

  const state = (at = NOW): SeatPoolState => ({
    model: MODEL,
    modelCause: "working",
    restoring: false,
    slots: slots.listOpen(JOB).map((slot) => {
      const seat = slot.nodeId ? seats.get(slot.nodeId) : undefined;
      const idle = !!seat && seat.running && seat.idle;
      return {
        slotId: slot.slotId, logicalId: slot.logicalId, sessionName: seatSessionName(slot.logicalId, RIG), nodeId: slot.nodeId,
        phase: slot.phase as "live", adopted: slot.opId === null, nodeExists: !!seat, openRows: 0, heldMessages: 0,
        idleSince: idle ? IDLE_SINCE : null,
        cause: !seat || !seat.running ? "stopped" : idle ? "idle" : "working",
      };
    }),
    adoptable: [],
    pending: [],
    demand: { desired: DEMAND, sampledAt: new Date(at).toISOString() },
    lastScaleAt: slots.lastScaleAt(JOB),
  });

  const makeExecutor = (launchBudget?: LaunchBudget) => new SeatPoolExecutor({
    slots,
    readJob: () => ({ active: true, targetSession: MODEL.sessionName, spec: jobSpec }),
    readState: () => state(),
    outOfUsage: async () => false,
    effects: effects(),
    record: (_jobId, entry) => { history.push(entry); },
    now: () => new Date(NOW),
    launchBudget,
  });
  const run = async (executor: SeatPoolExecutor, intents: SeatPoolIntent[]) => {
    expect(executor.submit(JOB, intents)).toBe(true);
    await executor.settled(JOB);
  };
  const plan = (at = NOW) => planSeatPool(jobSpec, state(at), at, 2 * 60 * 1000);
  /** A clock past the cooldown of the pool's latest add: a plan then holds an add only for want of room. */
  const pastCooldown = () => {
    const at = NOW + (jobSpec.cooldownSeconds + 1) * 1000;
    const last = slots.lastScaleAt(JOB);
    if (last !== null) expect(at - Date.parse(last)).toBeGreaterThan(jobSpec.cooldownSeconds * 1000);
    return at;
  };
  /** Room the pool has after adopting the real seats: max counts every seat on record. */
  const roomOf = (max: number) => Math.max(0, max - N);
  const plannedAdd = (intents: SeatPoolIntent[]) =>
    intents.reduce((sum, intent) => sum + (intent.kind === "add" ? intent.count : 0), 0);
  /** Seats the pool added (not adopted), not retired. */
  const born = () => slots.listOpen(JOB).filter((slot) => slot.opId !== null).length;

  /** The pool after adopting the real seats: each runs as captured, or (night) only the first two of the spec. */
  const setUp = (max: number, mix: Mix) => {
    jobSpec = poolSpec(max);
    ADOPTED.forEach((logicalId, i) => {
      const node = byId.get(logicalId)!;
      const capturedRunning = node.sessionStatus === "running";
      const running = mix === "captured" ? capturedRunning : i < 2 && capturedRunning;
      const nodeId = `node-${logicalId}`;
      seats.set(nodeId, { logicalId, running, idle: node.agentActivity?.state === "idle" });
      slots.adopt({ poolJobId: JOB, logicalId, nodeId, at: new Date(NOW - 7_200_000).toISOString() });
    });
  };

  beforeEach(() => {
    db = createFullTestDb();
    slots = new SeatPoolSlots(db);
    seats = new Map();
    history = [];
  });
  afterEach(() => db.close());

  it("reads the real shape: the build-dev pool's model and more adopted seats than the observation's max=3", () => {
    expect(POOL.policy).toBe("seat-pool");
    expect(modelNode?.canonicalSessionName).toBe("build-dev-1@fabrica");
    expect(SPEC_ADOPT.length).toBeGreaterThan(0);
    expect(N).toBeGreaterThan(3);
    expect(SPEC_MAX).toBeGreaterThan(N);
  });

  for (const mix of MIXES) {
    for (const { name, max } of CASES) {
      it(`${name}, adopted seats ${mix}: every add the plan asks for becomes a seat, and none beyond it`, async () => {
        setUp(max, mix);
        const executor = makeExecutor();
        const first = plan();
        const asked = plannedAdd(first.intents);
        await run(executor, first.intents);
        // The add the planner asked for is not a no-op: each one became a seat.
        expect(born()).toBe(asked);
        // The pool is now at the ceiling both sides count: past the cooldown, the planner asks for no more...
        expect(plannedAdd(plan(pastCooldown()).intents)).toBe(0);
        // ...and the executor, handed a stale add, makes no seat the planner would not ask for.
        await run(executor, [{ kind: "add", count: 2 }]);
        expect(born()).toBe(asked);
        if (max >= N + 3) expect(asked).toBeGreaterThanOrEqual(1);
      });

      it(`${name}, adopted seats ${mix}: max counts every adopted seat, running or stopped`, async () => {
        setUp(max, mix);
        const executor = makeExecutor();
        await run(executor, plan().intents);
        await run(executor, [{ kind: "add", count: 2 }]);
        // With more demand than max, the pool fills exactly up to max, adopted seats included: no seat more, none less.
        expect(born()).toBe(roomOf(max));
        if (max >= N + 3) expect(born()).toBeGreaterThanOrEqual(1);
      });

      it(`${name}, adopted seats ${mix}: an add bigger than the room left makes exactly the room`, async () => {
        setUp(max, mix);
        const executor = makeExecutor();
        const room = roomOf(max);
        if (room >= 1) {
          await run(executor, [{ kind: "add", count: 1 }]);
          expect(born()).toBe(1);
        }
        // One room left, or a few: an add of more seats stops at max, seat by seat.
        await run(executor, [{ kind: "add", count: room + 2 }]);
        expect(born()).toBe(room);
      });
    }
  }

  it("a seat the pool is draining still counts against max", async () => {
    const max = N + 3;
    setUp(max, "night");
    // One seat the pool added earlier is being drained (running, idle): it is still on record.
    const drainingNode = "node-draining";
    seats.set(drainingNode, { logicalId: "build.dev-1-s1", running: true, idle: true });
    slots.insert({ poolJobId: JOB, logicalId: "build.dev-1-s1", nodeId: drainingNode, phase: "draining", intent: "drain",
      opId: "op-draining", at: new Date(NOW - 7_200_000).toISOString() });
    expect(born()).toBe(1);
    const executor = makeExecutor();
    const intents = plan().intents.filter((intent) => intent.kind === "add");
    // The planner asks only for the room the draining seat leaves...
    expect(plannedAdd(intents)).toBe(roomOf(max) - 1);
    await run(executor, intents);
    // ...the executor makes exactly that, and a stale add makes none beyond max.
    expect(born()).toBe(roomOf(max));
    await run(executor, [{ kind: "add", count: 2 }]);
    expect(born()).toBe(roomOf(max));
  });

  it("an add the pool has no room for is recorded in the history and spends none of the tick's launches", async () => {
    setUp(3, "night");
    const budget = new LaunchBudget({
      limits: () => ({ maxInFlight: 1, maxPerTick: 1, minFreeMemoryBytes: 0, maxLoadPerCpu: 100 }),
      sample: () => ({ cpuCount: 32, load1: 0, freeMemoryBytes: 1 }),
    });
    await run(makeExecutor(budget), [{ kind: "add", count: 1 }]);
    expect(born()).toBe(0);
    // Not a silent no-op: the history says the add was refused.
    expect(history.length).toBeGreaterThan(0);
    expect(history.some((entry) => entry.reason === "seat_pool_launched")).toBe(false);
    // The tick's one launch is still there for another pool (or a relaunch) in the same tick.
    expect(budget.currentTick().admitted).toBe(0);
    const room = budget.reserve();
    expect(room.ok).toBe(true);
    if (room.ok) room.release();
  });
});
