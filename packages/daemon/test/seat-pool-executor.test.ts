import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { eventsSchema } from "../src/db/migrations/003_events.js";
import { queueItemsSchema } from "../src/db/migrations/024_queue_items.js";
import { queueTransitionsSchema } from "../src/db/migrations/025_queue_transitions.js";
import { outboxEntriesSchema } from "../src/db/migrations/027_outbox_entries.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { PodRepository } from "../src/domain/pod-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import type { StopPrecondition } from "../src/domain/seat-lifecycle-service.js";
import {
  readSeatPoolState,
  runSeatPoolCommand,
  SEAT_POOL_NEEDS_INPUT_TAG,
  SeatPoolExecutor,
  seatPoolQueueEffects,
  SeatPoolSlots,
  seatSessionName,
  slotMemberFragment,
  type SeatPoolEffectResult,
  type SeatPoolEffects,
  type SeatPoolSlot,
} from "../src/domain/seat-pool-executor.js";
import {
  parseSeatPoolSpec,
  planSeatPool,
  seatPoolOutOfUsage,
  type SeatPoolCommand,
  type SeatPoolModel,
  type SeatPoolSeatCause,
  type SeatPoolSpec,
  type SeatPoolState,
} from "../src/domain/policies/seat-pool.js";
import type { Node } from "../src/domain/types.js";

const JOB = "pool-job";
const MODEL: SeatPoolModel = {
  rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model-node", sessionName: "dev-impl@r",
};
const T0 = Date.parse("2026-11-09T12:00:00.000Z");

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", idle_grace_s: 0, cooldown_s: 0, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

interface FakeNode { logicalId: string; session: "none" | "running" | "stopped"; sessionId: string | null; openRows: number; idle: boolean; held?: number; cause?: SeatPoolSeatCause }
interface FakeRow { destination: string; waitingSince: string; nudged: string[] }

/** The daemon as the executor sees it: nodes, sessions and rows, changed only through the effects. */
class World {
  nodes = new Map<string, FakeNode>();
  operations = new Map<string, string | null>();
  calls: string[] = [];
  env: Array<Record<string, string>> = [];
  restoring = false;
  exit: Record<string, number> = {};
  launchResult: SeatPoolEffectResult = { ok: true };
  stopResult: SeatPoolEffectResult = { ok: true };
  /** Called inside the lease, before the release command: a test can deliver work there. */
  onLease: () => void = () => {};
  launchGate: Promise<void> | null = null;
  commandGate: ((signal: AbortSignal) => Promise<void>) | null = null;
  demandStdout = "";
  /** Pending rows, by id; a moved row changes destination, a claimed one is deleted. */
  rows = new Map<string, FakeRow>();
  modelCause: SeatPoolSeatCause = "working";
  moveResult: SeatPoolEffectResult = { ok: true };
  relaunchResult: SeatPoolEffectResult = { ok: true };
  escalations: string[] = [];
  escalationOwner: string | null = "lead@r";
  private seq = 0;

  effects(): SeatPoolEffects {
    return {
      addSlot: async ({ memberId, cwd, operationId }) => {
        this.calls.push(`add ${memberId} ${cwd}`);
        if (this.operations.has(operationId)) {
          const nodeId = this.operations.get(operationId);
          return nodeId ? { ok: true, nodeId } : { ok: false, code: "add_not_completed", error: "no node" };
        }
        const nodeId = `node-${++this.seq}`;
        this.operations.set(operationId, nodeId);
        this.nodes.set(nodeId, { logicalId: `dev.${memberId}`, session: "none", sessionId: null, openRows: 0, idle: false });
        return { ok: true, nodeId };
      },
      operationNodeId: (operationId) => this.operations.get(operationId) ?? null,
      launchSlot: async ({ nodeId }) => {
        this.calls.push(`launch ${this.nodes.get(nodeId)!.logicalId}`);
        if (this.launchGate) await this.launchGate;
        if (!this.launchResult.ok) return this.launchResult;
        Object.assign(this.nodes.get(nodeId)!, { session: "running", sessionId: `sess-${++this.seq}`, idle: true });
        return { ok: true };
      },
      sessionRunning: (nodeId) => this.nodes.get(nodeId)?.session === "running",
      lifecycle: async (nodeId, fn) => {
        this.calls.push(`lease ${this.nodes.get(nodeId)!.logicalId}`);
        this.onLease();
        try { return await fn(); } finally { this.calls.push("lease released"); }
      },
      stopPrecondition: (nodeId) => ({ nodeId, sessionId: this.nodes.get(nodeId)?.sessionId ?? null }),
      stopSlot: async ({ sessionName, expected }) => {
        this.calls.push(`stop ${sessionName} ${JSON.stringify(expected)}`);
        if (!this.stopResult.ok) return this.stopResult;
        this.nodes.get(expected.nodeId)!.session = "stopped";
        return { ok: true };
      },
      removeSlot: async ({ nodeId, expected }) => {
        this.calls.push(`remove ${this.nodes.get(nodeId)?.logicalId ?? nodeId} ${JSON.stringify(expected)}`);
        if (!this.nodes.delete(nodeId)) return { ok: false, code: "node_not_found", error: "gone" };
        return { ok: true };
      },
      runCommand: async (command: SeatPoolCommand, env, signal) => {
        const name = command.argv[0]!;
        this.calls.push(`run ${name}`);
        this.env.push(env);
        if (this.commandGate) await this.commandGate(signal);
        if (signal.aborted) return { exitCode: null, stdout: "", timedOut: false, error: "aborted" };
        return { exitCode: this.exit[name] ?? 0, stdout: name === "demand" ? this.demandStdout : "", timedOut: false };
      },
      moveRow: ({ qitemId, from, to }) => {
        this.calls.push(`move ${qitemId} ${from} -> ${to}`);
        if (!this.moveResult.ok) return this.moveResult;
        // As the reroute does: the row now waits at its new seat from this moment.
        Object.assign(this.rows.get(qitemId)!, { destination: to, waitingSince: new Date(T0).toISOString() });
        const node = [...this.nodes.values()].find((n) => seatSessionName(n.logicalId, "r") === to);
        if (node) node.openRows++;
        return { ok: true };
      },
      relaunchSeat: async ({ logicalId }) => {
        this.calls.push(`relaunch ${logicalId}`);
        if (!this.relaunchResult.ok) return this.relaunchResult;
        if (logicalId === MODEL.logicalId) this.modelCause = "working";
        return { ok: true };
      },
      nudgeRow: async ({ qitemId, seat }) => {
        this.calls.push(`nudge ${qitemId} ${seat}`);
        this.rows.get(qitemId)!.nudged.push(seat);
        return { ok: true, result: "verified" };
      },
      escalate: async ({ seat, qitemIds }) => {
        this.calls.push(`escalate ${seat} ${qitemIds.join(",")}`);
        if (!this.escalationOwner) return { ok: false, code: "no_escalation_owner", error: "no escalation destination declared" };
        const existing = this.escalations.includes(seat);
        if (!existing) this.escalations.push(seat);
        return { ok: true, qitemId: `esc-${seat}`, destination: this.escalationOwner, existing };
      },
    };
  }

  state(slots: SeatPoolSlots, executor: { demandSample(jobId: string): { desired: number; sampledAt: string } | null }, jobSpec: SeatPoolSpec): SeatPoolState {
    return {
      model: MODEL,
      restoring: this.restoring,
      modelCause: this.modelCause,
      slots: slots.listOpen(JOB).map((s: SeatPoolSlot) => {
        const node = s.nodeId ? this.nodes.get(s.nodeId) : undefined;
        const idle = !!node && node.session === "running" && node.idle;
        return {
          slotId: s.slotId, logicalId: s.logicalId, sessionName: seatSessionName(s.logicalId, "r"), nodeId: s.nodeId, phase: s.phase as "live",
          adopted: s.opId === null, nodeExists: !!node, openRows: node?.openRows ?? 0, heldMessages: node?.held ?? 0,
          idleSince: idle ? new Date(T0 - 3_600_000).toISOString() : null,
          cause: node?.cause ?? (node?.session !== "running" ? "stopped" : idle ? "idle" : "working"),
        };
      }),
      adoptable: [],
      pending: [...this.rows].map(([qitemId, row]) => ({ qitemId, destination: row.destination, waitingSince: row.waitingSince, nudged: row.nudged.includes(row.destination) })),
      demand: jobSpec.demandCmd ? executor.demandSample(JOB) : null,
      lastScaleAt: slots.lastScaleAt(JOB),
    };
  }
}

describe("seat-pool executor", () => {
  let db: Database.Database;
  let slots: SeatPoolSlots;
  let world: World;
  let jobSpec: SeatPoolSpec;
  let active: boolean;
  let history: Array<{ failed: boolean; reason: string; notes: Record<string, unknown> }>;

  const makeExecutor = (effectTimeoutMs?: number): SeatPoolExecutor => {
    const executor: SeatPoolExecutor = new SeatPoolExecutor({
      slots,
      readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
      readState: () => world.state(slots, executor, jobSpec),
      outOfUsage: async () => seatPoolOutOfUsage(world.state(slots, executor, jobSpec)),
      effects: world.effects(),
      record: (_jobId, entry) => { history.push(entry); },
      now: () => new Date(T0),
      effectTimeoutMs,
    });
    return executor;
  };
  const run = async (executor: SeatPoolExecutor, intents: Parameters<SeatPoolExecutor["submit"]>[1]) => {
    expect(executor.submit(JOB, intents)).toBe(true);
    await executor.settled(JOB);
  };
  const live = () => slots.listOpen(JOB).filter((s) => s.phase === "live").map((s) => s.logicalId);
  const reasons = () => history.map((h) => h.reason);

  beforeEach(() => {
    db = createFullTestDb();
    slots = new SeatPoolSlots(db);
    world = new World();
    jobSpec = spec({ prepare_cmd: { argv: ["prepare"] } });
    active = true;
    history = [];
  });
  afterEach(() => db.close());

  it("adds, prepares and launches each seat, in that order, with its own name and working directory", async () => {
    await run(makeExecutor(), [{ kind: "add", count: 2 }]);
    expect(live()).toEqual(["dev.impl-s1", "dev.impl-s2"]);
    expect(world.calls).toEqual([
      "add impl-s1 /work/r/impl-s1", "run prepare", "launch dev.impl-s1",
      "add impl-s2 /work/r/impl-s2", "run prepare", "launch dev.impl-s2",
    ]);
    expect(world.env[0]).toEqual({
      OPENRIG_SEAT_POOL_JOB: JOB, OPENRIG_SEAT_POOL_MODEL: "dev-impl@r",
      OPENRIG_SEAT_POOL_SLOT: "dev-impl-s1@r", OPENRIG_SEAT_POOL_SLOT_CWD: "/work/r/impl-s1",
    });
    expect(slots.listOpen(JOB).every((s) => s.intent === null && s.opId !== null)).toBe(true);
    expect(reasons()).toEqual(["seat_pool_launched", "seat_pool_launched"]);
  });

  it("never gives a removed seat's name to a new one", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 2 }]);
    const second = slots.listOpen(JOB)[1]!;
    await run(executor, [{ kind: "drain", slotId: second.slotId }]);
    await run(executor, [{ kind: "add", count: 1 }]);
    expect(live()).toEqual(["dev.impl-s1", "dev.impl-s3"]);
  });

  it("reads the spec again before each add, so a lowered max or drain stops the adds", async () => {
    let adds = 0;
    const effects = world.effects();
    const executor = new SeatPoolExecutor({
      slots, readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
      readState: () => world.state(slots, executor, jobSpec), record: (_j, e) => { history.push(e); }, now: () => new Date(T0),
      effects: { ...effects, addSlot: async (input) => { if (++adds === 1) jobSpec = spec({ max: 1 }); return effects.addSlot(input); } },
    });
    await run(executor, [{ kind: "add", count: 3 }]);
    expect(live()).toEqual(["dev.impl-s1"]);
  });

  it("removes a seat whose prepare command fails, without launching it", async () => {
    world.exit["prepare"] = 3;
    await run(makeExecutor(), [{ kind: "add", count: 1 }]);
    expect(world.calls).toEqual(["add impl-s1 /work/r/impl-s1", "run prepare", 'remove dev.impl-s1 {"nodeId":"node-1","sessionId":null}']);
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(reasons()).toEqual(["seat_pool_prepare_failed", "seat_pool_removed"]);
  });

  it("removes a seat whose launch fails", async () => {
    world.launchResult = { ok: false, code: "startup_failed", error: "harness exited" };
    await run(makeExecutor(), [{ kind: "add", count: 1 }]);
    expect(world.nodes.size).toBe(0);
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(history[0]).toMatchObject({ failed: true, reason: "seat_pool_launch_failed", notes: { code: "startup_failed" } });
  });

  it("after a restart, recognizes a launch that ran but was not recorded, by its running session", async () => {
    const slot = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-9", phase: "added", intent: "launch", opId: "op-9", at: new Date(T0).toISOString() });
    world.nodes.set("node-9", { logicalId: "dev.impl-s1", session: "running", sessionId: "sess-9", openRows: 0, idle: true });
    await run(makeExecutor(), [{ kind: "launch", slotId: slot.slotId }]);
    expect(world.calls).toEqual([]);
    expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null });
    expect(history).toEqual([{ failed: false, reason: "seat_pool_launched", notes: { slot: "dev.impl-s1", nodeId: "node-9", interrupted: "launch" } }]);
  });

  it("after a restart, finishes an interrupted add through its Operation-Id instead of adding again", async () => {
    world.operations.set("op-1", "node-7");
    world.nodes.set("node-7", { logicalId: "dev.impl-s1", session: "none", sessionId: null, openRows: 0, idle: false });
    const created = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: null, phase: "adding", intent: "add", opId: "op-1", at: new Date(T0).toISOString() });
    world.operations.set("op-2", null);
    const lost = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: null, phase: "adding", intent: "add", opId: "op-2", at: new Date(T0).toISOString() });
    await run(makeExecutor(), [{ kind: "launch", slotId: created.slotId }, { kind: "launch", slotId: lost.slotId }]);
    expect(slots.get(created.slotId)).toMatchObject({ phase: "live", nodeId: "node-7" });
    expect(world.nodes.size).toBe(1);
    expect(slots.get(lost.slotId)).toMatchObject({ phase: "retired" });
    expect(history.find((h) => h.reason === "seat_pool_add_failed")).toMatchObject({ notes: { slot: "dev.impl-s2", code: "add_not_completed", interrupted: "add" } });
  });

  it("removes an unwanted seat whose add was interrupted, resolving its node first", async () => {
    world.operations.set("op-1", "node-7");
    world.nodes.set("node-7", { logicalId: "dev.impl-s1", session: "none", sessionId: null, openRows: 0, idle: false });
    const slot = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: null, phase: "adding", intent: "add", opId: "op-1", at: new Date(T0).toISOString() });
    await run(makeExecutor(), [{ kind: "remove", slotId: slot.slotId }]);
    expect(world.calls).toEqual(['remove dev.impl-s1 {"nodeId":"node-7","sessionId":null}']);
    expect(slots.get(slot.slotId)).toMatchObject({ phase: "retired", nodeId: "node-7" });
  });

  it("drains a free seat under one lease: release, checked stop, then removal with the same expected", async () => {
    jobSpec = spec({ release_cmd: { argv: ["release"] } });
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    world.calls = [];
    const [slot] = slots.listOpen(JOB);
    await run(executor, [{ kind: "drain", slotId: slot!.slotId }]);
    const expected: StopPrecondition = { nodeId: "node-1", sessionId: "sess-2" };
    expect(world.calls).toEqual([
      "lease dev.impl-s1", "run release", `stop dev-impl-s1@r ${JSON.stringify(expected)}`, `remove dev.impl-s1 ${JSON.stringify(expected)}`, "lease released",
    ]);
    expect(world.env.at(-1)).toMatchObject({ OPENRIG_SEAT_POOL_SLOT: "dev-impl-s1@r" });
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(reasons().at(-1)).toBe("seat_pool_removed");
  });

  it("returns a seat to the pool, untouched, when its release command refuses or work arrives in the lease", async () => {
    jobSpec = spec({ release_cmd: { argv: ["release"] } });
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 2 }]);
    const [first, second] = slots.listOpen(JOB);
    world.calls = [];
    world.exit["release"] = 1;
    await run(executor, [{ kind: "drain", slotId: first!.slotId }]);
    world.exit["release"] = 0;
    world.onLease = () => { world.nodes.get(second!.nodeId!)!.openRows = 1; };
    await run(executor, [{ kind: "drain", slotId: second!.slotId }]);
    expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("remove"))).toEqual([]);
    expect(live()).toEqual(["dev.impl-s1", "dev.impl-s2"]);
    expect(history.slice(-2)).toEqual([
      { failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s1", why: "release_refused", exitCode: 1, error: null } },
      { failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s2", why: "work_arrived" } },
    ]);
  });

  it("leaves a seat that was relaunched since the read, and never removes it", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    world.stopResult = { ok: false, code: "selection_changed", error: "the seat changed" };
    world.calls = [];
    await run(executor, [{ kind: "drain", slotId: slots.listOpen(JOB)[0]!.slotId }]);
    expect(world.calls.some((c) => c.startsWith("remove"))).toBe(false);
    expect(live()).toEqual(["dev.impl-s1"]);
    expect(history.at(-1)).toMatchObject({ failed: true, reason: "seat_pool_stop_failed", notes: { code: "selection_changed" } });
  });

  it("does not drain a seat that took work after the plan", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    const [slot] = slots.listOpen(JOB);
    world.nodes.get(slot!.nodeId!)!.openRows = 1;
    world.calls = [];
    await run(executor, [{ kind: "drain", slotId: slot!.slotId }]);
    expect(world.calls).toEqual([]);
    expect(slots.get(slot!.slotId)).toMatchObject({ phase: "live" });
  });

  it("does not drain a seat holding a message for it, with no open row", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 2 }]);
    const [first, second] = slots.listOpen(JOB);
    world.nodes.get(first!.nodeId!)!.held = 1;
    world.onLease = () => { world.nodes.get(second!.nodeId!)!.held = 1; };
    world.calls = [];
    await run(executor, [{ kind: "drain", slotId: first!.slotId }, { kind: "drain", slotId: second!.slotId }]);
    expect(world.calls).toEqual(["lease dev.impl-s2", "lease released"]);
    expect(live()).toEqual(["dev.impl-s1", "dev.impl-s2"]);
    expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s2", why: "work_arrived" } });
  });

  it("reads the seat again after its release command, and keeps it when work arrived meanwhile", async () => {
    jobSpec = spec({ release_cmd: { argv: ["release"] } });
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    const [slot] = slots.listOpen(JOB);
    // The lease holds deliveries back, not queue writes: a row is created while the command runs.
    world.commandGate = async () => { world.nodes.get(slot!.nodeId!)!.openRows = 1; };
    world.calls = [];
    await run(executor, [{ kind: "drain", slotId: slot!.slotId }]);
    expect(world.calls).toEqual(["lease dev.impl-s1", "run release", "lease released"]);
    expect(slots.get(slot!.slotId)).toMatchObject({ phase: "live", intent: null });
    expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s1", why: "work_arrived" } });
  });

  it("removes instead of launching a seat whose pool was set to drain while its prepare command ran", async () => {
    world.commandGate = async () => { jobSpec = spec({ drain: true, prepare_cmd: { argv: ["prepare"] } }); };
    await run(makeExecutor(), [{ kind: "add", count: 1 }]);
    expect(world.calls).toEqual(["add impl-s1 /work/r/impl-s1", "run prepare", 'remove dev.impl-s1 {"nodeId":"node-1","sessionId":null}']);
    expect(world.nodes.size).toBe(0);
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(reasons()).toEqual(["seat_pool_removed"]);
  });

  it("adds, launches and relaunches nothing while a seat of the pool is out of usage, read just before", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    const [limited] = slots.listOpen(JOB);
    world.nodes.get(limited!.nodeId!)!.cause = "usage_limit";
    world.nodes.set("node-9", { logicalId: "dev.impl-s9", session: "none", sessionId: null, openRows: 0, idle: false });
    const added = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s9", nodeId: "node-9", phase: "added", intent: null, opId: "op-9", at: new Date(T0).toISOString() });
    world.modelCause = "stopped";
    world.calls = [];
    history = [];
    await run(executor, [
      { kind: "launch", slotId: added.slotId },
      { kind: "relaunch", seat: MODEL.sessionName, nodeId: MODEL.nodeId },
      { kind: "add", count: 1 },
    ]);
    expect(world.calls).toEqual([]);
    expect(slots.listOpen(JOB)).toMatchObject([{ phase: "live" }, { slotId: added.slotId, phase: "added", intent: null }]);
    expect(history).toEqual([
      { failed: false, reason: "seat_pool_held", notes: { reason: "usage_limit", slot: "dev.impl-s9" } },
      { failed: false, reason: "seat_pool_held", notes: { reason: "usage_limit", seat: "dev-impl@r" } },
    ]);
  });

  it("holds a seat whose pool ran out of usage while its prepare command ran, and launches it once the limit lifts", async () => {
    world.commandGate = async () => { world.modelCause = "usage_limit"; };
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 2 }]);
    expect(world.calls).toEqual(["add impl-s1 /work/r/impl-s1", "run prepare"]);
    expect(slots.listOpen(JOB)).toMatchObject([{ logicalId: "dev.impl-s1", phase: "added", intent: null }]);
    expect(history).toEqual([{ failed: false, reason: "seat_pool_held", notes: { reason: "usage_limit", slot: "dev.impl-s1" } }]);
    // The next plan under the limit leaves it as it is.
    expect(planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents).toEqual([]);
    world.commandGate = null;
    world.modelCause = "working";
    world.calls = [];
    await run(executor, planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents);
    expect(world.calls).toEqual(["run prepare", "launch dev.impl-s1"]);
    expect(live()).toEqual(["dev.impl-s1"]);
  });

  it("drain turned on empties the pool through the planner without killing work", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 3 }]);
    const [busy, free, unstarted] = slots.listOpen(JOB);
    world.nodes.get(busy!.nodeId!)!.openRows = 2;
    Object.assign(world.nodes.get(unstarted!.nodeId!)!, { session: "none", idle: false });
    slots.update(unstarted!.slotId, { phase: "added" });
    jobSpec = spec({ drain: true });
    world.calls = [];
    const plan = planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000);
    await run(executor, plan.intents);
    expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("remove"))).toEqual([
      `stop dev-impl-s2@r {"nodeId":"${free!.nodeId}","sessionId":"sess-4"}`,
      `remove dev.impl-s2 {"nodeId":"${free!.nodeId}","sessionId":"sess-4"}`,
      `remove dev.impl-s3 {"nodeId":"${unstarted!.nodeId}","sessionId":"sess-6"}`,
    ]);
    expect(slots.listOpen(JOB).map((s) => s.logicalId)).toEqual(["dev.impl-s1"]);
  });

  it("does nothing while the rig is restored, or once the job is no longer active", async () => {
    world.restoring = true;
    await run(makeExecutor(), [{ kind: "add", count: 1 }]);
    world.restoring = false;
    active = false;
    await run(makeExecutor(), [{ kind: "add", count: 1 }]);
    expect(world.calls).toEqual([]);
    expect(slots.listOpen(JOB)).toEqual([]);
  });

  it("owns a pool for the whole run: a second plan is refused until the first settles", async () => {
    let release!: () => void;
    world.launchGate = new Promise((resolve) => { release = resolve; });
    const executor = makeExecutor();
    expect(executor.submit(JOB, [{ kind: "add", count: 1 }])).toBe(true);
    expect(executor.submit(JOB, [{ kind: "add", count: 1 }])).toBe(false);
    release();
    await executor.settled(JOB);
    expect(executor.submit(JOB, [])).toBe(true);
    await executor.settled(JOB);
    expect(live()).toEqual(["dev.impl-s1"]);
  });

  it("stops waiting for an effect past its timeout, but keeps the pool until the effect settles", async () => {
    let release!: () => void;
    world.launchGate = new Promise((resolve) => { release = resolve; });
    const executor = makeExecutor(20);
    expect(executor.submit(JOB, [{ kind: "add", count: 2 }])).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(history).toEqual([{ failed: true, reason: "seat_pool_launch_failed", notes: { slot: "dev.impl-s1", code: "effect_timeout" } }]);
    expect(executor.submit(JOB, [])).toBe(false);
    expect(slots.listOpen(JOB)).toMatchObject([{ phase: "added", intent: "launch" }]);
    release();
    await executor.settled(JOB);
    expect(executor.submit(JOB, [])).toBe(true);
    await executor.settled(JOB);
    // The second add never started; the interrupted launch is the next plan's to finish.
    expect(world.calls.filter((c) => c.startsWith("add"))).toHaveLength(1);
  });

  it("stop cancels a run: the running command is aborted and nothing further starts", async () => {
    world.commandGate = (signal) => new Promise((resolve) => signal.addEventListener("abort", () => resolve()));
    const executor = makeExecutor();
    expect(executor.submit(JOB, [{ kind: "add", count: 2 }])).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 10));
    await executor.stop();
    expect(world.calls).toEqual(["add impl-s1 /work/r/impl-s1", "run prepare"]);
    expect(slots.listOpen(JOB)).toMatchObject([{ phase: "added", intent: "prepare" }]);
    expect(executor.submit(JOB, [])).toBe(false);
  });

  describe("dispatch and wake", () => {
    const waitingRow = (qitemId: string, destination = MODEL.sessionName) =>
      world.rows.set(qitemId, { destination, waitingSince: new Date(T0 - 600_000).toISOString(), nudged: [] });

    it("moves a waiting row to a seat that is still free, and leaves it when the seat took work since the plan", async () => {
      const executor = makeExecutor();
      await run(executor, [{ kind: "add", count: 2 }]);
      const [first, second] = slots.listOpen(JOB);
      waitingRow("q-1");
      waitingRow("q-2", "dev-impl-s9@r");
      world.nodes.get(second!.nodeId!)!.openRows = 1;
      world.calls = [];
      await run(executor, [
        { kind: "dispatch", qitemId: "q-1", from: MODEL.sessionName, slotId: first!.slotId },
        { kind: "dispatch", qitemId: "q-2", from: "dev-impl-s9@r", slotId: second!.slotId },
      ]);
      expect(world.calls).toEqual(["move q-1 dev-impl@r -> dev-impl-s1@r"]);
      expect(world.rows.get("q-2")!.destination).toBe("dev-impl-s9@r");
      expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_dispatched", notes: { qitemId: "q-1", from: "dev-impl@r", to: "dev-impl-s1@r" } });
    });

    it("records a row that moved on before the reroute as skipped, and any other refusal as failed", async () => {
      const executor = makeExecutor();
      await run(executor, [{ kind: "add", count: 1 }]);
      const [slot] = slots.listOpen(JOB);
      waitingRow("q-1");
      world.moveResult = { ok: false, code: "fallback_state_mismatch", error: "qitem q-1 is in-progress, not pending" };
      await run(executor, [{ kind: "dispatch", qitemId: "q-1", from: MODEL.sessionName, slotId: slot!.slotId }]);
      world.moveResult = { ok: false, code: "wake_intent_store_unavailable", error: "no store" };
      await run(executor, [{ kind: "dispatch", qitemId: "q-1", from: MODEL.sessionName, slotId: slot!.slotId }]);
      expect(history.slice(-2)).toEqual([
        { failed: false, reason: "seat_pool_dispatch_skipped", notes: { qitemId: "q-1", from: "dev-impl@r", to: "dev-impl-s1@r", code: "fallback_state_mismatch" } },
        { failed: true, reason: "seat_pool_dispatch_failed", notes: { qitemId: "q-1", from: "dev-impl@r", to: "dev-impl-s1@r", code: "wake_intent_store_unavailable", error: "no store" } },
      ]);
      expect(world.rows.get("q-1")!.destination).toBe(MODEL.sessionName);
    });

    it("relaunches a seat only while it is still stopped, and records a launch that fails", async () => {
      const executor = makeExecutor();
      await run(executor, [{ kind: "add", count: 1 }]);
      const [slot] = slots.listOpen(JOB);
      world.nodes.get(slot!.nodeId!)!.session = "stopped";
      world.modelCause = "stopped";
      world.relaunchResult = { ok: false, code: "awaiting-decision", error: "the seat came up awaiting-decision" };
      world.calls = [];
      await run(executor, [
        { kind: "relaunch", seat: MODEL.sessionName, nodeId: MODEL.nodeId },
        { kind: "relaunch", seat: "dev-impl-s1@r", nodeId: slot!.nodeId! },
      ]);
      world.relaunchResult = { ok: true };
      world.modelCause = "working";
      world.nodes.get(slot!.nodeId!)!.session = "running";
      await run(executor, [
        { kind: "relaunch", seat: MODEL.sessionName, nodeId: MODEL.nodeId },
        { kind: "relaunch", seat: "dev-impl-s1@r", nodeId: slot!.nodeId! },
      ]);
      expect(world.calls).toEqual(["relaunch dev.impl", "relaunch dev.impl-s1"]);
      expect(history.slice(-2)).toEqual([
        { failed: true, reason: "seat_pool_relaunch_failed", notes: { seat: "dev-impl@r", code: "awaiting-decision", error: "the seat came up awaiting-decision" } },
        { failed: true, reason: "seat_pool_relaunch_failed", notes: { seat: "dev-impl-s1@r", code: "awaiting-decision", error: "the seat came up awaiting-decision" } },
      ]);
    });

    it("nudges an idle seat about a waiting row once, and not a seat that is no longer idle", async () => {
      const executor = makeExecutor();
      waitingRow("q-1");
      waitingRow("q-2");
      world.modelCause = "idle";
      await run(executor, [{ kind: "nudge", seat: MODEL.sessionName, qitemId: "q-1" }]);
      await run(executor, [{ kind: "nudge", seat: MODEL.sessionName, qitemId: "q-1" }]);
      world.modelCause = "working";
      await run(executor, [{ kind: "nudge", seat: MODEL.sessionName, qitemId: "q-2" }]);
      expect(world.calls).toEqual(["nudge q-1 dev-impl@r"]);
      expect(history).toEqual([{ failed: false, reason: "seat_pool_nudged", notes: { seat: "dev-impl@r", qitemId: "q-1", result: "verified" } }]);
    });

    it("escalates a seat that needs input once while the escalation is open, and records a seat with nowhere to escalate", async () => {
      const executor = makeExecutor();
      waitingRow("q-1");
      world.modelCause = "needs_input";
      const escalate = { kind: "escalate" as const, seat: MODEL.sessionName, qitemIds: ["q-1"] };
      await run(executor, [escalate]);
      await run(executor, [escalate]);
      world.escalations = [];
      world.escalationOwner = null;
      await run(executor, [escalate]);
      world.modelCause = "idle";
      await run(executor, [escalate]);
      expect(world.calls).toEqual(["escalate dev-impl@r q-1", "escalate dev-impl@r q-1", "escalate dev-impl@r q-1"]);
      expect(history).toEqual([
        { failed: false, reason: "seat_pool_escalated", notes: { seat: "dev-impl@r", to: "lead@r", qitemId: "esc-dev-impl@r", rows: ["q-1"] } },
        { failed: true, reason: "seat_pool_escalation_failed", notes: { seat: "dev-impl@r", code: "no_escalation_owner", error: "no escalation destination declared" } },
      ]);
    });

    it("moves and wakes nothing once the pool is set to drain, before the run or after one of its effects", async () => {
      const effects = world.effects();
      const executor: SeatPoolExecutor = new SeatPoolExecutor({
        slots, readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
        readState: () => world.state(slots, executor, jobSpec), record: (_j, e) => { history.push(e); }, now: () => new Date(T0),
        effects: {
          ...effects,
          // Drain is set while the relaunch runs, and the model comes up at a prompt that needs input.
          relaunchSeat: async (input) => {
            const launched = await effects.relaunchSeat(input);
            jobSpec = spec({ drain: true });
            world.modelCause = "needs_input";
            return launched;
          },
        },
      });
      await run(executor, [{ kind: "add", count: 1 }]);
      const [free] = slots.listOpen(JOB);
      waitingRow("q-1");
      waitingRow("q-2", "dev-impl-s1@r");
      world.modelCause = "stopped";
      world.calls = [];
      history = [];
      const planned = planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents;
      await run(executor, [
        { kind: "relaunch", seat: MODEL.sessionName, nodeId: MODEL.nodeId },
        { kind: "dispatch", qitemId: "q-1", from: MODEL.sessionName, slotId: free!.slotId },
        { kind: "nudge", seat: "dev-impl-s1@r", qitemId: "q-2" },
        { kind: "escalate", seat: MODEL.sessionName, qitemIds: ["q-1"] },
      ]);
      expect(world.calls).toEqual(["relaunch dev.impl"]);
      // A plan made before drain was set changes nothing either.
      expect(planned.map((intent) => intent.kind)).toEqual(["dispatch", "nudge"]);
      await run(executor, planned);
      expect(world.calls).toEqual(["relaunch dev.impl"]);
      expect(world.rows.get("q-1")).toMatchObject({ destination: MODEL.sessionName, nudged: [] });
      expect(world.rows.get("q-2")).toMatchObject({ destination: "dev-impl-s1@r", nudged: [] });
      expect(world.escalations).toEqual([]);
      expect(history).toEqual([{ failed: false, reason: "seat_pool_relaunched", notes: { seat: "dev-impl@r" } }]);
    });

    it("moves a waiting row to a seat it just added, once the seat is up", async () => {
      jobSpec = spec({ dispatch_after_s: 60 });
      const executor = makeExecutor();
      waitingRow("q-1");
      await run(executor, planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents);
      expect(live()).toEqual(["dev.impl-s1"]);
      world.calls = [];
      await run(executor, planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents);
      expect(world.calls).toEqual(["move q-1 dev-impl@r -> dev-impl-s1@r"]);
      // The seat now holds the row: the next plan neither drains it nor adds another.
      expect(planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents).toEqual([]);
    });
  });


  it("keeps the demand command's last good answer, and records a bad one", async () => {
    jobSpec = spec({ demand_cmd: { argv: ["demand"] } });
    const executor = makeExecutor();
    world.demandStdout = '{"desired": 2, "reason": "two open tasks"}';
    await run(executor, []);
    expect(executor.demandSample(JOB)).toEqual({ desired: 2, sampledAt: new Date(T0).toISOString() });
    expect(world.env.at(-1)).toMatchObject({ OPENRIG_SEAT_POOL_SLOTS: "[]" });
    world.demandStdout = '{"desired": -1}';
    await run(executor, []);
    expect(executor.demandSample(JOB)?.desired).toBe(2);
    expect(history.at(-1)).toMatchObject({ failed: true, reason: "seat_pool_demand_failed" });
  });
});

describe("runSeatPoolCommand", () => {
  const node = process.execPath;

  it("runs an argv without a shell and returns its output", async () => {
    const result = await runSeatPoolCommand({ argv: [node, "-e", "process.stdout.write(process.argv[1] + process.env.OPENRIG_SEAT_POOL_JOB)", "$HOME;"], timeoutSeconds: 10 },
      { OPENRIG_SEAT_POOL_JOB: "j1" }, new AbortController().signal);
    expect(result).toEqual({ exitCode: 0, stdout: "$HOME;j1", timedOut: false });
  });

  it("reports the exit code, and kills a command at its timeout", async () => {
    const failed = await runSeatPoolCommand({ argv: [node, "-e", "process.exit(4)"], timeoutSeconds: 10 }, {}, new AbortController().signal);
    expect(failed.exitCode).toBe(4);
    const slow = await runSeatPoolCommand({ argv: [node, "-e", "setTimeout(() => {}, 60000)"], timeoutSeconds: 1 }, {}, new AbortController().signal);
    expect(slow).toMatchObject({ exitCode: null, timedOut: true, error: "timed out after 1s" });
  });
});

describe("readSeatPoolState", () => {
  let db: Database.Database;
  afterEach(() => db.close());

  it("resolves the model from its session and links seats only through the pool's rows", () => {
    db = createFullTestDb();
    const rigRepo = new RigRepository(db);
    const rig = rigRepo.createRig("r");
    const pod = new PodRepository(db).createPod(rig.id, "dev", "Dev");
    const add = (logicalId: string, session: string | null) => {
      const node = rigRepo.addNode(rig.id, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
      if (session) db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`s-${logicalId}`, node.id, session);
      return node;
    };
    const model = add("dev.impl", "dev-impl@r");
    const pooled = add("dev.impl-s1", "dev-impl-s1@r");
    add("dev.impl-s2", "dev-impl-s2@r"); // named like a pool seat, but not linked: never counted or adopted
    const fixed = add("dev.impl2", "dev-impl2@r");
    const slots = new SeatPoolSlots(db);
    slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: pooled.id, phase: "live", intent: null, opId: "op-1", at: "2026-11-09T11:00:00.000Z" });
    const changedAt = "2026-11-09T11:30:00.000Z";
    const rows: Record<string, Array<{ state: string }>> = { "dev-impl@r": [{ state: "pending" }, { state: "in-progress" }], "dev-impl-s1@r": [] };
    const state = readSeatPoolState({
      db, slots,
      openRows: (session) => rows[session] ?? [],
      sessionAddresses: (session) => [session],
      heldMessages: (nodeId) => (nodeId === pooled.id ? 2 : 0),
      seatStopped: () => false,
      seatState: () => ({ activity: "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: null, changedAt }) as never,
      restoring: () => false,
      demand: () => null,
    }, { jobId: JOB, targetSession: "dev-impl@r", spec: spec({ adopt: ["dev.impl2", "dev.impl", "dev.missing"] }) });
    expect(state.model).toEqual({ rigId: rig.id, rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: model.id, sessionName: "dev-impl@r" });
    expect(state.slots).toEqual([{
      slotId: 1, logicalId: "dev.impl-s1", sessionName: "dev-impl-s1@r", nodeId: pooled.id, phase: "live", adopted: false, nodeExists: true,
      openRows: 0, heldMessages: 2, idleSince: changedAt, cause: "idle",
    }]);
    expect(state.adoptable).toEqual([{ logicalId: "dev.impl2", nodeId: fixed.id }]);
    expect(state.modelCause).toBe("idle");
    expect(state.lastScaleAt).toBe("2026-11-09T11:00:00.000Z");
    expect(slots.nextLogicalId(state.model!)).toBe("dev.impl-s3");
  });

  it("reads the pending rows to the model, its seats and the seats it removed, and each seat's cause", () => {
    db = createFullTestDb();
    const rigRepo = new RigRepository(db);
    const rig = rigRepo.createRig("r");
    const pod = new PodRepository(db).createPod(rig.id, "dev", "Dev");
    const model = rigRepo.addNode(rig.id, "dev.impl", { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
    const s1 = rigRepo.addNode(rig.id, "dev.impl-s1", { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
    const s2 = rigRepo.addNode(rig.id, "dev.impl-s2", { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
    db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES ('s-model', ?, 'dev-impl@r', 'running')").run(model.id);
    const slots = new SeatPoolSlots(db);
    for (const [logicalId, nodeId] of [["dev.impl-s1", s1.id], ["dev.impl-s2", s2.id]] as const) {
      slots.insert({ poolJobId: JOB, logicalId, nodeId, phase: "live", intent: null, opId: `op-${logicalId}`, at: "2026-11-09T11:00:00.000Z" });
    }
    const removed = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s3", nodeId: null, phase: "live", intent: null, opId: "op-s3", at: "2026-11-09T11:00:00.000Z" });
    slots.retire(removed.slotId, "2026-11-09T11:10:00.000Z");
    const insert = (id: string, destination: string, state: string, ts: string) => db.prepare(
      `INSERT INTO queue_items (qitem_id, ts_created, ts_updated, source_session, destination_session, state, priority, body)
       VALUES (?, ?, ?, 'lead@r', ?, ?, 'routine', 'work')`,
    ).run(id, ts, ts, destination, state);
    insert("q-model", "impl.alias@r", "pending", "2026-11-09T11:20:00.000Z");
    insert("q-old", "dev-impl@r", "pending", "2026-11-09T11:05:00.000Z");
    insert("q-slot", "dev-impl-s1@r", "pending", "2026-11-09T11:30:00.000Z");
    insert("q-removed", "dev-impl-s3@r", "pending", "2026-11-09T11:40:00.000Z");
    // Rerouted here at 11:50: it has waited at this seat since then, not since its creation.
    insert("q-moved", "dev-impl@r", "pending", "2026-11-09T10:00:00.000Z");
    db.prepare("INSERT INTO queue_transitions (qitem_id, ts, state, actor_session) VALUES ('q-moved', ?, 'pending', 'system:queue-fallback')")
      .run("2026-11-09T11:50:00.000Z");
    insert("q-blocked", "dev-impl@r", "blocked", "2026-11-09T11:00:00.000Z");
    insert("q-claimed", "dev-impl@r", "in-progress", "2026-11-09T11:00:00.000Z");
    insert("q-human", "human@kernel", "pending", "2026-11-09T11:00:00.000Z");
    db.prepare("INSERT INTO queue_transitions (qitem_id, ts, state, actor_session, transition_note) VALUES ('q-old', ?, 'pending', 'seat-pool@system', 'seat-pool nudge: dev-impl@r')")
      .run("2026-11-09T11:06:00.000Z");
    db.prepare("INSERT INTO queue_transitions (qitem_id, ts, state, actor_session, transition_note) VALUES ('q-slot', ?, 'pending', 'seat-pool@system', 'seat-pool nudge: dev-impl@r')")
      .run("2026-11-09T11:31:00.000Z");

    const state = readSeatPoolState({
      db, slots,
      openRows: () => [],
      sessionAddresses: (session) => (session === "dev-impl@r" ? ["dev-impl@r", "impl.alias@r"] : [session]),
      heldMessages: () => 0,
      seatStopped: (nodeId) => nodeId === s2.id,
      seatState: (nodeId) => ({
        activity: "idle-at-prompt", needsInput: { count: nodeId === s1.id ? 1 : 0, reason: null }, backgroundWork: null, changedAt: "2026-11-09T11:00:00.000Z",
      }) as never,
      usageLimited: (session) => session === "dev-impl@r",
      restoring: () => false,
      demand: () => null,
    }, { jobId: JOB, targetSession: "dev-impl@r", spec: spec() });

    // Only pending rows, under the name the pool knows each seat by; a blocked, claimed or human row is never read.
    expect(state.pending).toEqual([
      { qitemId: "q-old", destination: "dev-impl@r", waitingSince: "2026-11-09T11:05:00.000Z", nudged: true },
      { qitemId: "q-model", destination: "dev-impl@r", waitingSince: "2026-11-09T11:20:00.000Z", nudged: false },
      // A nudge to another seat is not a nudge to this one.
      { qitemId: "q-slot", destination: "dev-impl-s1@r", waitingSince: "2026-11-09T11:30:00.000Z", nudged: false },
      { qitemId: "q-removed", destination: "dev-impl-s3@r", waitingSince: "2026-11-09T11:40:00.000Z", nudged: false },
      { qitemId: "q-moved", destination: "dev-impl@r", waitingSince: "2026-11-09T11:50:00.000Z", nudged: false },
    ]);
    expect(state.modelCause).toBe("usage_limit");
    expect(state.slots.map((slot) => [slot.sessionName, slot.cause])).toEqual([["dev-impl-s1@r", "needs_input"], ["dev-impl-s2@r", "stopped"]]);
  });

  it("builds a seat's member from the model's launch shape, with its own id and working directory", () => {
    const model = {
      runtime: "claude-code", agentRef: "local:agents/impl", profile: "default", model: "opus", effort: "high", role: "worker",
      permissionPolicy: "builtin:standard", restorePolicy: "resume_if_possible", env: { A: "1" }, launchWrapper: [], label: "Impl",
      codexConfigProfile: null, cwd: "/repo/impl",
    } as unknown as Node;
    expect(slotMemberFragment(model, "impl-s1", "/work/impl-s1", { claude: { allow: ["Read"] } })).toEqual({
      id: "impl-s1", runtime: "claude-code", cwd: "/work/impl-s1", agent_ref: "local:agents/impl", profile: "default",
      permission_policy: "builtin:standard", claude_permissions: { allow: ["Read"] }, model: "opus", effort: "high", role: "worker",
      restore_policy: "resume_if_possible", env: { A: "1" },
    });
  });
});

describe("seatPoolQueueEffects", () => {
  let db: Database.Database;
  let repo: QueueRepository;
  let owner: { owner: string | null; reason: string };
  let effects: ReturnType<typeof seatPoolQueueEffects>;

  beforeEach(() => {
    db = createDb();
    migrate(db, [coreSchema, eventsSchema, queueItemsSchema, queueTransitionsSchema, outboxEntriesSchema]);
    repo = new QueueRepository(db, new EventBus(db));
    repo.attachOutbox(new OutboxHandler(db));
    owner = { owner: "lead@r", reason: "routed" };
    effects = seatPoolQueueEffects({ queueRepo: repo, escalationOwner: () => owner });
  });
  afterEach(() => db.close());

  const fallbackWakes = (qitemId: string) =>
    db.prepare("SELECT destination_session FROM outbox_entries WHERE audit_pointer = ? AND outbox_id LIKE 'wake-intent-fallback-%'").all(qitemId);

  it("moves a row still pending at its seat, with a wake for the new seat", async () => {
    const row = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl@r", body: "work", nudge: false });
    expect(effects.moveRow({ qitemId: row.qitemId, from: "dev-impl@r", to: "dev-impl-s1@r", reason: "free seat" })).toEqual({ ok: true });
    expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: "dev-impl-s1@r", state: "pending" });
    expect(fallbackWakes(row.qitemId)).toEqual([{ destination_session: "dev-impl-s1@r" }]);
  });

  it("refuses to move a row that was claimed, blocked or moved since the read, and leaves it as it is", async () => {
    const claimed = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl@r", body: "work", nudge: false });
    repo.update({ qitemId: claimed.qitemId, actorSession: "dev-impl@r", state: "in-progress" });
    const blocked = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl@r", body: "work", nudge: false });
    repo.update({ qitemId: blocked.qitemId, actorSession: "dev-impl@r", state: "blocked", blockedOn: claimed.qitemId });
    const elsewhere = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl-s2@r", body: "work", nudge: false });
    const move = (qitemId: string) => effects.moveRow({ qitemId, from: "dev-impl@r", to: "dev-impl-s1@r", reason: "free seat" });
    expect(move(claimed.qitemId)).toMatchObject({ ok: false, code: "fallback_state_mismatch" });
    expect(move(blocked.qitemId)).toMatchObject({ ok: false, code: "fallback_state_mismatch" });
    expect(move(elsewhere.qitemId)).toMatchObject({ ok: false, code: "fallback_destination_mismatch" });
    expect([claimed, blocked].map((r) => repo.getById(r.qitemId)!.destinationSession)).toEqual(["dev-impl@r", "dev-impl@r"]);
    expect(repo.getById(elsewhere.qitemId)!.destinationSession).toBe("dev-impl-s2@r");
    expect(fallbackWakes(claimed.qitemId)).toEqual([]);
  });

  it("records the nudge on the row before sending it", async () => {
    const sends: string[] = [];
    repo.attachTransport({
      send: async (session: string) => {
        sends.push(session);
        // The note is already on the row when the send happens.
        expect(repo.listTransitions(row.qitemId).some((t) => t.transitionNote === "seat-pool nudge: dev-impl@r")).toBe(true);
        return { ok: true, verified: true };
      },
    } as never);
    const row = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl@r", body: "work", nudge: false });
    expect(await effects.nudgeRow({ qitemId: row.qitemId, seat: "dev-impl@r" })).toMatchObject({ ok: true });
    expect(sends).toEqual(["dev-impl@r"]);
    expect(repo.getById(row.qitemId)!.state).toBe("pending");
  });

  it("opens one escalation per seat while it is open, and none without an escalation owner", async () => {
    const first = await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1"] });
    expect(first).toMatchObject({ ok: true, destination: "lead@r", existing: false });
    const row = repo.getById(first.qitemId!)!;
    expect(row).toMatchObject({ destinationSession: "lead@r", sourceSession: "daemon@kernel", tags: [`${SEAT_POOL_NEEDS_INPUT_TAG}:dev-impl-s1@r`] });
    expect(row.body).toContain("q-1");
    expect(await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1", "q-2"] }))
      .toEqual({ ok: true, qitemId: first.qitemId, destination: "lead@r", existing: true });
    owner = { owner: null, reason: "no escalation destination declared" };
    expect(await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s2@r", model: "dev-impl@r", qitemIds: ["q-3"] }))
      .toEqual({ ok: false, code: "no_escalation_owner", error: "no escalation destination declared" });
  });
});
