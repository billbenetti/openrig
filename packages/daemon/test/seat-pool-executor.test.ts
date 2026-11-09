import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { PodRepository } from "../src/domain/pod-repository.js";
import type { StopPrecondition } from "../src/domain/seat-lifecycle-service.js";
import {
  readSeatPoolState,
  runSeatPoolCommand,
  SeatPoolExecutor,
  SeatPoolSlots,
  slotMemberFragment,
  type SeatPoolEffectResult,
  type SeatPoolEffects,
  type SeatPoolSlot,
} from "../src/domain/seat-pool-executor.js";
import {
  parseSeatPoolSpec,
  planSeatPool,
  type SeatPoolCommand,
  type SeatPoolModel,
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

interface FakeNode { logicalId: string; session: "none" | "running" | "stopped"; sessionId: string | null; openRows: number; idle: boolean; held?: number }

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
    };
  }

  state(slots: SeatPoolSlots, executor: { demandSample(jobId: string): { desired: number; sampledAt: string } | null }, jobSpec: SeatPoolSpec): SeatPoolState {
    return {
      model: MODEL,
      restoring: this.restoring,
      slots: slots.listOpen(JOB).map((s: SeatPoolSlot) => {
        const node = s.nodeId ? this.nodes.get(s.nodeId) : undefined;
        return {
          slotId: s.slotId, logicalId: s.logicalId, nodeId: s.nodeId, phase: s.phase as "live", adopted: s.opId === null,
          nodeExists: !!node, openRows: node?.openRows ?? 0, heldMessages: node?.held ?? 0,
          idleSince: node && node.session === "running" && node.idle ? new Date(T0 - 3_600_000).toISOString() : null,
        };
      }),
      adoptable: [],
      pendingForModel: 0,
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
      heldMessages: (nodeId) => (nodeId === pooled.id ? 2 : 0),
      seatState: () => ({ activity: "idle-at-prompt", needsInput: { count: 0 }, backgroundWork: null, changedAt }) as never,
      restoring: () => false,
      demand: () => null,
    }, { jobId: JOB, targetSession: "dev-impl@r", spec: spec({ adopt: ["dev.impl2", "dev.impl", "dev.missing"] }) });
    expect(state.model).toEqual({ rigId: rig.id, rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: model.id, sessionName: "dev-impl@r" });
    expect(state.slots).toEqual([{ slotId: 1, logicalId: "dev.impl-s1", nodeId: pooled.id, phase: "live", adopted: false, nodeExists: true, openRows: 0, heldMessages: 2, idleSince: changedAt }]);
    expect(state.adoptable).toEqual([{ logicalId: "dev.impl2", nodeId: fixed.id }]);
    expect(state.pendingForModel).toBe(1);
    expect(state.lastScaleAt).toBe("2026-11-09T11:00:00.000Z");
    expect(slots.nextLogicalId(state.model!)).toBe("dev.impl-s3");
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
