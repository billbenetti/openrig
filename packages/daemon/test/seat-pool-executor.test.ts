import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { eventsSchema } from "../src/db/migrations/003_events.js";
import { streamItemsSchema } from "../src/db/migrations/023_stream_items.js";
import { queueItemsSchema } from "../src/db/migrations/024_queue_items.js";
import { queueTransitionsSchema } from "../src/db/migrations/025_queue_transitions.js";
import { inboxEntriesSchema } from "../src/db/migrations/026_inbox_entries.js";
import { outboxEntriesSchema } from "../src/db/migrations/027_outbox_entries.js";
import { i3IdentityProvenanceSchema } from "../src/db/migrations/067_i3_identity_provenance.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { PodRepository } from "../src/domain/pod-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { QueueWakeRepository } from "../src/domain/queue-wake-repository.js";
import { WatchdogHistoryLog } from "../src/domain/watchdog-history-log.js";
import { watchdogHistorySchema } from "../src/db/migrations/032_watchdog_history.js";
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
import { LaunchBudget } from "../src/domain/launch-budget.js";

const JOB = "pool-job";
const MODEL: SeatPoolModel = {
  rigId: "rig-1", rigName: "r", podNamespace: "dev", memberId: "impl", logicalId: "dev.impl", nodeId: "model-node", sessionName: "dev-impl@r",
};
const T0 = Date.parse("2026-11-09T12:00:00.000Z");
/** When an idle seat in the World became idle. */
const IDLE_SINCE = new Date(T0 - 3_600_000).toISOString();
const drainOf = (slotId: number) => ({ kind: "drain" as const, slotId, idleSince: IDLE_SINCE });

function spec(overrides: Record<string, unknown> = {}): SeatPoolSpec {
  const parsed = parseSeatPoolSpec({ max: 3, cwd_template: "/work/{rig}/{slot}", idle_grace_s: 0, cooldown_s: 0, ...overrides });
  if (!("spec" in parsed)) throw new Error(parsed.problem.message);
  return parsed.spec;
}

interface FakeNode {
  logicalId: string; session: "none" | "running" | "stopped"; sessionId: string | null; openRows: number; idle: boolean; held?: number;
  cause?: SeatPoolSeatCause; idleSince?: string;
}
interface FakeRow { destination: string; waitingSince: string; state?: "pending" | "in-progress" | "blocked"; reaps?: number }

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
  cleanResult: SeatPoolEffectResult = { ok: true };
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
        if (signal.aborted) return { exitCode: null, stdout: "", error: "aborted" };
        return { exitCode: this.exit[name] ?? 0, stdout: name === "demand" ? this.demandStdout : "" };
      },
      cleanSlot: async ({ sessionName }) => {
        this.calls.push(`clean ${sessionName}`);
        return this.cleanResult;
      },
      openRows: (sessionName) => [...this.rows].filter(([, row]) => row.destination === sessionName)
        .map(([qitemId, row]) => ({ qitemId, state: row.state ?? "pending" })),
      unclaimRow: ({ qitemId, reason }) => {
        this.calls.push(`unclaim ${qitemId} ${reason}`);
        this.rows.get(qitemId)!.state = "pending";
        return { ok: true };
      },
      reapCount: (qitemId) => this.rows.get(qitemId)?.reaps ?? 0,
      moveRow: ({ qitemId, from, to, reason, state = "pending" }) => {
        this.calls.push(`move ${qitemId} ${from} -> ${to}`);
        if (!this.moveResult.ok) return this.moveResult;
        const row = this.rows.get(qitemId)!;
        if ((row.state ?? "pending") !== state) return { ok: false, code: "fallback_state_mismatch", error: `${qitemId} is ${row.state}` };
        if (reason.startsWith("reap:")) row.reaps = (row.reaps ?? 0) + 1;
        // As the reroute does: the row now waits at its new seat from this moment.
        Object.assign(row, { destination: to, waitingSince: new Date(T0).toISOString() });
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
      escalationOwner: (_model, override) => ({ owner: override ?? this.escalationOwner, reason: "no escalation destination declared" }),
      escalate: async ({ seat, qitemIds, owner }) => {
        this.calls.push(`escalate ${seat} ${qitemIds.join(",")}`);
        const to = owner ?? this.escalationOwner;
        if (!to) return { ok: false, code: "no_escalation_owner", error: "no escalation destination declared" };
        const existing = this.escalations.includes(seat);
        if (!existing) this.escalations.push(seat);
        return { ok: true, qitemId: `esc-${seat}`, destination: to, existing };
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
          idleSince: idle ? node.idleSince ?? IDLE_SINCE : null,
          cause: node?.cause ?? (node?.session !== "running" ? "stopped" : idle ? "idle" : "working"),
        };
      }),
      adoptable: [],
      pending: [...this.rows].map(([qitemId, row]) => ({ qitemId, destination: row.destination, waitingSince: row.waitingSince, tags: [] })),
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

  const makeExecutor = (effectTimeoutMs?: number, launchBudget?: LaunchBudget): SeatPoolExecutor => {
    const executor: SeatPoolExecutor = new SeatPoolExecutor({
      slots,
      readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
      readState: () => world.state(slots, executor, jobSpec),
      outOfUsage: async () => seatPoolOutOfUsage(world.state(slots, executor, jobSpec)),
      effects: world.effects(),
      record: (_jobId, entry) => { history.push(entry); },
      now: () => new Date(T0),
      effectTimeoutMs,
      launchBudget,
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
    await run(executor, [drainOf(second.slotId)]);
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
    expect(history).toEqual([{ failed: false, reason: "seat_pool_launched", notes: { slot: "dev.impl-s1", nodeId: "node-9" } }]);
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
    expect(history.find((h) => h.reason === "seat_pool_add_failed")).toMatchObject({ notes: { slot: "dev.impl-s2", code: "add_not_completed" } });
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
    await run(executor, [drainOf(slot!.slotId)]);
    const expected: StopPrecondition = { nodeId: "node-1", sessionId: "sess-2" };
    expect(world.calls).toEqual([
      "lease dev.impl-s1", "run release", `stop dev-impl-s1@r ${JSON.stringify(expected)}`, `remove dev.impl-s1 ${JSON.stringify(expected)}`, "lease released",
    ]);
    expect(world.env.at(-1)).toMatchObject({ OPENRIG_SEAT_POOL_SLOT: "dev-impl-s1@r" });
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(reasons().at(-1)).toBe("seat_pool_removed");
  });

  it("runs remove_cmd once a removed seat's node is gone, and keeps the seat on record until it agrees", async () => {
    jobSpec = spec({ remove_cmd: { argv: ["remove"] } });
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    const [slot] = slots.listOpen(JOB);
    world.calls = [];
    world.env = [];
    world.exit["remove"] = 1;
    await run(executor, [drainOf(slot!.slotId)]);
    expect(world.calls.slice(-2)).toEqual(["lease released", "run remove"]);
    expect(world.env.at(-1)).toEqual({ OPENRIG_SEAT_POOL_JOB: JOB, OPENRIG_SEAT_POOL_MODEL: "dev-impl@r", OPENRIG_SEAT_POOL_SLOT: "dev-impl-s1@r" });
    expect(world.nodes.size).toBe(0);
    expect(slots.listOpen(JOB)).toHaveLength(1);
    expect(history.slice(-2)).toEqual([
      { failed: false, reason: "seat_pool_removed", notes: { slot: "dev.impl-s1" } },
      { failed: true, reason: "seat_pool_remove_cmd_failed", notes: { slot: "dev.impl-s1", exitCode: 1, error: null } },
    ]);

    world.exit["remove"] = 0;
    const plan = planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000);
    expect(plan.intents).toEqual([{ kind: "remove", slotId: slot!.slotId }]);
    await run(executor, plan.intents);
    expect(world.calls.at(-1)).toBe("run remove");
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_removed", notes: { slot: "dev.impl-s1", why: "node_gone" } });
  });

  it("never runs remove_cmd for an adopted seat whose node is gone", async () => {
    jobSpec = spec({ remove_cmd: { argv: ["remove"] } });
    const adopted = slots.adopt({ poolJobId: JOB, logicalId: "dev.impl2", nodeId: "node-9", at: new Date(T0).toISOString() });
    await run(makeExecutor(), [{ kind: "remove", slotId: adopted.slotId }]);
    expect(world.calls).toEqual([]);
    expect(slots.listOpen(JOB)).toEqual([]);
  });

  it("returns a seat to the pool, untouched, when its release command refuses or work arrives in the lease", async () => {
    jobSpec = spec({ release_cmd: { argv: ["release"] } });
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 2 }]);
    const [first, second] = slots.listOpen(JOB);
    world.calls = [];
    world.exit["release"] = 1;
    await run(executor, [drainOf(first!.slotId)]);
    world.exit["release"] = 0;
    world.onLease = () => { world.nodes.get(second!.nodeId!)!.openRows = 1; };
    await run(executor, [drainOf(second!.slotId)]);
    expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("remove"))).toEqual([]);
    expect(live()).toEqual(["dev.impl-s1", "dev.impl-s2"]);
    expect(history.slice(-2)).toEqual([
      { failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s1", why: "release_refused", exitCode: 1, error: null } },
      { failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s2", why: "work_arrived" } },
    ]);
  });

  // T-004870 F3, r1 rule 2: the drain's clock is the job's history. Once the retention prunes a drain's entries, the
  // drain is over for the executor too (as for the job's stop): the next run starts a new one, from what it recorded
  // in memory neither.
  it("starts a new drain once the history's drain entries are pruned, without falling back to what it recorded", async () => {
    jobSpec = spec({ drain: true, release_cmd: { argv: ["release"] } });
    let nowMs = T0;
    // The history the daemon keeps for the job, without the job's row (the executor reads its job through readJob).
    db.pragma("foreign_keys = OFF");
    migrate(db, [watchdogHistorySchema]);
    const log = new WatchdogHistoryLog(db);
    const executor: SeatPoolExecutor = new SeatPoolExecutor({
      slots, readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
      readState: () => world.state(slots, executor, jobSpec), effects: world.effects(), now: () => new Date(nowMs),
      record: (jobId, entry) => {
        history.push(entry);
        log.record({ jobId, evaluatedAt: new Date(nowMs).toISOString(), outcome: entry.failed ? "failed" : "skipped", skipReason: entry.reason, evaluationNotes: entry.notes });
      },
    });
    const slot = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
    world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "running", sessionId: "sess-1", openRows: 1, idle: true });
    world.exit["release"] = 1;
    const drainReasons = () => history.map((h) => h.reason).filter((r) => r.startsWith("seat_pool_drain_"));

    await run(executor, []);
    nowMs = T0 + 901_000;
    await run(executor, []);
    expect(drainReasons()).toEqual(["seat_pool_drain_started", "seat_pool_drain_expired"]);
    expect(history.at(-1)).toMatchObject({ notes: { left: [{ slot: "dev.impl-s1", why: "release_refused" }] } });
    await run(executor, []);
    expect(drainReasons()).toHaveLength(2);

    db.prepare("DELETE FROM watchdog_history WHERE job_id = ?").run(JOB);
    nowMs = T0 + 2_000_000;
    await run(executor, []);
    expect(drainReasons()).toEqual(["seat_pool_drain_started", "seat_pool_drain_expired", "seat_pool_drain_started"]);
    expect(history.at(-1)!.notes).toMatchObject({ deadlineAt: new Date(T0 + 2_000_000 + 900_000).toISOString() });
    expect(slots.get(slot.slotId)?.retiredAt).toBeNull();
  });

  it("leaves a seat that was relaunched since the read, and never removes it", async () => {
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 1 }]);
    world.stopResult = { ok: false, code: "selection_changed", error: "the seat changed" };
    world.calls = [];
    await run(executor, [drainOf(slots.listOpen(JOB)[0]!.slotId)]);
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
    await run(executor, [drainOf(slot!.slotId)]);
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
    await run(executor, [drainOf(first!.slotId), drainOf(second!.slotId)]);
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
    await run(executor, [drainOf(slot!.slotId)]);
    expect(world.calls).toEqual(["lease dev.impl-s1", "run release", "lease released"]);
    expect(slots.get(slot!.slotId)).toMatchObject({ phase: "live", intent: null });
    expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s1", why: "work_arrived" } });
  });

  it("does not stop a seat woken since the plan, even one idle again with nothing left open", async () => {
    jobSpec = spec({ release_cmd: { argv: ["release"] } });
    const executor = makeExecutor();
    await run(executor, [{ kind: "add", count: 2 }]);
    const [first, second] = slots.listOpen(JOB);
    // A message woke the first between the plan and the lease; it took its turn and is back at its prompt.
    world.onLease = () => { world.nodes.get(first!.nodeId!)!.idleSince = new Date(T0 - 5_000).toISOString(); };
    await run(executor, [drainOf(first!.slotId)]);
    // The second is woken while its release command runs.
    world.onLease = () => {};
    world.commandGate = async () => { Object.assign(world.nodes.get(second!.nodeId!)!, { idle: false }); };
    world.calls = [];
    await run(executor, [drainOf(second!.slotId)]);
    expect(world.calls).toEqual(["lease dev.impl-s2", "run release", "lease released"]);
    expect(live()).toEqual(["dev.impl-s1", "dev.impl-s2"]);
    expect(reasons().slice(-2)).toEqual(["seat_pool_returned", "seat_pool_returned"]);
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

  it("gives an adopted seat back under drain without touching it, and never stops or removes it", async () => {
    world.nodes.set("node-9", { logicalId: "dev.impl2", session: "running", sessionId: "sess-9", openRows: 0, idle: true });
    const adopted = slots.adopt({ poolJobId: JOB, logicalId: "dev.impl2", nodeId: "node-9", at: new Date(T0).toISOString() });
    const executor = makeExecutor();
    await run(executor, [drainOf(adopted.slotId), { kind: "unlink", slotId: adopted.slotId }]);
    // Without drain, neither the drain nor the unlink does anything.
    expect(world.calls).toEqual([]);
    expect(slots.get(adopted.slotId)).toMatchObject({ phase: "live", retiredAt: null });
    jobSpec = spec({ drain: true });
    await run(executor, planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents);
    expect(world.calls).toEqual([]);
    expect(world.nodes.get("node-9")).toMatchObject({ session: "running" });
    expect(slots.listOpen(JOB)).toEqual([]);
    expect(history).toEqual([{ failed: false, reason: "seat_pool_unlinked", notes: { slot: "dev.impl2" } }]);
    // A seat given back is not one the pool removed: rows to it are not the pool's, and it starts no cooldown.
    expect(slots.listRetired(JOB)).toEqual([]);
    expect(slots.lastScaleAt(JOB)).toBeNull();
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

  it("stops without waiting for a launch that never settles, whether the run gave up on it or was waiting for it", async () => {
    world.launchGate = new Promise(() => {});
    const gaveUp = makeExecutor(20);
    expect(gaveUp.submit(JOB, [{ kind: "add", count: 1 }])).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 60));
    const waiting = makeExecutor();
    expect(waiting.submit("other-pool", [{ kind: "add", count: 1 }])).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 10));
    const stopped = await Promise.race([
      Promise.all([gaveUp.stop(), waiting.stop()]).then(() => true),
      new Promise((resolve) => setTimeout(() => resolve(false), 500)),
    ]);
    expect(stopped).toBe(true);
    // Only the timeout is recorded; the cancel is not a failure, and the seat's intent is left for the next start.
    expect(reasons()).toEqual(["seat_pool_launch_failed"]);
    expect([...slots.listOpen(JOB), ...slots.listOpen("other-pool")].map((s) => s.intent)).toEqual(["launch", "launch"]);
  });

  describe("dispatch and wake", () => {
    const waitingRow = (qitemId: string, destination = MODEL.sessionName) =>
      world.rows.set(qitemId, { destination, waitingSince: new Date(T0 - 600_000).toISOString() });

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
        { kind: "escalate", seat: MODEL.sessionName, qitemIds: ["q-1"] },
      ]);
      expect(world.calls).toEqual(["relaunch dev.impl"]);
      // A plan made before drain was set changes nothing either.
      expect(planned.map((intent) => intent.kind)).toEqual(["dispatch"]);
      await run(executor, planned);
      expect(world.calls).toEqual(["relaunch dev.impl"]);
      expect(world.rows.get("q-1")).toMatchObject({ destination: MODEL.sessionName });
      expect(world.rows.get("q-2")).toMatchObject({ destination: "dev-impl-s1@r" });
      expect(world.escalations).toEqual([]);
      // The first run that reads drain: true starts the drain's clock (T-004870 F3); nothing else is recorded.
      expect(history.filter((entry) => entry.reason === "seat_pool_drain_started")).toHaveLength(1);
      expect(history.filter((entry) => entry.reason !== "seat_pool_drain_started")).toEqual([{ failed: false, reason: "seat_pool_relaunched", notes: { seat: "dev-impl@r" } }]);
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


  describe("reap", () => {
    const row = (qitemId: string, destination: string, state: FakeRow["state"] = "pending", reaps = 0) =>
      world.rows.set(qitemId, { destination, waitingSince: new Date(T0 - 600_000).toISOString(), state, reaps });
    /** One seat of the pool, live, with its node set as given. */
    const seat = async (executor: SeatPoolExecutor, node: Partial<FakeNode>) => {
      await run(executor, [{ kind: "add", count: 1 }]);
      const [added] = slots.listOpen(JOB).slice(-1);
      Object.assign(world.nodes.get(added!.nodeId!)!, node);
      world.calls = [];
      world.env = [];
      history = [];
      return added!;
    };

    it("reaps a seat whose session is gone under one lease: clean, reap_cmd, unclaim, rows back to the model, checked removal", async () => {
      jobSpec = spec({ reap_cmd: { argv: ["reap"] } });
      const executor = makeExecutor();
      const dead = await seat(executor, { session: "stopped", idle: false, openRows: 3 });
      row("q-1", "dev-impl-s1@r", "in-progress");
      row("q-2", "dev-impl-s1@r");
      row("q-3", "dev-impl-s1@r", "blocked");
      await run(executor, [{ kind: "reap", slotId: dead.slotId }]);
      const expected = JSON.stringify({ nodeId: dead.nodeId, sessionId: "sess-2" });
      expect(world.calls).toEqual([
        "lease dev.impl-s1", "clean dev-impl-s1@r", "run reap", "unclaim q-1 reap:dev-impl-s1@r",
        "move q-1 dev-impl-s1@r -> dev-impl@r", "move q-2 dev-impl-s1@r -> dev-impl@r", "move q-3 dev-impl-s1@r -> dev-impl@r",
        `remove dev.impl-s1 ${expected}`, "lease released",
      ]);
      expect(world.env).toEqual([{ OPENRIG_SEAT_POOL_JOB: JOB, OPENRIG_SEAT_POOL_MODEL: "dev-impl@r", OPENRIG_SEAT_POOL_SLOT: "dev-impl-s1@r" }]);
      expect([...world.rows.values()].map((r) => [r.destination, r.state])).toEqual([
        ["dev-impl@r", "pending"], ["dev-impl@r", "pending"], ["dev-impl@r", "blocked"],
      ]);
      expect(slots.listOpen(JOB)).toEqual([]);
      expect(history).toEqual([{ failed: false, reason: "seat_pool_reaped", notes: {
        slot: "dev.impl-s1",
        rows: ["q-1", "q-2", "q-3"].map((qitemId) => ({ qitemId, to: "dev-impl@r" })),
      } }]);
    });

    it("runs remove_cmd after a reap removed the seat's node", async () => {
      jobSpec = spec({ reap_cmd: { argv: ["reap"] }, remove_cmd: { argv: ["remove"] } });
      const executor = makeExecutor();
      const dead = await seat(executor, { session: "stopped", idle: false });
      await run(executor, [{ kind: "reap", slotId: dead.slotId }]);
      expect(world.calls.slice(-3)).toEqual([`remove dev.impl-s1 ${JSON.stringify({ nodeId: dead.nodeId, sessionId: "sess-2" })}`, "lease released", "run remove"]);
      expect(world.env.at(-1)).toMatchObject({ OPENRIG_SEAT_POOL_SLOT: "dev-impl-s1@r" });
      expect(slots.listOpen(JOB)).toEqual([]);
    });

    it("asks the release command first, and launches the seat again instead when it refuses, reaping it only if that fails", async () => {
      jobSpec = spec({ release_cmd: { argv: ["release"] }, reap_cmd: { argv: ["reap"] } });
      const executor = makeExecutor();
      const saved = await seat(executor, { session: "stopped", idle: false, openRows: 1 });
      row("q-1", "dev-impl-s1@r", "in-progress");
      await run(executor, [{ kind: "reap", slotId: saved.slotId }]);
      expect(world.calls.slice(0, 4)).toEqual(["lease dev.impl-s1", "run release", "clean dev-impl-s1@r", "run reap"]);
      expect(reasons()).toEqual(["seat_pool_reaped"]);

      // Its work isn't saved: the seat comes back to finish it, and nothing of it is moved or removed.
      const unsaved = await seat(executor, { session: "stopped", idle: false, openRows: 1 });
      row("q-2", "dev-impl-s2@r", "in-progress");
      world.exit["release"] = 1;
      await run(executor, [{ kind: "reap", slotId: unsaved.slotId }]);
      expect(world.calls).toEqual(["lease dev.impl-s2", "run release", "lease released", "relaunch dev.impl-s2"]);
      expect(history).toEqual([{ failed: false, reason: "seat_pool_relaunched", notes: { seat: "dev-impl-s2@r", why: "release_refused", exitCode: 1, error: null } }]);
      expect(world.rows.get("q-2")).toMatchObject({ destination: "dev-impl-s2@r", state: "in-progress" });
      expect(slots.get(unsaved.slotId)).toMatchObject({ phase: "live", intent: null });

      // A seat that can't come back is reaped, without asking the release command again.
      world.calls = [];
      history = [];
      world.relaunchResult = { ok: false, code: "resume_failed", error: "no resumable session" };
      await run(executor, [{ kind: "reap", slotId: unsaved.slotId }]);
      expect(world.calls).toEqual([
        "lease dev.impl-s2", "run release", "lease released", "relaunch dev.impl-s2",
        "lease dev.impl-s2", "clean dev-impl-s2@r", "run reap", "unclaim q-2 reap:dev-impl-s2@r", "move q-2 dev-impl-s2@r -> dev-impl@r",
        `remove dev.impl-s2 ${JSON.stringify({ nodeId: unsaved.nodeId, sessionId: "sess-4" })}`, "lease released",
      ]);
      expect(reasons()).toEqual(["seat_pool_relaunch_failed", "seat_pool_reaped"]);
    });

    it("never reaps an adopted seat whose session is gone", async () => {
      world.nodes.set("node-9", { logicalId: "dev.impl2", session: "stopped", sessionId: "sess-9", openRows: 1, idle: false });
      const adopted = slots.adopt({ poolJobId: JOB, logicalId: "dev.impl2", nodeId: "node-9", at: new Date(T0).toISOString() });
      await run(makeExecutor(), [{ kind: "reap", slotId: adopted.slotId }]);
      expect(world.calls).toEqual(["lease dev.impl2", "lease released"]);
      expect(slots.get(adopted.slotId)).toMatchObject({ phase: "live", retiredAt: null });
    });

    it("sends a row a reap already moved to the escalation owner, and to the model when there is none", async () => {
      const executor = makeExecutor();
      const first = await seat(executor, { session: "stopped", idle: false });
      row("q-1", "dev-impl-s1@r", "in-progress", 1);
      row("q-2", "dev-impl-s1@r");
      await run(executor, [{ kind: "reap", slotId: first.slotId }]);
      expect(history.at(-1)).toMatchObject({ reason: "seat_pool_reaped", notes: { rows: [
        { qitemId: "q-1", to: "lead@r", escalated: true }, { qitemId: "q-2", to: "dev-impl@r" },
      ] } });
      expect(world.rows.get("q-1")).toMatchObject({ destination: "lead@r", reaps: 2 });

      world.escalationOwner = null;
      const second = await seat(executor, { session: "stopped", idle: false });
      world.rows.get("q-2")!.destination = "dev-impl-s2@r";
      await run(executor, [{ kind: "reap", slotId: second.slotId }]);
      expect(history).toEqual([
        { failed: true, reason: "seat_pool_escalation_failed", notes: { seat: "dev-impl-s2@r", qitemId: "q-2", code: "no_escalation_owner", error: "no escalation destination declared" } },
        { failed: false, reason: "seat_pool_reaped", notes: { slot: "dev.impl-s2", rows: [{ qitemId: "q-2", to: "dev-impl@r" }] } },
      ]);
    });

    it("moves nothing when the session is found alive or reap_cmd fails, and finishes on a later plan", async () => {
      jobSpec = spec({ reap_cmd: { argv: ["reap"] } });
      const executor = makeExecutor();
      const dead = await seat(executor, { session: "stopped", idle: false, openRows: 1 });
      row("q-1", "dev-impl-s1@r", "in-progress");
      world.cleanResult = { ok: false, code: "session_live", error: "alive in tmux" };
      await run(executor, [{ kind: "reap", slotId: dead.slotId }]);
      world.cleanResult = { ok: false, code: "nothing_to_clean", error: "already clean" };
      world.exit["reap"] = 3;
      await run(executor, [{ kind: "reap", slotId: dead.slotId }]);
      expect(world.calls.filter((c) => /^(unclaim|move|remove)/.test(c))).toEqual([]);
      expect(world.rows.get("q-1")).toMatchObject({ destination: "dev-impl-s1@r", state: "in-progress" });
      expect(slots.get(dead.slotId)).toMatchObject({ phase: "live", intent: null });
      expect(history).toEqual([
        { failed: true, reason: "seat_pool_reap_failed", notes: { slot: "dev.impl-s1", step: "clean", code: "session_live", error: "alive in tmux" } },
        { failed: true, reason: "seat_pool_reap_failed", notes: { slot: "dev.impl-s1", step: "reap_cmd", exitCode: 3, error: null } },
      ]);
      world.exit["reap"] = 0;
      await run(executor, planSeatPool(jobSpec, world.state(slots, executor, jobSpec), T0, 120_000).intents);
      expect(world.rows.get("q-1")).toMatchObject({ destination: MODEL.sessionName, state: "pending" });
      expect(slots.listOpen(JOB)).toEqual([]);
    });

    it("leaves a seat that came back since the plan", async () => {
      const executor = makeExecutor();
      const back = await seat(executor, {});
      await run(executor, [{ kind: "reap", slotId: back.slotId }]);
      expect(world.calls).toEqual(["lease dev.impl-s1", "lease released"]);
      expect(history).toEqual([]);
      expect(live()).toEqual(["dev.impl-s1"]);
    });
  });

  describe("launch budget", () => {
    // The host itself always has room, so only launches in flight and the tick's launches refuse.
    const budgetOf = (maxInFlight: number, maxPerTick: number) => new LaunchBudget({
      limits: () => ({ maxInFlight, maxPerTick, minFreeMemoryBytes: 0, maxLoadPerCpu: 100 }),
      sample: () => ({ cpuCount: 1, load1: 0, freeMemoryBytes: 1 }),
    });
    // Room for one launch at a time, and more launches per tick than these tests start.
    const oneAtATime = () => budgetOf(1, 100);

    it("adds nothing for a pool while another pool's launch holds the daemon's room, and adds once it settles", async () => {
      const budget = oneAtATime();
      const executor = makeExecutor(undefined, budget);
      let release!: () => void;
      world.launchGate = new Promise((resolve) => { release = resolve; });
      expect(executor.submit("other-pool", [{ kind: "add", count: 1 }])).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(budget.launchesInFlight).toBe(1);
      await run(executor, [{ kind: "add", count: 2 }]);
      expect(world.calls).toEqual(["add impl-s1 /work/r/impl-s1", "run prepare", "launch dev.impl-s1"]);
      expect(slots.listOpen(JOB)).toEqual([]);
      expect(history).toEqual([{ failed: false, reason: "seat_pool_held", notes: { reason: "launch_budget", why: "launches_in_flight", inFlight: 1, maxInFlight: 1, add: 2 } }]);
      release();
      await executor.settled("other-pool");
      expect(budget.launchesInFlight).toBe(0);
      world.calls = [];
      await run(executor, [{ kind: "add", count: 2 }]);
      // One at a time: the second add reserves only once the first launch has settled.
      expect(world.calls).toEqual([
        "add impl-s2 /work/r/impl-s2", "run prepare", "launch dev.impl-s2",
        "add impl-s3 /work/r/impl-s3", "run prepare", "launch dev.impl-s3",
      ]);
      expect(live()).toEqual(["dev.impl-s2", "dev.impl-s3"]);
    });

    it("holds a seat's first launch and a relaunch without room, but not a seat whose launch already ran", async () => {
      const budget = oneAtATime();
      const elsewhere = budget.reserve();
      world.nodes.set("node-8", { logicalId: "dev.impl-s8", session: "none", sessionId: null, openRows: 0, idle: false });
      const unstarted = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s8", nodeId: "node-8", phase: "added", intent: null, opId: "op-8", at: new Date(T0).toISOString() });
      world.nodes.set("node-9", { logicalId: "dev.impl-s9", session: "running", sessionId: "sess-9", openRows: 0, idle: true });
      const launched = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s9", nodeId: "node-9", phase: "added", intent: "launch", opId: "op-9", at: new Date(T0).toISOString() });
      world.modelCause = "stopped";
      await run(makeExecutor(undefined, budget), [
        { kind: "launch", slotId: unstarted.slotId },
        { kind: "relaunch", seat: MODEL.sessionName, nodeId: MODEL.nodeId },
        { kind: "launch", slotId: launched.slotId },
      ]);
      expect(world.calls).toEqual([]);
      expect(slots.get(unstarted.slotId)).toMatchObject({ phase: "added", intent: null });
      expect(slots.get(launched.slotId)).toMatchObject({ phase: "live", intent: null });
      const held = { failed: false, reason: "seat_pool_held" };
      expect(history).toEqual([
        { ...held, notes: { reason: "launch_budget", why: "launches_in_flight", inFlight: 1, maxInFlight: 1, slot: "dev.impl-s8" } },
        { ...held, notes: { reason: "launch_budget", why: "launches_in_flight", inFlight: 1, maxInFlight: 1, seat: "dev-impl@r" } },
        { failed: false, reason: "seat_pool_launched", notes: { slot: "dev.impl-s9", nodeId: "node-9" } },
      ]);
      if (elsewhere.ok) elsewhere.release();
      world.calls = [];
      await run(makeExecutor(undefined, budget), [{ kind: "launch", slotId: unstarted.slotId }, { kind: "relaunch", seat: MODEL.sessionName, nodeId: MODEL.nodeId }]);
      expect(world.calls).toEqual(["run prepare", "launch dev.impl-s8", "relaunch dev.impl"]);
      expect(budget.launchesInFlight).toBe(0);
    });

    it("starts one seat of a plan per tick even when each launch settles before the next, and the next tick one more", async () => {
      const budget = budgetOf(1, 1);
      const executor = makeExecutor(undefined, budget);
      await run(executor, [{ kind: "add", count: 3 }]);
      expect(world.calls).toEqual(["add impl-s1 /work/r/impl-s1", "run prepare", "launch dev.impl-s1"]);
      expect(budget.launchesInFlight).toBe(0);
      expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_held", notes: { reason: "launch_budget", why: "launches_per_tick", thisTick: 1, maxPerTick: 1, add: 2 } });
      // A second plan in the same tick finds it spent.
      world.calls = [];
      await run(executor, [{ kind: "add", count: 2 }]);
      expect(world.calls).toEqual([]);
      budget.nextTick();
      await run(executor, [{ kind: "add", count: 2 }]);
      expect(world.calls).toEqual(["add impl-s2 /work/r/impl-s2", "run prepare", "launch dev.impl-s2"]);
      expect(live()).toEqual(["dev.impl-s1", "dev.impl-s2"]);
    });

    it("shares a tick's launches with the pools planned in it, and charges a run that outlasts its tick to that tick", async () => {
      const budget = budgetOf(2, 1);
      const executor = makeExecutor(undefined, budget);
      let release!: () => void;
      world.launchGate = new Promise((resolve) => { release = resolve; });
      expect(executor.submit("other-pool", [{ kind: "add", count: 1 }])).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 10));
      // Room for a second launch in flight, but the tick's one launch went to the other pool.
      await run(executor, [{ kind: "add", count: 1 }]);
      expect(history.at(-1)).toEqual({ failed: false, reason: "seat_pool_held", notes: { reason: "launch_budget", why: "launches_per_tick", thisTick: 1, maxPerTick: 1, add: 1 } });
      release();
      await executor.settled("other-pool");
      world.launchGate = new Promise((resolve) => { release = resolve; });
      world.calls = [];
      budget.nextTick();
      expect(executor.submit(JOB, [{ kind: "add", count: 2 }])).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 10));
      // The tick ends while the run's first launch is still in flight; its second add still counts against it.
      budget.nextTick();
      release();
      await executor.settled(JOB);
      expect(world.calls).toEqual(["add impl-s2 /work/r/impl-s2", "run prepare", "launch dev.impl-s2"]);
      expect(history.at(-1)).toMatchObject({ reason: "seat_pool_held", notes: { why: "launches_per_tick", add: 1 } });
      // The tick that began meanwhile still has its launch for the next plan.
      world.launchGate = null;
      world.calls = [];
      await run(executor, [{ kind: "add", count: 1 }]);
      expect(world.calls).toEqual(["add impl-s3 /work/r/impl-s3", "run prepare", "launch dev.impl-s3"]);
    });

    it("keeps the room until a launch past its timeout settles, and frees it after a failed launch", async () => {
      const budget = oneAtATime();
      let release!: () => void;
      world.launchGate = new Promise((resolve) => { release = resolve; });
      const executor = makeExecutor(20, budget);
      expect(executor.submit(JOB, [{ kind: "add", count: 1 }])).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(history).toEqual([{ failed: true, reason: "seat_pool_launch_failed", notes: { slot: "dev.impl-s1", code: "effect_timeout" } }]);
      // The run stopped waiting, but the launch is still running: it still counts.
      expect(budget.launchesInFlight).toBe(1);
      release();
      await executor.settled(JOB);
      expect(budget.launchesInFlight).toBe(0);
      world.launchGate = null;
      world.launchResult = { ok: false, code: "startup_failed", error: "harness exited" };
      await run(executor, [{ kind: "add", count: 1 }]);
      expect(history.at(-1)).toMatchObject({ reason: "seat_pool_removed", notes: { slot: "dev.impl-s2" } });
      expect(budget.launchesInFlight).toBe(0);
    });
  });

  describe("F6a: relaunch backoff, the add's admission, a reaped seat's blocked rows, the spec's escalation owner", () => {
    const budgetOf = (maxPerTick: number) => new LaunchBudget({
      limits: () => ({ maxInFlight: 10, maxPerTick, minFreeMemoryBytes: 0, maxLoadPerCpu: 100 }),
      sample: () => ({ cpuCount: 1, load1: 0, freeMemoryBytes: 1 }),
    });
    const resumeFailed: SeatPoolEffectResult = { ok: false, code: "resume_failed", error: "no conversation to resume" };
    const relaunchModel = [{ kind: "relaunch" as const, seat: MODEL.sessionName, nodeId: MODEL.nodeId }];
    let clock: number;
    const clocked = (opts: { launchBudget?: LaunchBudget; effects?: SeatPoolEffects; outOfUsage?: () => Promise<boolean> } = {}): SeatPoolExecutor => {
      const executor: SeatPoolExecutor = new SeatPoolExecutor({
        slots,
        readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
        readState: () => world.state(slots, executor, jobSpec),
        outOfUsage: opts.outOfUsage ?? (async () => seatPoolOutOfUsage(world.state(slots, executor, jobSpec))),
        effects: opts.effects ?? world.effects(),
        record: (_jobId, entry) => { history.push(entry); },
        now: () => new Date(clock),
        launchBudget: opts.launchBudget,
      });
      return executor;
    };
    const relaunches = () => world.calls.filter((call) => call.startsWith("relaunch ")).length;
    const backoffHolds = () => history.filter((h) => h.reason === "seat_pool_held" && h.notes["reason"] === "relaunch_backoff");
    beforeEach(() => { clock = T0; });

    it("a stopped seat whose relaunch keeps failing does not starve the add", async () => {
      const budget = budgetOf(1);
      const executor = clocked({ launchBudget: budget });
      world.modelCause = "stopped";
      world.relaunchResult = resumeFailed;
      const old = new Date(T0 - 3_600_000).toISOString();
      world.rows.set("q-1", { destination: MODEL.sessionName, waitingSince: old });
      world.rows.set("q-2", { destination: MODEL.sessionName, waitingSince: old });
      const tick = async () => {
        budget.nextTick();
        const planned = planSeatPool(jobSpec, world.state(slots, executor, jobSpec), clock, 120_000);
        await run(executor, planned.intents);
        return planned;
      };
      expect((await tick()).intents.map((intent) => intent.kind)).toEqual(["relaunch", "add"]);
      // The failed relaunch took the tick's only launch: the add waits for the next tick.
      expect(world.calls.filter((call) => call.startsWith("add "))).toEqual([]);
      clock += 1_000;
      await tick();
      clock += 1_000;
      await tick();
      expect(relaunches()).toBe(1);
      expect(world.calls.filter((call) => call.startsWith("add ")).length).toBeGreaterThanOrEqual(1);
      expect(world.calls).toContain("add impl-s1 /work/r/impl-s1");
      // Held while the backoff runs, recorded once, when it starts holding.
      expect(backoffHolds()).toEqual([{ failed: false, reason: "seat_pool_held", notes: {
        reason: "relaunch_backoff", seat: MODEL.sessionName, failures: 1, retryAt: new Date(T0 + 60_000).toISOString(),
      } }]);
    });

    it("tries a failed relaunch again once its backoff passes, doubles it, and starts over once the seat stays up", async () => {
      const executor = clocked();
      world.modelCause = "stopped";
      world.relaunchResult = resumeFailed;
      await run(executor, relaunchModel);
      clock += 59_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(1);
      clock += 2_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(2);
      // The second failure in a row waits 120 s.
      clock += 119_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(2);
      clock += 2_000;
      world.relaunchResult = { ok: true };
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(3);
      // The next plan has no relaunch for it: it stayed up, and its count starts over.
      clock += 1_000;
      await run(executor, []);
      world.modelCause = "stopped";
      world.relaunchResult = resumeFailed;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(4);
      clock += 61_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(5);
    });

    it("counts a relaunch whose seat is stopped again at the next plan as one that failed", async () => {
      const executor = clocked();
      world.modelCause = "stopped";
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(1);
      world.modelCause = "stopped";
      clock += 1_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(1);
      expect(backoffHolds().at(-1)?.notes).toMatchObject({ seat: MODEL.sessionName, failures: 1 });
    });

    it("counts a relaunch past the effect timeout as failed: the next plan waits out the backoff", async () => {
      // The launch settles only after the run stopped waiting for it (5 ms), so the pool is free again once it does.
      const executor: SeatPoolExecutor = new SeatPoolExecutor({
        slots, readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
        readState: () => world.state(slots, executor, jobSpec), record: (_j, e) => { history.push(e); }, now: () => new Date(clock),
        effects: { ...world.effects(), relaunchSeat: async (input) => {
          world.calls.push(`relaunch ${input.logicalId}`);
          return new Promise((resolve) => setTimeout(() => resolve({ ok: true }), 30));
        } },
        effectTimeoutMs: 5,
      });
      world.modelCause = "stopped";
      await run(executor, relaunchModel);
      expect(history.at(-1)).toMatchObject({ reason: "seat_pool_relaunch_failed", notes: { code: "effect_timeout" } });
      world.modelCause = "stopped";
      clock += 1_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(1);
      expect(backoffHolds().at(-1)?.notes).toMatchObject({ failures: 1 });
    });

    it("never backs off a relaunch of the reap (a refused release), which the reap waits on", async () => {
      jobSpec = spec({ release_cmd: { argv: ["release"] } });
      world.exit["release"] = 1;
      world.relaunchResult = resumeFailed;
      world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "stopped", sessionId: "sess-1", openRows: 0, idle: false });
      const dead = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
      await run(clocked(), [{ kind: "reap", slotId: dead.slotId }]);
      // The refused release launches it again; that launch failed, so the reap goes on without asking again.
      expect(relaunches()).toBe(1);
      expect(slots.get(dead.slotId)?.retiredAt).not.toBeNull();
      expect(backoffHolds()).toEqual([]);
    });

    // T-005232 S4 (mutante m1 do QA-f6a): the backoff doubles from 60 s and stops at 30 min.
    it("S4: six relaunch failures in a row wait 30 min for the next try, not 32 (the ceiling), with no escalation nor failed hold meanwhile", async () => {
      const executor = clocked();
      world.modelCause = "stopped";
      world.relaunchResult = resumeFailed;
      // Failures 1 to 5 wait 60, 120, 240, 480 and 960 s; each try runs just past the wait.
      await run(executor, relaunchModel);
      for (const waitS of [60, 120, 240, 480, 960]) {
        clock += (waitS + 1) * 1_000;
        await run(executor, relaunchModel);
      }
      expect(relaunches()).toBe(6);
      const sixth = clock;
      clock += 1_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(6);
      expect(backoffHolds().at(-1)?.notes).toMatchObject({ failures: 6, retryAt: new Date(sixth + 30 * 60_000).toISOString() });
      // N12: while it waits, no escalation and no hold recorded as a failure.
      expect(world.calls.filter((call) => call.startsWith("escalate"))).toEqual([]);
      expect(history.filter((h) => h.reason === "seat_pool_held" && h.failed)).toEqual([]);
      clock = sixth + 30 * 60_000 + 1_000;
      await run(executor, relaunchModel);
      expect(relaunches()).toBe(7);
    });

    // T-005232 S2: a relaunch that throws (the delivery guard's lifecycle, a database error) is a failed relaunch: it
    // waits out its backoff, and the rest of the plan (the add) still runs.
    it("S2: a relaunch that throws counts as failed and backs off: the plan goes on, and the add starts at the 2nd tick", async () => {
      const budget = budgetOf(1);
      const executor = clocked({ launchBudget: budget, effects: { ...world.effects(), relaunchSeat: async (input) => {
        world.calls.push(`relaunch ${input.logicalId}`);
        throw new Error("draft_aware_lifecycle");
      } } });
      world.modelCause = "stopped";
      const old = new Date(T0 - 3_600_000).toISOString();
      world.rows.set("q-1", { destination: MODEL.sessionName, waitingSince: old });
      world.rows.set("q-2", { destination: MODEL.sessionName, waitingSince: old });
      const tick = async () => {
        budget.nextTick();
        const planned = planSeatPool(jobSpec, world.state(slots, executor, jobSpec), clock, 120_000);
        await run(executor, planned.intents);
        return planned;
      };
      expect((await tick()).intents.map((intent) => intent.kind)).toEqual(["relaunch", "add"]);
      expect(history.find((h) => h.reason === "seat_pool_relaunch_failed")).toMatchObject({
        failed: true, notes: { seat: MODEL.sessionName, code: "relaunch_threw", error: "draft_aware_lifecycle" },
      });
      expect(reasons()).not.toContain("seat_pool_run_failed");
      expect(world.calls.filter((call) => call.startsWith("add "))).toEqual([]);
      clock += 1_000;
      await tick();
      expect(world.calls).toContain("add impl-s1 /work/r/impl-s1");
      clock += 1_000;
      await tick();
      expect(relaunches()).toBe(1);
      expect(reasons()).not.toContain("seat_pool_run_failed");
      expect(backoffHolds().at(-1)?.notes).toMatchObject({ failures: 1, retryAt: new Date(T0 + 60_000).toISOString() });
    });

    it("an add the max refuses during the usage check spends none of the tick's launches", async () => {
      const budget = budgetOf(1);
      const executor = clocked({ launchBudget: budget, outOfUsage: async () => { jobSpec = spec({ max: 0 }); return false; } });
      await run(executor, [{ kind: "add", count: 1 }]);
      expect(world.calls).toEqual([]);
      expect(budget.currentTick().admitted).toBe(0);
      expect(history).toEqual([{ failed: false, reason: "seat_pool_held", notes: { reason: "max_reached", max: 0, seats: 0, add: 1 } }]);
      // The tick still has its launch for someone else.
      const room = budget.reserve();
      expect(room.ok).toBe(true);
    });

    describe("with the real queue", () => {
      let repo: QueueRepository;
      let queueEffects: ReturnType<typeof seatPoolQueueEffects>;
      let escalatesTo: string;
      beforeEach(() => {
        // The shared fixture leaves out the outbox (migration-fixture-parity): the wakes a reroute stages need it.
        migrate(db, [inboxEntriesSchema, outboxEntriesSchema, i3IdentityProvenanceSchema]);
        repo = new QueueRepository(db, new EventBus(db));
        repo.attachOutbox(new OutboxHandler(db));
        escalatesTo = "coordenacao-orquestradora@comando";
        queueEffects = seatPoolQueueEffects({ queueRepo: repo, escalationOwner: () => ({ owner: escalatesTo, reason: "routed" }) });
      });
      const withQueue = (): SeatPoolEffects => ({ ...world.effects(), ...queueEffects });
      const deadSeat = () => {
        world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "stopped", sessionId: "sess-1", openRows: 1, idle: false });
        return slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
      };
      const parked = async (blockedOn: string, wakeAfterSeconds?: number) => {
        const row = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl-s1@r", body: "work", nudge: false });
        repo.update({ qitemId: row.qitemId, actorSession: "dev-impl-s1@r", state: "blocked", blockedOn, ...(wakeAfterSeconds ? { wakeAfterSeconds } : {}) });
        return row.qitemId;
      };
      const fallbackWakes = (qitemId: string) =>
        db.prepare("SELECT destination_session FROM outbox_entries WHERE audit_pointer = ? AND outbox_id LIKE 'wake-intent-fallback-%'").all(qitemId);
      const armedTimer = (qitemId: string) => {
        const wake = new QueueWakeRepository(db).getStatus(qitemId);
        if (wake?.kind !== "timer" || !wake.live) return null;
        const job = new WatchdogJobsRepository(db).getById(wake.ref);
        return job ? { target: job.targetSession, interval: job.intervalSeconds, state: job.state } : null;
      };

      it("a reaped seat's row parked with a --wake-after timer reaches the model still parked, with the timer aimed at the model", async () => {
        const qitemId = await parked("external:ci", 600);
        expect(armedTimer(qitemId)).toMatchObject({ target: "dev-impl-s1@r", interval: 600 });
        const dead = deadSeat();
        await run(clocked({ effects: withQueue() }), [{ kind: "reap", slotId: dead.slotId }]);
        expect(repo.getById(qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "blocked", blockedOn: "external:ci" });
        expect(armedTimer(qitemId)).toEqual({ target: MODEL.sessionName, interval: 600, state: "active" });
        expect(fallbackWakes(qitemId)).toEqual([]);
      });

      it("a reaped seat's row blocked on a live row stays blocked on it, with no wake: the blocker wakes it", async () => {
        const blocker = await repo.create({ sourceSession: "lead@r", destinationSession: "qa@r", body: "check", nudge: false });
        const qitemId = await parked(blocker.qitemId);
        const dead = deadSeat();
        await run(clocked({ effects: withQueue() }), [{ kind: "reap", slotId: dead.slotId }]);
        expect(repo.getById(qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "blocked", blockedOn: blocker.qitemId });
        expect(fallbackWakes(qitemId)).toEqual([]);
      });

      it("a reaped seat's row parked on a gate with nothing to wake it goes to the model pending, with a wake and the old blocker in the note", async () => {
        const qitemId = await parked("external:deploy");
        const dead = deadSeat();
        await run(clocked({ effects: withQueue() }), [{ kind: "reap", slotId: dead.slotId }]);
        expect(repo.getById(qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending", blockedOn: null });
        expect(fallbackWakes(qitemId)).toEqual([{ destination_session: MODEL.sessionName }]);
        expect(repo.listTransitions(qitemId).map((t) => t.transitionNote ?? "").join("\n")).toContain("external:deploy");
        expect(history.at(-1)).toMatchObject({ reason: "seat_pool_reaped" });
      });

      it("the spec's escalation_owner takes a seat's needs-input escalation, and nothing reaches the model's escalates_to", async () => {
        jobSpec = spec({ escalation_owner: "plataforma-supervisor@comando" });
        world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "running", sessionId: "sess-1", openRows: 1, idle: false, cause: "needs_input" });
        slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
        await run(clocked({ effects: withQueue() }), [{ kind: "escalate", seat: "dev-impl-s1@r", qitemIds: ["q-1"] }]);
        expect(repo.list({ destinationSession: "plataforma-supervisor@comando" }).map((row) => row.tags)).toEqual([[`${SEAT_POOL_NEEDS_INPUT_TAG}:dev-impl-s1@r`]]);
        expect(repo.list({ destinationSession: escalatesTo })).toEqual([]);
        expect(db.prepare("SELECT COUNT(*) AS n FROM outbox_entries WHERE destination_session = ?").get(escalatesTo)).toEqual({ n: 0 });
        expect(history.at(-1)).toMatchObject({ reason: "seat_pool_escalated", notes: { to: "plataforma-supervisor@comando" } });
        // T-005232 S1 (mutante m2 do QA-f6a): the escalation keeps its nudge: the supervisor's wake is staged.
        const [escalation] = repo.list({ destinationSession: "plataforma-supervisor@comando" });
        expect(db.prepare("SELECT COUNT(*) AS n FROM outbox_entries WHERE destination_session = ? AND audit_pointer = ?")
          .get("plataforma-supervisor@comando", escalation!.qitemId)).toEqual({ n: 1 });
      });

      // T-005232 S3 (mutante m3 do QA-f6a): an escalation_owner the queue refuses never falls back to the model's
      // escalates_to (the orchestrator's terminal): the escalation fails, owned by no one, and a row the reap moves a
      // second time stays at the model.
      it("S3: an escalation_owner the queue refuses leaves the escalation without owner, and nothing reaches escalates_to", async () => {
        jobSpec = spec({ escalation_owner: "plataforma-supervisor@comando" });
        const refusing = new QueueRepository(db, new EventBus(db), { validateRig: (ref) => !ref.startsWith("plataforma-supervisor@") });
        refusing.attachOutbox(new OutboxHandler(db));
        const effects = (): SeatPoolEffects => ({ ...world.effects(), ...seatPoolQueueEffects({ queueRepo: refusing, escalationOwner: () => ({ owner: escalatesTo, reason: "routed" }) }) });
        world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "running", sessionId: "sess-1", openRows: 1, idle: false, cause: "needs_input" });
        slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
        await run(clocked({ effects: effects() }), [{ kind: "escalate", seat: "dev-impl-s1@r", qitemIds: ["q-1"] }]);
        expect(history.at(-1)).toMatchObject({ failed: true, reason: "seat_pool_escalation_failed", notes: { seat: "dev-impl-s1@r" } });
        expect(refusing.list({ destinationSession: escalatesTo })).toEqual([]);
        expect(db.prepare("SELECT COUNT(*) AS n FROM outbox_entries WHERE destination_session = ?").get(escalatesTo)).toEqual({ n: 0 });

        // The 2nd reap of a row: with the override refused, it stays at the model, never at escalates_to.
        world.nodes.delete("node-1");
        const row = await refusing.create({ sourceSession: "lead@r", destinationSession: MODEL.sessionName, body: "work", nudge: false });
        refusing.routeToFallback(row.qitemId, "dev-impl-s2@r", "reap:dev-impl-s9@r");
        world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "stopped", sessionId: "sess-2", openRows: 1, idle: false });
        const dead = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "live", intent: null, opId: "op-2", at: new Date(T0).toISOString() });
        await run(clocked({ effects: effects() }), [{ kind: "reap", slotId: dead.slotId }]);
        expect(refusing.getById(row.qitemId)).toMatchObject({ destinationSession: MODEL.sessionName });
        expect(refusing.list({ destinationSession: escalatesTo })).toEqual([]);
        expect(db.prepare("SELECT COUNT(*) AS n FROM outbox_entries WHERE destination_session = ?").get(escalatesTo)).toEqual({ n: 0 });
      });

      it("a row the reap moves a second time goes to the spec's escalation_owner, never to the model's escalates_to", async () => {
        jobSpec = spec({ escalation_owner: "plataforma-supervisor@comando" });
        const row = await repo.create({ sourceSession: "lead@r", destinationSession: MODEL.sessionName, body: "work", nudge: false });
        repo.routeToFallback(row.qitemId, "dev-impl-s1@r", "reap:dev-impl-s9@r");
        const dead = deadSeat();
        await run(clocked({ effects: withQueue() }), [{ kind: "reap", slotId: dead.slotId }]);
        expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: "plataforma-supervisor@comando", state: "pending" });
        expect(fallbackWakes(row.qitemId)).toEqual([{ destination_session: "plataforma-supervisor@comando" }]);
        expect(db.prepare("SELECT COUNT(*) AS n FROM outbox_entries WHERE destination_session = ?").get(escalatesTo)).toEqual({ n: 0 });
      });

      it("without escalation_owner, escalations go to the model's escalates_to as before", async () => {
        world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "running", sessionId: "sess-1", openRows: 1, idle: false, cause: "needs_input" });
        slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
        escalatesTo = "lead@r";
        await run(clocked({ effects: withQueue() }), [{ kind: "escalate", seat: "dev-impl-s1@r", qitemIds: ["q-1"] }]);
        expect(repo.list({ destinationSession: "lead@r" })).toHaveLength(1);
      });
    });
  });

  // T-004870 F3, volta 3 (REVISAO-f3 "Volta 2"): the forced pass at a drain's deadline.
  describe("F3 v3: the forced pass reads the seat again after its release, and a seat stopped stays out of admission", () => {
    let clock: number;
    let repo: QueueRepository;
    const SEAT = "dev-impl-s1@r";
    const DEADLINE_MS = 900_000;
    const executorWith = (effects: SeatPoolEffects): SeatPoolExecutor => {
      const executor: SeatPoolExecutor = new SeatPoolExecutor({
        slots,
        readJob: () => ({ active, targetSession: MODEL.sessionName, spec: jobSpec }),
        readState: () => world.state(slots, executor, jobSpec),
        effects,
        record: (_jobId, entry) => { history.push(entry); },
        now: () => new Date(clock),
      });
      return executor;
    };
    const withQueue = (): SeatPoolEffects => ({ ...world.effects(), ...seatPoolQueueEffects({ queueRepo: repo, escalationOwner: () => ({ owner: "lead@r", reason: "routed" }) }) });
    const liveSeat = (session: FakeNode["session"] = "running", phase: "live" | "draining" = "live", intent: string | null = null) => {
      world.nodes.set("node-1", { logicalId: "dev.impl-s1", session, sessionId: "sess-1", openRows: 1, idle: false });
      return slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase, intent, opId: "op-1", at: new Date(T0).toISOString() });
    };
    /** The drain starts at T0 (one run), and the next run is past its deadline (the forced pass). */
    const toDeadline = async (executor: SeatPoolExecutor) => {
      await run(executor, []);
      expect(history.at(-1)).toMatchObject({ reason: "seat_pool_drain_started" });
      clock = T0 + DEADLINE_MS + 1_000;
      await run(executor, []);
    };
    const expired = () => history.filter((h) => h.reason === "seat_pool_drain_expired");
    const retrying = () => history.filter((h) => h.reason === "seat_pool_drain_retrying");
    beforeEach(() => {
      clock = T0;
      migrate(db, [inboxEntriesSchema, outboxEntriesSchema, i3IdentityProvenanceSchema]);
      repo = new QueueRepository(db, new EventBus(db));
      repo.attachOutbox(new OutboxHandler(db));
      jobSpec = spec({ drain: true, release_cmd: { argv: ["release"] } });
    });

    // B1: the lifecycle lease holds deliveries back, not the claim. A turn already under way when the lease began claims
    // a pending row of the seat while the release command (drain-deadline) runs: the seat is working again.
    it("B1: a row the seat claims while the release command runs keeps the seat running: no stop, no unclaim, the row stays", async () => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      const slot = liveSeat();
      world.commandGate = async () => {
        if (world.calls.at(-1) === "run release") repo.claim({ qitemId: row.qitemId, destinationSession: SEAT });
      };
      await toDeadline(executorWith(withQueue()));
      expect(world.env.at(-1)).toMatchObject({ OPENRIG_SEAT_POOL_RELEASE_REASON: "drain-deadline", OPENRIG_SEAT_POOL_SLOT: SEAT });
      expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("remove") || c.startsWith("unclaim"))).toEqual([]);
      expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: SEAT, state: "in-progress" });
      expect(world.nodes.get("node-1")?.session).toBe("running");
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      // T-005250 S11: asked again until the retry window closes, not left for good at once.
      expect(expired()).toEqual([]);
      expect(retrying().at(-1)?.notes).toMatchObject({ left: [], moved: [], waiting: [{ slot: "dev.impl-s1", exitCode: 0, why: "work_arrived" }] });
    });

    it("B1: a row claimed before the release, that the release command lets go, is still unclaimed and moved (the claim is not new)", async () => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      repo.claim({ qitemId: row.qitemId, destinationSession: SEAT });
      const slot = liveSeat();
      await toDeadline(executorWith(withQueue()));
      expect(world.calls.filter((c) => c.startsWith("stop"))).toHaveLength(1);
      expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending" });
      expect(slots.get(slot.slotId)?.retiredAt).not.toBeNull();
      expect(expired()).toEqual([expect.objectContaining({ failed: false, notes: expect.objectContaining({ left: [], moved: [{ qitemId: row.qitemId, to: MODEL.sessionName }] }) })]);
    });

    it("B1: a row that only arrives (pending, unclaimed) while the release command runs goes to the model with the others", async () => {
      const first = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      const slot = liveSeat();
      let arrived: string | null = null;
      world.commandGate = async () => {
        if (world.calls.at(-1) === "run release") arrived = (await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "more", nudge: false })).qitemId;
      };
      await toDeadline(executorWith(withQueue()));
      expect(world.calls.filter((c) => c.startsWith("stop"))).toHaveLength(1);
      expect(repo.getById(first.qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending" });
      expect(repo.getById(arrived!)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending" });
      expect(slots.get(slot.slotId)?.retiredAt).not.toBeNull();
    });

    // S1: a seat the pass already stopped (intent stop, its session gone) whose stop fails again is not put back in
    // admission (phase live), where the reap would launch it: it stays draining with its intent, listed.
    it("S1: a resumed seat (stopped, intent stop) whose stop fails stays draining with its intent, never live", async () => {
      const slot = liveSeat("stopped", "draining", "stop");
      world.stopResult = { ok: false, code: "stop_uncertain", error: "Checked stop did not complete" };
      await toDeadline(executorWith(world.effects()));
      expect(world.calls.filter((c) => c.startsWith("run release"))).toEqual([]);
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "draining", intent: "stop", retiredAt: null });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "stop_failed", exitCode: null, stopped: true }] }) })]);
    });

    it("S1: a stop that fails after the kill (the session gone) leaves the seat draining with its intent, never live", async () => {
      const slot = liveSeat();
      await toDeadline(executorWith({
        ...world.effects(),
        stopSlot: async (input) => {
          world.calls.push(`stop ${input.sessionName}`);
          world.nodes.get(input.expected.nodeId)!.session = "stopped";
          return { ok: false, code: "stop_uncertain", error: "Session kill completed, but stop finalization failed" };
        },
      }));
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "draining", intent: "stop", retiredAt: null });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "stop_failed", exitCode: 0, stopped: true }] }) })]);
    });

    // T-005250 S12: a stop that tried the kill and failed to record it, its session found gone by tmux (the clean), has
    // the proof a session_not_live stop gives: the pass carries on, its rows to the model and the node removed.
    for (const clean of [{ ok: true } as const, { ok: false, code: "nothing_to_clean", error: "already clean" } as const]) {
      it(`S12: a kill_attempted stop whose session tmux finds gone (${clean.ok ? "cleaned" : clean.code}) moves its rows and removes the seat`, async () => {
        const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
        repo.claim({ qitemId: row.qitemId, destinationSession: SEAT });
        const slot = liveSeat();
        world.cleanResult = clean;
        await toDeadline(executorWith({
          ...withQueue(),
          // As the daemon's stopSeat answers when the kill ran but the stop could not be persisted: the session's record
          // (what sessionRunning reads) still says running.
          stopSlot: async (input) => {
            world.calls.push(`stop ${input.sessionName}`);
            return { ok: false, code: "stop_uncertain", error: "Session kill completed, but stop state could not be persisted", killAttempted: true };
          },
        }));
        expect(world.calls).toContain(`clean ${SEAT}`);
        expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending" });
        expect(world.calls.some((c) => c.startsWith("remove dev.impl-s1"))).toBe(true);
        expect(slots.get(slot.slotId)?.retiredAt).not.toBeNull();
        expect(expired()).toEqual([expect.objectContaining({ failed: false, notes: expect.objectContaining({ left: [], moved: [{ qitemId: row.qitemId, to: MODEL.sessionName }] }) })]);
      });
    }

    it("S12: a stop that failed at its finalization (kill_attempted, the record already exited) moves the rows once tmux finds the session gone", async () => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      const slot = liveSeat();
      await toDeadline(executorWith({
        ...withQueue(),
        // As stopSeat answers when the kill and the stop's record went through and only the finalization failed.
        stopSlot: async (input) => {
          world.calls.push(`stop ${input.sessionName}`);
          world.nodes.get(input.expected.nodeId)!.session = "stopped";
          return { ok: false, code: "stop_uncertain", error: "Session kill completed, but stop finalization failed", killAttempted: true };
        },
      }));
      expect(world.calls).toContain(`clean ${SEAT}`);
      expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending" });
      expect(slots.get(slot.slotId)?.retiredAt).not.toBeNull();
      expect(expired()).toEqual([expect.objectContaining({ failed: false, notes: expect.objectContaining({ left: [], moved: [{ qitemId: row.qitemId, to: MODEL.sessionName }] }) })]);
    });

    it("S12: a kill_attempted stop with the record exited whose clean cannot tell (probe failed) stays draining, its rows unmoved", async () => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      const slot = liveSeat();
      world.cleanResult = { ok: false, code: "probe_failed", error: "tmux did not answer" };
      await toDeadline(executorWith({
        ...withQueue(),
        stopSlot: async (input) => {
          world.calls.push(`stop ${input.sessionName}`);
          world.nodes.get(input.expected.nodeId)!.session = "stopped";
          return { ok: false, code: "stop_uncertain", error: "Session kill completed, but stop finalization failed", killAttempted: true };
        },
      }));
      expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: SEAT, state: "pending" });
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "draining", intent: "stop", retiredAt: null });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "stop_failed", exitCode: 0, stopped: true }] }) })]);
    });

    it("S12: a kill_attempted stop whose clean fails on the probe (session state unknown) leaves the seat running, back in admission", async () => {
      const slot = liveSeat();
      world.cleanResult = { ok: false, code: "probe_failed", error: "tmux did not answer" };
      await toDeadline(executorWith({
        ...world.effects(),
        stopSlot: async (input) => {
          world.calls.push(`stop ${input.sessionName}`);
          return { ok: false, code: "stop_uncertain", error: "kill-session failed", killAttempted: true };
        },
      }));
      expect(world.calls.filter((c) => c.startsWith("remove") || c.startsWith("move"))).toEqual([]);
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "stop_failed", exitCode: 0 }] }) })]);
    });

    it("S1: a stop refused with the session still running leaves the seat untouched, back in admission (as before)", async () => {
      const slot = liveSeat();
      world.stopResult = { ok: false, code: "selection_changed", error: "the seat was relaunched" };
      await toDeadline(executorWith(world.effects()));
      expect(world.nodes.get("node-1")?.session).toBe("running");
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "stop_failed", exitCode: 0 }] }) })]);
    });

    // S2: the guard that keeps a seat stopped on its way out for the forced pass holds only while that pass is still to
    // run: once the drain expired, the pass never runs again, and the guard would hold the seat draining for ever.
    it("S2: after the drain expired, a seat stopped on its way out is no longer held for the forced pass", async () => {
      jobSpec = spec({ drain: true });
      const holder = liveSeat("running");
      world.exit["release"] = 0;
      const executor = executorWith(world.effects());
      // A seat the drain keeps (it holds a row) starts the clock; at the deadline, the pass takes it out (no release_cmd).
      await toDeadline(executor);
      expect(expired()).toHaveLength(1);
      expect(slots.get(holder.slotId)?.retiredAt).not.toBeNull();
      // Since: a seat the plain drain stopped and a restart cut short before its removal (intent stop, session gone).
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "stopped", sessionId: "sess-2", openRows: 0, idle: false });
      const cut = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "draining", intent: "stop", opId: "op-2", at: new Date(T0).toISOString() });
      world.calls = [];
      await run(executor, [drainOf(cut.slotId)]);
      expect(world.calls).toContain("lease dev.impl-s2");
      expect(slots.get(cut.slotId)?.phase).not.toBe("draining");
    });

    it("S2: after the drain expired, a seat the pass left stopped (listed) is still held: never back in admission", async () => {
      const slot = liveSeat();
      const executor = executorWith({
        ...world.effects(),
        stopSlot: async (input) => {
          world.calls.push(`stop ${input.sessionName}`);
          world.nodes.get(input.expected.nodeId)!.session = "stopped";
          return { ok: false, code: "stop_uncertain", error: "Session kill completed, but stop finalization failed" };
        },
      });
      await toDeadline(executor);
      expect(expired()).toHaveLength(1);
      world.calls = [];
      await run(executor, [drainOf(slot.slotId)]);
      expect(world.calls).toEqual([]);
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "draining", intent: "stop", retiredAt: null });
    });

    it("S2: after the drain expired, a seat the pass left running (release_refused), stopped since by the plain drain and cut short, is not held", async () => {
      const slot = liveSeat();
      world.exit["release"] = 1;
      const executor = executorWith(world.effects());
      await toDeadline(executor);
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "release_refused", exitCode: 1 }] }) })]);
      // Since: the seat got free, the plain drain stopped it, and a restart cut it short before its removal.
      slots.update(slot.slotId, { phase: "draining", intent: "stop" });
      world.nodes.get("node-1")!.session = "stopped";
      world.calls = [];
      await run(executor, [drainOf(slot.slotId)]);
      expect(world.calls).toContain("lease dev.impl-s1");
      expect(slots.get(slot.slotId)?.phase).not.toBe("draining");
    });

    // T-005245 S7: the seat worked on its rows while the release command ran, in a way the B1 check (a new
    // in-progress id) missed: the same row released and claimed again, a claim and then a block or a close, a pending
    // row parked straight away. Each keeps the seat running: no stop, no unclaim, work_arrived (asked again, T-005250 S11).
    const workedDuringRelease = async (before: (id: string) => void, during: (id: string) => Promise<void> | void) => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      before(row.qitemId);
      const slot = liveSeat();
      world.commandGate = async () => { if (world.calls.at(-1) === "run release") await during(row.qitemId); };
      await toDeadline(executorWith(withQueue()));
      expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("remove") || c.startsWith("unclaim"))).toEqual([]);
      expect(world.nodes.get("node-1")?.session).toBe("running");
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(expired()).toEqual([]);
      expect(retrying().at(-1)?.notes).toMatchObject({ left: [], moved: [], waiting: [{ slot: "dev.impl-s1", exitCode: 0, why: "work_arrived" }] });
      return row.qitemId;
    };
    it("S7: the same row released and claimed again during the release keeps the seat running", async () => {
      const id = await workedDuringRelease(
        (qitemId) => { repo.claim({ qitemId, destinationSession: SEAT }); },
        async (qitemId) => {
          repo.unclaim(qitemId, SEAT, "manual");
          await new Promise((resolve) => setTimeout(resolve, 5));
          repo.claim({ qitemId, destinationSession: SEAT });
        });
      expect(repo.getById(id)).toMatchObject({ destinationSession: SEAT, state: "in-progress" });
    });
    it("S7: a row claimed and then parked during the release keeps the seat running, the row still blocked there", async () => {
      const id = await workedDuringRelease(() => {}, (qitemId) => {
        repo.claim({ qitemId, destinationSession: SEAT });
        repo.update({ qitemId, actorSession: SEAT, state: "blocked", blockedOn: "external:suite" });
      });
      expect(repo.getById(id)).toMatchObject({ destinationSession: SEAT, state: "blocked" });
    });
    it("S7: a row claimed and then closed during the release keeps the seat running", async () => {
      const id = await workedDuringRelease(() => {}, (qitemId) => {
        repo.claim({ qitemId, destinationSession: SEAT });
        repo.update({ qitemId, actorSession: SEAT, state: "done", closureReason: "no-follow-on" });
      });
      expect(repo.getById(id)).toMatchObject({ state: "done" });
    });
    it("S7: a pending row parked straight away during the release keeps the seat running", async () => {
      const id = await workedDuringRelease(() => {}, (qitemId) => {
        repo.update({ qitemId, actorSession: SEAT, state: "blocked", blockedOn: "external:suite" });
      });
      expect(repo.getById(id)).toMatchObject({ destinationSession: SEAT, state: "blocked" });
    });

    // T-005245 S8: kill_attempted also comes out of a kill-session that failed with the session alive. tmux decides:
    // a session the clean finds alive is not a seat stopped, and goes back to admission (never held draining).
    it("S8: a kill_attempted stop whose session tmux finds alive leaves the seat running, back in admission, not stopped", async () => {
      const slot = liveSeat();
      world.cleanResult = { ok: false, code: "session_alive", error: "Session is alive in tmux" };
      await toDeadline(executorWith({
        ...world.effects(),
        stopSlot: async (input) => {
          world.calls.push(`stop ${input.sessionName}`);
          return { ok: false, code: "stop_uncertain", error: "kill-session failed", killAttempted: true };
        },
      }));
      expect(world.calls).toContain(`clean ${SEAT}`);
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "stop_failed", exitCode: 0 }] }) })]);
    });

    // T-005245 S9: a seat the plain drain stopped and a restart cut short before its removal carries on to the removal,
    // never back to live (where the reap would launch it again), also after the forced pass.
    it("S9: after the drain expired, a seat the plain drain stopped (its removal cut short) is removed, never relaunched", async () => {
      jobSpec = spec({ drain: true });
      liveSeat("running");
      const executor = executorWith(world.effects());
      await toDeadline(executor);
      expect(expired()).toHaveLength(1);
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "stopped", sessionId: "sess-2", openRows: 0, idle: false });
      const cut = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "draining", intent: "stop", opId: "op-2", at: new Date(T0).toISOString() });
      world.calls = [];
      // The plans that follow: the planner's own intents, tick after tick.
      for (let i = 0; i < 3; i++) {
        clock += 30_000;
        await run(executor, planSeatPool(jobSpec, world.state(slots, executor, jobSpec), clock, 120_000).intents);
      }
      expect(world.calls.filter((c) => c.startsWith("relaunch") || c.startsWith("run release") || c.startsWith("stop"))).toEqual([]);
      expect(world.calls).toContain("remove dev.impl-s2 " + JSON.stringify({ nodeId: "node-2", sessionId: "sess-2" }));
      expect(slots.get(cut.slotId)?.retiredAt).not.toBeNull();
      expect(history).toContainEqual(expect.objectContaining({ reason: "seat_pool_removed", notes: { slot: "dev.impl-s2", resumed: true } }));
    });

    it("S9: a seat the plain drain stopped that a row reached since goes back to admission (the reap gives the row back)", async () => {
      jobSpec = spec({ drain: false });
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "stopped", sessionId: "sess-2", openRows: 1, idle: false });
      const cut = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "draining", intent: "remove", opId: "op-2", at: new Date(T0).toISOString() });
      await run(executorWith(world.effects()), [drainOf(cut.slotId)]);
      expect(world.calls.filter((c) => c.startsWith("remove"))).toEqual([]);
      expect(slots.get(cut.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
    });

    // T-005245 S4: the delivery guard refusing the lease (a typing guard on hold, a draft-aware seat with input) is that
    // seat's result: the forced pass leaves it (delivery_guard) and goes on with the others; the run never fails.
    it("S4: a lease the delivery guard refuses leaves that seat in the forced pass, and the pass goes on with the next", async () => {
      jobSpec = spec({ drain: true });
      const guarded = liveSeat();
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "running", sessionId: "sess-2", openRows: 1, idle: false });
      const other = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "live", intent: null, opId: "op-2", at: new Date(T0).toISOString() });
      const effects = world.effects();
      await toDeadline(executorWith({
        ...effects,
        lifecycle: async (nodeId, fn) => {
          if (nodeId === "node-1") throw Object.assign(new Error("Automatic input is paused for this seat."), { code: "typing_guard_enabled" });
          return effects.lifecycle(nodeId, fn);
        },
      }));
      expect(history.map((h) => h.reason)).not.toContain("seat_pool_run_failed");
      expect(slots.get(guarded.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(slots.get(other.slotId)?.retiredAt).not.toBeNull();
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "delivery_guard", exitCode: null }] }) })]);
    });

    it("S4: the plain drain and the reap take a guard refusal as that seat's result, and the run goes on", async () => {
      jobSpec = spec({});
      const effects = world.effects();
      const refusing: SeatPoolEffects = { ...effects, lifecycle: async () => { throw Object.assign(new Error("draft-aware seat"), { code: "draft_aware_lifecycle" }); } };
      world.nodes.set("node-1", { logicalId: "dev.impl-s1", session: "running", sessionId: "sess-1", openRows: 0, idle: true });
      const free = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: "node-1", phase: "live", intent: null, opId: "op-1", at: new Date(T0).toISOString() });
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "stopped", sessionId: "sess-2", openRows: 0, idle: false });
      const dead = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "live", intent: null, opId: "op-2", at: new Date(T0).toISOString() });
      await run(executorWith(refusing), [drainOf(free.slotId), { kind: "reap", slotId: dead.slotId }]);
      expect(reasons()).not.toContain("seat_pool_run_failed");
      expect(slots.get(free.slotId)).toMatchObject({ phase: "live", intent: null });
      expect(history).toContainEqual({ failed: false, reason: "seat_pool_returned", notes: { slot: "dev.impl-s1", why: "delivery_guard", code: "draft_aware_lifecycle", error: "draft-aware seat" } });
      expect(history).toContainEqual({ failed: true, reason: "seat_pool_reap_failed", notes: { slot: "dev.impl-s2", step: "lease", code: "draft_aware_lifecycle", error: "draft-aware seat" } });
    });

    // T-005245 S6: a pass cut short records what it did so far (keep), and a seat whose release does not answer is
    // asked again only before deadline + 600 s, never at that instant.
    it("S6: a pass cut short records what it did so far (seat_pool_drain_retrying), never the pass's end", async () => {
      jobSpec = spec({ drain: true, release_cmd: { argv: ["release"] } });
      liveSeat();
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "running", sessionId: "sess-2", openRows: 1, idle: false });
      slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "live", intent: null, opId: "op-2", at: new Date(T0).toISOString() });
      world.exit["release"] = 3;
      let asked = 0;
      world.commandGate = async () => { if (world.calls.at(-1) === "run release" && ++asked === 2) active = false; };
      await toDeadline(executorWith(world.effects()));
      const retrying = history.filter((h) => h.reason === "seat_pool_drain_retrying");
      expect(retrying).toHaveLength(1);
      // The job stopped while the 2nd seat's release ran: both seats are still to ask, and the pass is not through.
      expect(retrying[0]!.notes).toMatchObject({ waiting: [{ slot: "dev.impl-s1", exitCode: 3 }, { slot: "dev.impl-s2", exitCode: 3 }] });
      expect(expired()).toEqual([]);
    });

    it("S6: a release that never answers is asked again until deadline + 600 s, and at that instant the seat is left release_unavailable", async () => {
      jobSpec = spec({ drain: true, release_cmd: { argv: ["release"] } });
      liveSeat();
      world.exit["release"] = 3;
      const executor = executorWith(world.effects());
      await toDeadline(executor);
      const releases = () => world.calls.filter((c) => c === "run release").length;
      expect(releases()).toBe(1);
      clock = T0 + DEADLINE_MS + 600_000 - 1;
      await run(executor, []);
      expect(releases()).toBe(2);
      expect(expired()).toEqual([]);
      clock = T0 + DEADLINE_MS + 600_000;
      await run(executor, []);
      expect(releases()).toBe(2);
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "release_unavailable", exitCode: 3 }] }) })]);
    });

    it("S2: before the deadline, the guard still holds a seat stopped on its way out for the forced pass", async () => {
      jobSpec = spec({ drain: true });
      liveSeat("running");
      const executor = executorWith(world.effects());
      await run(executor, []);
      expect(history.at(-1)).toMatchObject({ reason: "seat_pool_drain_started" });
      world.nodes.set("node-2", { logicalId: "dev.impl-s2", session: "stopped", sessionId: "sess-2", openRows: 0, idle: false });
      const cut = slots.insert({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: "node-2", phase: "draining", intent: "stop", opId: "op-2", at: new Date(T0).toISOString() });
      world.calls = [];
      await run(executor, [drainOf(cut.slotId)]);
      expect(world.calls).toEqual([]);
      expect(slots.get(cut.slotId)).toMatchObject({ phase: "draining", intent: "stop" });
    });

    // T-005250 S11: the picture also sees what another actor did to the seat's rows while its release command ran (an
    // auto-unpark when the blocker closes, the wake ladder's usage-limit park). Such a seat did not work: it is asked
    // again on the next run, before the retry window closes, and taken out when the command lets it go.
    const askedAgain = async (setup: () => Promise<{ rowId: string; during: () => Promise<void> }>) => {
      const { rowId, during } = await setup();
      const slot = liveSeat();
      world.commandGate = async () => { if (world.calls.at(-1) === "run release") await during(); };
      const executor = executorWith(withQueue());
      await toDeadline(executor);
      expect(world.calls.filter((c) => c.startsWith("stop"))).toEqual([]);
      expect(expired()).toEqual([]);
      expect(retrying().at(-1)?.notes).toMatchObject({ waiting: [{ slot: "dev.impl-s1", exitCode: 0, why: "work_arrived" }] });
      // The next run, inside the window: nothing moves while the command runs, and the seat goes.
      world.commandGate = null;
      world.calls = [];
      clock += 30_000;
      await run(executor, []);
      expect(world.calls).toContain("run release");
      expect(world.calls.filter((c) => c.startsWith("stop"))).toHaveLength(1);
      expect(slots.get(slot.slotId)?.retiredAt).not.toBeNull();
      expect(expired()).toEqual([expect.objectContaining({ failed: false, notes: expect.objectContaining({ left: [], moved: [{ qitemId: rowId, to: MODEL.sessionName }] }) })]);
      return rowId;
    };
    it("S11: a row auto-unparked (its blocker closed) while the release runs is asked again, and the seat goes on the next run", async () => {
      const id = await askedAgain(async () => {
        const blocker = await repo.create({ sourceSession: "lead@r", destinationSession: "lead@r", body: "blocker", nudge: false });
        const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
        await repo.update({ qitemId: row.qitemId, actorSession: SEAT, state: "blocked", blockedOn: blocker.qitemId });
        return {
          rowId: row.qitemId,
          during: async () => {
            await repo.update({ qitemId: blocker.qitemId, actorSession: "lead@r", state: "done", closureReason: "no-follow-on" });
            expect(repo.getById(row.qitemId)).toMatchObject({ state: "pending", destinationSession: SEAT });
          },
        };
      });
      expect(repo.getById(id)).toMatchObject({ destinationSession: MODEL.sessionName, state: "pending" });
    });
    it("S11: a pending row the wake ladder parks (usage limit) while the release runs is asked again, and moves blocked", async () => {
      const id = await askedAgain(async () => {
        const blocker = await repo.create({ sourceSession: "lead@r", destinationSession: "lead@r", body: "usage-limit blocker", nudge: false });
        const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
        return {
          rowId: row.qitemId,
          during: async () => {
            // As queue-wake-ladder parks a pending baton under a usage limit (its own actor, never the seat).
            await repo.update({ qitemId: row.qitemId, actorSession: "wake-ladder@system", state: "blocked", blockedOn: blocker.qitemId,
              transitionNote: "usage-limit suppressed: pool=claude reset=later; waiting on shared blocker" });
          },
        };
      });
      expect(repo.getById(id)).toMatchObject({ destinationSession: MODEL.sessionName, state: "blocked" });
    });

    it("S11: a seat still working when the retry window closes is left work_arrived, not asked again (not release_unavailable)", async () => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      const slot = liveSeat();
      world.commandGate = async () => { if (world.calls.at(-1) === "run release") repo.claim({ qitemId: row.qitemId, destinationSession: SEAT }); };
      const executor = executorWith(withQueue());
      await toDeadline(executor);
      world.commandGate = null;
      world.calls = [];
      clock = T0 + DEADLINE_MS + 600_000;
      await run(executor, []);
      expect(world.calls.filter((c) => c.startsWith("run release") || c.startsWith("stop") || c.startsWith("unclaim"))).toEqual([]);
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", retiredAt: null });
      expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: SEAT, state: "in-progress" });
      expect(expired()).toEqual([expect.objectContaining({ failed: true, notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "work_arrived", exitCode: 0 }], moved: [] }) })]);
    });

    it("S11: a seat asked again while it still holds its claim, whose release command refuses, is left release_refused with its row", async () => {
      const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      liveSeat();
      world.commandGate = async () => { if (world.calls.at(-1) === "run release") repo.claim({ qitemId: row.qitemId, destinationSession: SEAT }); };
      const executor = executorWith(withQueue());
      await toDeadline(executor);
      world.commandGate = null;
      world.exit["release"] = 1;
      clock += 30_000;
      await run(executor, []);
      expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("unclaim"))).toEqual([]);
      expect(repo.getById(row.qitemId)).toMatchObject({ destinationSession: SEAT, state: "in-progress" });
      expect(expired()).toEqual([expect.objectContaining({ notes: expect.objectContaining({ left: [{ slot: "dev.impl-s1", why: "release_refused", exitCode: 1 }] }) })]);
    });

    // T-005250 S10: a row that arrives, is claimed and is closed (or handed off) while the release command runs is in
    // neither picture of the seat's open rows; the queue's transitions say the seat itself moved it: the seat worked.
    it("S10: a new row the seat claims and closes while the release runs keeps the seat running (asked again)", async () => {
      const slot = liveSeat();
      let arrived: string | null = null;
      world.commandGate = async () => {
        if (world.calls.at(-1) !== "run release") return;
        arrived = (await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "quick", nudge: false })).qitemId;
        repo.claim({ qitemId: arrived, destinationSession: SEAT });
        await repo.update({ qitemId: arrived, actorSession: SEAT, state: "done", closureReason: "no-follow-on" });
      };
      await toDeadline(executorWith(withQueue()));
      expect(repo.getById(arrived!)).toMatchObject({ state: "done" });
      expect(world.calls.filter((c) => c.startsWith("stop") || c.startsWith("remove"))).toEqual([]);
      expect(slots.get(slot.slotId)).toMatchObject({ phase: "live", intent: null, retiredAt: null });
      expect(expired()).toEqual([]);
      expect(retrying().at(-1)?.notes).toMatchObject({ waiting: [{ slot: "dev.impl-s1", exitCode: 0, why: "work_arrived" }] });
    });

    it("S10: a new row another actor creates and cancels while the release runs is not the seat's work: the seat goes", async () => {
      const slot = liveSeat();
      world.commandGate = async () => {
        if (world.calls.at(-1) !== "run release") return;
        const row = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "never mind", nudge: false });
        await repo.update({ qitemId: row.qitemId, actorSession: "lead@r", state: "done", closureReason: "canceled" });
        expect(repo.getById(row.qitemId)).toMatchObject({ state: "done" });
      };
      await toDeadline(executorWith(withQueue()));
      expect(world.calls.filter((c) => c.startsWith("stop"))).toHaveLength(1);
      expect(slots.get(slot.slotId)?.retiredAt).not.toBeNull();
      expect(expired()).toEqual([expect.objectContaining({ failed: false, notes: expect.objectContaining({ left: [] }) })]);
    });

    it("S10: the queue's mark reads only the seat's own moves out of pending, in any of its addresses", async () => {
      const effects = withQueue();
      const mark = effects.queueMark!();
      const row = await repo.create({ sourceSession: SEAT, destinationSession: "lead@r", body: "a row the seat sends", nudge: false });
      expect(effects.seatActedSince!(SEAT, mark)).toBe(false);
      repo.claim({ qitemId: row.qitemId, destinationSession: "lead@r" });
      expect(effects.seatActedSince!(SEAT, mark)).toBe(false);
      const mine = await repo.create({ sourceSession: "lead@r", destinationSession: SEAT, body: "work", nudge: false });
      repo.claim({ qitemId: mine.qitemId, destinationSession: SEAT });
      expect(effects.seatActedSince!(SEAT, mark)).toBe(true);
      expect(effects.seatActedSince!(SEAT, effects.queueMark!())).toBe(false);
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
    expect(result).toEqual({ exitCode: 0, stdout: "$HOME;j1" });
  });

  it("reports the exit code, and kills a command at its timeout", async () => {
    const failed = await runSeatPoolCommand({ argv: [node, "-e", "process.exit(4)"], timeoutSeconds: 10 }, {}, new AbortController().signal);
    expect(failed.exitCode).toBe(4);
    const slow = await runSeatPoolCommand({ argv: [node, "-e", "setTimeout(() => {}, 60000)"], timeoutSeconds: 1 }, {}, new AbortController().signal);
    expect(slow).toMatchObject({ exitCode: null, error: "timed out after 1s" });
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

  it("frees the seats of a pool that has ended, so another pool adopts them, and keeps a live pool's", () => {
    db = createFullTestDb();
    const rigRepo = new RigRepository(db);
    const rig = rigRepo.createRig("r");
    const pod = new PodRepository(db).createPod(rig.id, "dev", "Dev");
    const add = (logicalId: string) => {
      const node = rigRepo.addNode(rig.id, logicalId, { runtime: "claude-code", cwd: "/tmp", podId: pod.id });
      db.prepare("INSERT INTO sessions (id, node_id, session_name, status) VALUES (?, ?, ?, 'running')").run(`s-${logicalId}`, node.id, `${logicalId.replace(".", "-")}@r`);
      return node;
    };
    add("dev.impl");
    const left = add("dev.impl-s1");
    const held = add("dev.impl-s2");
    const jobs = new WatchdogJobsRepository(db);
    const register = () => jobs.register({ policy: "seat-pool", specYaml: "policy: seat-pool", targetSession: "dev-impl@r", intervalSeconds: 60, registeredBySession: "test@r" }).jobId;
    const ended = register();
    const other = register();
    const slots = new SeatPoolSlots(db);
    const at = "2026-11-09T11:00:00.000Z";
    const old = slots.insert({ poolJobId: ended, logicalId: "dev.impl-s1", nodeId: left.id, phase: "live", intent: null, opId: "op-1", at });
    slots.insert({ poolJobId: other, logicalId: "dev.impl-s2", nodeId: held.id, phase: "live", intent: null, opId: "op-2", at });
    // A daemon without the seat-pool policy ends the job and leaves its seat running.
    jobs.markTerminal(ended, "unknown_policy:seat-pool");
    const state = readSeatPoolState({
      db, slots,
      openRows: () => [], sessionAddresses: (session) => [session], heldMessages: () => 0, seatStopped: () => false,
      seatState: () => null, restoring: () => false, demand: () => null,
    }, { jobId: JOB, targetSession: "dev-impl@r", spec: spec({ adopt: ["dev.impl-s1", "dev.impl-s2"] }) });
    expect(state.adoptable).toEqual([{ logicalId: "dev.impl-s1", nodeId: left.id }]);

    const adopted = slots.adopt({ poolJobId: JOB, logicalId: "dev.impl-s1", nodeId: left.id, at });
    // The ended pool added it: it stays a seat a pool added, which the new pool drains and reaps.
    expect(adopted).toMatchObject({ poolJobId: JOB, nodeId: left.id, phase: "live", opId: "op-1" });
    expect(slots.get(old.slotId)).toMatchObject({ phase: "retired", retiredAt: at });
    expect(slots.isLinked(left.id)).toBe(true);
    // A seat a live pool holds is never taken over: its link stays and the insert is refused.
    expect(() => slots.adopt({ poolJobId: JOB, logicalId: "dev.impl-s2", nodeId: held.id, at })).toThrow(/UNIQUE/);
    expect(slots.listOpen(other).map((s) => s.logicalId)).toEqual(["dev.impl-s2"]);
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
    db.prepare("UPDATE queue_items SET tags = ? WHERE qitem_id = 'q-model'").run(JSON.stringify(["role:impl"]));

    const state = readSeatPoolState({
      db, slots,
      openRows: () => [],
      sessionAddresses: (session) => (session === "dev-impl@r" ? ["dev-impl@r", "impl.alias@r"] : [session]),
      heldMessages: () => 0,
      seatStopped: (nodeId) => nodeId === s2.id || nodeId === model.id,
      seatState: (nodeId) => ({
        activity: "idle-at-prompt", needsInput: { count: nodeId === s1.id ? 1 : 0, reason: null }, backgroundWork: null, changedAt: "2026-11-09T11:00:00.000Z",
      }) as never,
      // The model and s2 are both stopped and out of usage.
      usageLimited: (session) => session === "dev-impl@r" || session === "dev-impl-s2@r",
      restoring: () => false,
      demand: () => null,
    }, { jobId: JOB, targetSession: "dev-impl@r", spec: spec() });

    // Only pending rows, under the name the pool knows each seat by; a blocked, claimed or human row is never read.
    expect(state.pending).toEqual([
      { qitemId: "q-old", destination: "dev-impl@r", waitingSince: "2026-11-09T11:05:00.000Z", tags: [] },
      { qitemId: "q-model", destination: "dev-impl@r", waitingSince: "2026-11-09T11:20:00.000Z", tags: ["role:impl"] },
      { qitemId: "q-slot", destination: "dev-impl-s1@r", waitingSince: "2026-11-09T11:30:00.000Z", tags: [] },
      { qitemId: "q-removed", destination: "dev-impl-s3@r", waitingSince: "2026-11-09T11:40:00.000Z", tags: [] },
      { qitemId: "q-moved", destination: "dev-impl@r", waitingSince: "2026-11-09T11:50:00.000Z", tags: [] },
    ]);
    // The model, which the pool can only launch, waits out the limit; a seat whose session is gone is reaped.
    expect(state.modelCause).toBe("usage_limit");
    expect(state.slots.map((slot) => [slot.sessionName, slot.cause])).toEqual([
      ["dev-impl-s1@r", "needs_input"],
      ["dev-impl-s2@r", "stopped"],
    ]);
    expect(planSeatPool(spec(), state, Date.parse("2026-11-09T12:00:00.000Z"), 120_000).intents.filter((i) => i.kind === "reap"))
      .toEqual([{ kind: "reap", slotId: 2 }]);
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
    migrate(db, [
      coreSchema, eventsSchema, streamItemsSchema, queueItemsSchema, queueTransitionsSchema, inboxEntriesSchema, outboxEntriesSchema,
      i3IdentityProvenanceSchema,
    ]);
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

  it("unclaims a claimed row, moves a blocked one without a wake, and counts only the reroutes a reap made", async () => {
    const claimed = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl-s1@r", body: "work", nudge: false });
    repo.update({ qitemId: claimed.qitemId, actorSession: "dev-impl-s1@r", state: "in-progress" });
    expect(effects.openRows("dev-impl-s1@r")).toMatchObject([{ qitemId: claimed.qitemId, state: "in-progress" }]);
    expect(effects.unclaimRow({ qitemId: claimed.qitemId, seat: "dev-impl-s1@r", reason: "reap:dev-impl-s1@r" })).toEqual({ ok: true });
    expect(repo.getById(claimed.qitemId)!.state).toBe("pending");
    expect(effects.unclaimRow({ qitemId: claimed.qitemId, seat: "dev-impl-s1@r", reason: "reap:dev-impl-s1@r" }))
      .toMatchObject({ ok: false, code: "qitem_not_in_progress" });

    expect(effects.moveRow({ qitemId: claimed.qitemId, from: "dev-impl-s1@r", to: "dev-impl@r", reason: "free seat" })).toEqual({ ok: true });
    expect(effects.reapCount(claimed.qitemId)).toBe(0);
    expect(effects.moveRow({ qitemId: claimed.qitemId, from: "dev-impl@r", to: "dev-impl-s2@r", reason: "reap:dev-impl@r" })).toEqual({ ok: true });
    // A note written through the update route reads the same, but it is not a reroute.
    repo.update({ qitemId: claimed.qitemId, actorSession: "system:queue-fallback", transitionNote: "fallback-routed: a → b (reap:a)", identityProvenance: "transport:v1" });
    expect(effects.reapCount(claimed.qitemId)).toBe(1);

    const blocked = await repo.create({ sourceSession: "lead@r", destinationSession: "dev-impl-s1@r", body: "work", nudge: false });
    repo.update({ qitemId: blocked.qitemId, actorSession: "dev-impl-s1@r", state: "blocked", blockedOn: claimed.qitemId });
    expect(effects.moveRow({ qitemId: blocked.qitemId, from: "dev-impl-s1@r", to: "dev-impl@r", reason: "reap:dev-impl-s1@r", state: "blocked" })).toEqual({ ok: true, park: "blocker" });
    expect(repo.getById(blocked.qitemId)).toMatchObject({ destinationSession: "dev-impl@r", state: "blocked" });
    expect(fallbackWakes(blocked.qitemId)).toEqual([]);
    expect(effects.reapCount(blocked.qitemId)).toBe(1);
  });

  it("an open escalation hides no row that waits after it: the new rows are added to it as a note, once", async () => {
    const first = await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1"] });
    const again = await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1", "q-2"] });
    expect(again).toMatchObject({ ok: true, qitemId: first.qitemId, existing: true, added: ["q-2"] });
    expect(repo.listTransitions(first.qitemId!).map((t) => t.transitionNote ?? "").join("\n")).toContain("q-2");
    const third = await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1", "q-2"] });
    expect(third).toEqual({ ok: true, qitemId: first.qitemId, destination: "lead@r", existing: true });
    // Another seat gets its own row: an open escalation is about its own seat only.
    const other = await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s2@r", model: "dev-impl@r", qitemIds: ["q-3"] });
    expect(other).toMatchObject({ ok: true, existing: false });
    expect(other.qitemId).not.toBe(first.qitemId);
    expect(repo.getById(first.qitemId!)!.body).toContain("no other row about dev-impl-s1@r");
  });

  it("opens one escalation per seat while it is open, and none without an escalation owner", async () => {
    const first = await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1"] });
    expect(first).toMatchObject({ ok: true, destination: "lead@r", existing: false });
    const row = repo.getById(first.qitemId!)!;
    expect(row).toMatchObject({ destinationSession: "lead@r", sourceSession: "daemon@kernel", tags: [`${SEAT_POOL_NEEDS_INPUT_TAG}:dev-impl-s1@r`] });
    expect(row.body).toContain("q-1");
    expect(await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s1@r", model: "dev-impl@r", qitemIds: ["q-1", "q-2"] }))
      .toEqual({ ok: true, qitemId: first.qitemId, destination: "lead@r", existing: true, added: ["q-2"] });
    owner = { owner: null, reason: "no escalation destination declared" };
    expect(await effects.escalate({ jobId: "pool-1", seat: "dev-impl-s2@r", model: "dev-impl@r", qitemIds: ["q-3"] }))
      .toEqual({ ok: false, code: "no_escalation_owner", error: "no escalation destination declared" });
  });
});
