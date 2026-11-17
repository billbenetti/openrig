// The heavy reads off the event loop (T-010231): /api/ps, /api/rigs/summary, the usage series and the wake ladder's
// prompt candidates read on the read worker's own read-only connection, with the same result as the synchronous read
// on the main handle, and the loop keeps turning while they run.
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { PsProjectionService } from "../src/domain/ps-projection.js";
import { AgentActivityStore } from "../src/domain/agent-activity-store.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { readOffLoop, setReadOffLoopEnabledForTest, stopReadOffLoopWorker } from "../src/domain/read-off-loop.js";
import { readRigSummaryEntries } from "../src/domain/rig-summary-projection.js";
import { queryUsageSeries } from "../src/domain/usage-series.js";
import { UsageSamplesStore } from "../src/domain/usage-samples-store.js";
import { readPromptRefusalCandidates, readPromptRefusalCandidatesOffLoop } from "../src/domain/queue-wake-ladder.js";
import { REFUSED_PREFIX } from "../src/domain/policies/parked-owner-consumer.js";
import { seedHostShaped, type HostShape } from "./helpers/seed-host-shaped.js";

/** The longest gap between ticks of a 5 ms timer while `run` is in flight: how long the loop stood still. */
async function withLongestTickGap<T>(run: () => Promise<T>): Promise<{ value: T; longestGapMs: number }> {
  let last = performance.now();
  let longestGapMs = 0;
  const timer = setInterval(() => {
    const now = performance.now();
    longestGapMs = Math.max(longestGapMs, now - last);
    last = now;
  }, 5);
  try {
    const value = await run();
    longestGapMs = Math.max(longestGapMs, performance.now() - last);
    return { value, longestGapMs };
  } finally {
    clearInterval(timer);
  }
}

/** Counts the statements the main handle prepares while `run` is in flight. */
async function mainPrepares<T>(db: Database.Database, run: () => Promise<T>): Promise<{ value: T; prepares: string[] }> {
  const spy = vi.spyOn(db, "prepare");
  try {
    const value = await run();
    return { value, prepares: spy.mock.calls.map((call) => String(call[0])) };
  } finally {
    spy.mockRestore();
  }
}

/** The daemon's hook store, wired as startup wires it. */
function daemonHookStore(db: Database.Database): AgentActivityStore {
  const sessions = new SessionRegistry(db);
  return new AgentActivityStore({
    db,
    eventBus: new EventBus(db),
    resolveOccupantGeneration: (nodeId) => sessions.currentOccupantTenure(nodeId)?.generationUuid ?? null,
    isRegisteredOccupantGeneration: (nodeId, generation) => sessions.isOccupantGenerationRegistered(nodeId, generation),
  });
}

describe("reads off the event loop", () => {
  let dir: string;
  let file: string;
  let db: Database.Database;
  let shape: HostShape;
  const NOW = Date.parse("2026-07-01T00:05:00.000Z");

  beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "read-off-loop-"));
    file = path.join(dir, "openrig.sqlite");
    db = createDb(file);
    migrate(db, ALL_MIGRATIONS);
    shape = seedHostShaped(db);
    // Assigned work, prompt refusals and usage samples, so every read has rows to return.
    const queue = db.prepare(`INSERT INTO queue_items (qitem_id, ts_created, ts_updated, source_session, destination_session, state, body, tags)
      VALUES (?, '2026-07-01T00:00:00.000Z', '2026-07-01T00:00:00.000Z', 'source@rig', ?, ?, 'body', NULL)`);
    const transition = db.prepare(`INSERT INTO queue_transitions (qitem_id, ts, state, transition_note, actor_session)
      VALUES (?, '2026-07-01T00:00:00.000Z', 'pending', ?, 'source@rig')`);
    shape.allNodeIds.slice(0, 40).forEach((nodeId, i) => {
      const id = `q-${String(i).padStart(3, "0")}`;
      queue.run(id, shape.sessionNameByNode.get(nodeId)!, ["pending", "in-progress", "blocked", "done"][i % 4]);
      if (i % 3 === 0) transition.run(id, `${REFUSED_PREFIX} the seat sits at a prompt`);
    });
    const usage = new UsageSamplesStore(db);
    for (let i = 0; i < 300; i++) {
      const at = new Date(Date.parse("2026-07-01T00:00:00.000Z") + i * 1000).toISOString();
      usage.appendContextSample({ nodeId: shape.allNodeIds[i % 10]!, seatSession: `seat-${i % 10}@rig`, source: "claude_statusline_json",
        sampledAt: at, totalInputTokens: i * 100, totalOutputTokens: i, usedPercentage: i % 100 }, at);
    }
  });

  afterAll(async () => {
    await stopReadOffLoopWorker();
    setReadOffLoopEnabledForTest(true);
    db.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("serves /api/ps's read from the worker, equal to the synchronous read, for every archive filter", async () => {
    const ps = new PsProjectionService({ db, agentActivity: daemonHookStore(db) });
    ps.setPeriodicSnapshotState(true, 300);
    for (const filter of [undefined, { includeArchived: true }, { archivedOnly: true }]) {
      const sync = ps.getEntries(filter, NOW);
      const { value: offLoop, prepares } = await mainPrepares(db, () => ps.getEntriesOffLoop(filter, NOW));
      expect(offLoop).toEqual(sync);
      // The main handle prepared nothing: every read ran on the worker's connection.
      expect(prepares).toEqual([]);
    }
    expect(ps.getEntries({ includeArchived: true }, NOW).length).toBe(shape.allRigIds.length);
  });

  it("serves the rig summaries, the usage series and the prompt candidates from the worker, equal to the synchronous read", async () => {
    for (const filter of [{}, { includeArchived: true }, { archivedOnly: true }]) {
      const { value, prepares } = await mainPrepares(db, () => readOffLoop(db, "rigSummaries", { filter }, () => readRigSummaryEntries(db, filter),
        { site: "rigs.summary.read", record: "whenSlow" }));
      expect(value).toEqual(readRigSummaryEntries(db, filter));
      expect(prepares).toEqual([]);
    }
    for (const query of [{}, { seatSession: "seat-3@rig" }, { lane: "context" as const, sinceIso: "2026-07-01T00:01:00.000Z", limit: 50 }]) {
      const { value, prepares } = await mainPrepares(db, () => readOffLoop(db, "usageSeriesJson", query,
        () => JSON.stringify(queryUsageSeries(db, query)), { site: "telemetry.usage_series.read", record: "whenSlow" }));
      expect(value).toBe(JSON.stringify(queryUsageSeries(db, query)));
      expect(JSON.parse(value).length).toBeGreaterThan(0);
      expect(prepares).toEqual([]);
    }
    const { value: candidates, prepares } = await mainPrepares(db, () => readPromptRefusalCandidatesOffLoop(db));
    expect(candidates).toEqual(readPromptRefusalCandidates(db));
    expect(candidates.length).toBeGreaterThan(0);
    expect(prepares).toEqual([]);
  });

  it("keeps the event loop turning while the worker reads, where the synchronous read holds it", async () => {
    const ps = new PsProjectionService({ db, agentActivity: daemonHookStore(db) });
    await ps.getEntriesOffLoop({ includeArchived: true }, NOW); // the worker is up and its module loaded
    const sync = await withLongestTickGap(async () => ps.getEntries({ includeArchived: true }, NOW));
    const offLoop = await withLongestTickGap(() => ps.getEntriesOffLoop({ includeArchived: true }, NOW));
    const summaries = await withLongestTickGap(() => readOffLoop(db, "rigSummaries", { filter: { includeArchived: true } },
      () => readRigSummaryEntries(db, { includeArchived: true }), { site: "rigs.summary.read", record: "whenSlow" }));
    expect(offLoop.value).toEqual(sync.value);
    expect(summaries.value.length).toBe(shape.allRigIds.length);
    // The fold of the in-memory state and the clone of the result are what is left on the loop.
    expect(offLoop.longestGapMs).toBeLessThan(50);
    expect(summaries.longestGapMs).toBeLessThan(50);
  });

  it("reads on the main handle inside a transaction, where only it sees the uncommitted rows", async () => {
    const ps = new PsProjectionService({ db });
    db.exec("BEGIN");
    try {
      db.prepare("INSERT INTO rigs (id, name) VALUES ('rig-uncommitted', 'zz-uncommitted')").run();
      const entries = await ps.getEntriesOffLoop(undefined, NOW);
      expect(entries.map((entry) => entry.name)).toContain("zz-uncommitted");
    } finally {
      db.exec("ROLLBACK");
    }
  });

  it("reads on the main handle for a memory database", async () => {
    const memory = createDb();
    migrate(memory, ALL_MIGRATIONS);
    try {
      memory.prepare("INSERT INTO rigs (id, name) VALUES ('rig-mem', 'mem')").run();
      const ps = new PsProjectionService({ db: memory });
      const { value, prepares } = await mainPrepares(memory, () => ps.getEntriesOffLoop(undefined, NOW));
      expect(value).toEqual(ps.getEntries(undefined, NOW));
      expect(prepares.length).toBeGreaterThan(0);
    } finally {
      memory.close();
    }
  });

  it("falls back to the main handle when the worker cannot open the file", async () => {
    // A second database whose file is removed while the main handle keeps it open: the worker's open fails.
    const gone = path.join(dir, "gone.sqlite");
    const other = createDb(gone);
    migrate(other, ALL_MIGRATIONS);
    other.prepare("INSERT INTO rigs (id, name) VALUES ('rig-gone', 'gone')").run();
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(gone + suffix, { force: true });
      const ps = new PsProjectionService({ db: other });
      expect((await ps.getEntriesOffLoop(undefined, NOW)).map((entry) => entry.name)).toEqual(["gone"]);
    } finally {
      errors.mockRestore();
      other.close();
    }
  });
});
