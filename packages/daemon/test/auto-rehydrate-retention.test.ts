// The backlog drain of the auto-rehydrate retention: a rig with nothing to delete sits out a few ticks, and the
// operator off-switch (`OPENRIG_AUTO_REHYDRATE_RETENTION=off` in the daemon's environment) stops the retention at capture
// and keeps the drain from starting, without a rebuild.
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { SnapshotRepository } from "../src/domain/snapshot-repository.js";
import {
  AUTO_REHYDRATE_RETENTION_IDLE_TICKS, AUTO_REHYDRATE_RETENTION_INTERVAL_MS, AutoRehydrateRetentionScheduler,
} from "../src/domain/auto-rehydrate-retention.js";

const HOUR = 3_600_000;
const sqliteUtc = (ms: number) => new Date(ms).toISOString().replace("T", " ").slice(0, 19);

describe("auto-rehydrate retention off-switch", () => {
  let db: Database.Database;
  let repo: SnapshotRepository;
  const saved = process.env.OPENRIG_AUTO_REHYDRATE_RETENTION;

  beforeEach(() => {
    db = createFullTestDb();
    repo = new SnapshotRepository(db);
    const now = Date.now();
    for (let i = 0; i < 10; i++) {
      db.prepare("INSERT INTO snapshots (id, rig_id, kind, data, created_at) VALUES (?, 'rig-x', 'auto-rehydrate', '{}', ?)")
        .run(`old-${i}`, sqliteUtc(now - (30 + i) * HOUR));
    }
  });

  afterEach(() => {
    if (saved === undefined) delete process.env.OPENRIG_AUTO_REHYDRATE_RETENTION;
    else process.env.OPENRIG_AUTO_REHYDRATE_RETENTION = saved;
    db.close();
  });

  const left = () => (db.prepare("SELECT COUNT(*) AS n FROM snapshots WHERE rig_id = 'rig-x'").get() as { n: number }).n;

  it.each(["off", "0", "false", " OFF "])("%j: no prune, no drain tick deletes, and the drain does not start", async (value) => {
    process.env.OPENRIG_AUTO_REHYDRATE_RETENTION = value;
    expect(repo.pruneAutoRehydrate("rig-x", { keep: 1, batchSize: 50 })).toBe(0);
    expect(await new AutoRehydrateRetentionScheduler({ db, snapshotRepo: repo, keep: 1 }).tick()).toBe(0);
    expect(left()).toBe(10);
    const { startAutoRehydrateRetentionScheduler } = await import("../src/index.js");
    const start = vi.fn();
    expect(startAutoRehydrateRetentionScheduler({ autoRehydrateRetentionScheduler: { start } })).toBe(false);
    expect(start).not.toHaveBeenCalled();
  });

  it("unset: the retention prunes and the daemon starts the drain on its interval", async () => {
    delete process.env.OPENRIG_AUTO_REHYDRATE_RETENTION;
    expect(repo.pruneAutoRehydrate("rig-x", { keep: 1, batchSize: 50 })).toBe(9);
    const { startAutoRehydrateRetentionScheduler } = await import("../src/index.js");
    const start = vi.fn();
    expect(startAutoRehydrateRetentionScheduler({ autoRehydrateRetentionScheduler: { start } })).toBe(true);
    expect(start).toHaveBeenCalledWith(AUTO_REHYDRATE_RETENTION_INTERVAL_MS);
  });
});

describe("auto-rehydrate drain: a rig with nothing to delete", () => {
  it("sits out the idle ticks, then is tried again", async () => {
    const db = createFullTestDb();
    try {
      const repo = new SnapshotRepository(db);
      const now = Date.now();
      for (let i = 0; i < 3; i++) {
        db.prepare("INSERT INTO snapshots (id, rig_id, kind, data, created_at) VALUES (?, 'rig-h', 'auto-rehydrate', '{}', ?)")
          .run(`old-${i}`, sqliteUtc(now - (30 + i) * HOUR));
      }
      // Every old capture past keep is held by an in-flight restore: the rig keeps counting more than keep.
      const releases = ["old-1", "old-2"].map((id) => repo.holdForRestore(id));
      const prune = vi.spyOn(repo, "pruneAutoRehydrate");
      const drain = new AutoRehydrateRetentionScheduler({ db, snapshotRepo: repo, keep: 1 });
      expect(await drain.tick()).toBe(0);
      expect(prune).toHaveBeenCalledTimes(1);
      for (let i = 1; i < AUTO_REHYDRATE_RETENTION_IDLE_TICKS; i++) await drain.tick();
      expect(prune).toHaveBeenCalledTimes(1);
      releases.forEach((release) => release());
      expect(await drain.tick()).toBe(2);
      expect(prune).toHaveBeenCalledTimes(2);
    } finally { db.close(); }
  });
});

describe("auto-rehydrate drain: stop", () => {
  it("stops the tick in flight before its next rig, and resolves only after that tick returns", async () => {
    const db = createFullTestDb();
    try {
      const repo = new SnapshotRepository(db);
      const now = Date.now();
      for (const rig of ["rig-1", "rig-2"]) {
        for (let i = 0; i < 6; i++) {
          db.prepare("INSERT INTO snapshots (id, rig_id, kind, data, created_at) VALUES (?, ?, 'auto-rehydrate', '{}', ?)")
            .run(`${rig}-old-${i}`, rig, sqliteUtc(now - (30 + i) * HOUR));
        }
      }
      const count = (rig: string) => (db.prepare("SELECT COUNT(*) AS n FROM snapshots WHERE rig_id = ?").get(rig) as { n: number }).n;
      const drain = new AutoRehydrateRetentionScheduler({ db, snapshotRepo: repo, keep: 1, batchSize: 50 });
      let tickReturned = false;
      const tick = drain.tick().then((n) => { tickReturned = true; return n; });
      // The first rig's batch ran synchronously; the tick now waits to yield before the second rig.
      expect(count("rig-1")).toBe(1);
      await drain.stop();
      expect(tickReturned).toBe(true);
      expect(await tick).toBe(5);
      expect(count("rig-2")).toBe(6);
      expect(await drain.tick()).toBe(0);
      expect(count("rig-2")).toBe(6);
    } finally { db.close(); }
  });
});
