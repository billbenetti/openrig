// Daemon start's unattended restore keeps its attempt after it returns, and the requests that meet it read the
// attempt's snapshot by id (joinAutomaticRestore, automaticRestoreOverrides): the auto-rehydrate retention keeps that
// snapshot while the attempt is the rig's latest, and lets it go once a newer attempt replaces it.
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SnapshotRepository } from "../src/domain/snapshot-repository.js";
import { CheckpointStore } from "../src/domain/checkpoint-store.js";
import { SnapshotCapture } from "../src/domain/snapshot-capture.js";
import { restoreExistingRigUnattended } from "../src/domain/existing-rig-restore.js";

const HOUR = 3_600_000;
const sqliteUtc = (ms: number) => new Date(ms).toISOString().replace("T", " ").slice(0, 19);

describe("daemon start's unattended restore and the auto-rehydrate retention", () => {
  let db: Database.Database;
  let rigRepo: RigRepository;
  let snapshotRepo: SnapshotRepository;
  let snapshotCapture: SnapshotCapture;

  beforeEach(() => {
    db = createFullTestDb();
    rigRepo = new RigRepository(db);
    snapshotRepo = new SnapshotRepository(db);
    snapshotCapture = new SnapshotCapture({
      db, rigRepo, sessionRegistry: new SessionRegistry(db), eventBus: new EventBus(db), snapshotRepo, checkpointStore: new CheckpointStore(db),
    });
  });

  afterEach(() => db.close());

  const autoRehydrate = (rigId: string) =>
    (db.prepare("SELECT id FROM snapshots WHERE rig_id = ? AND kind = 'auto-rehydrate'").all(rigId) as Array<{ id: string }>).map((r) => r.id);

  it("holds the attempt's snapshot after the restore returns, until a newer attempt of the rig replaces it", async () => {
    const rig = rigRepo.createRig("r-boot");
    const node = rigRepo.addNode(rig.id, "worker", { role: "worker", runtime: "claude-code" });
    const sessions = new SessionRegistry(db);
    sessions.updateStatus(sessions.registerSession(node.id, "worker@r-boot").id, "running");
    const now = Date.now();
    const target = snapshotCapture.captureSnapshot(rig.id, "auto-rehydrate");
    db.prepare("UPDATE snapshots SET created_at = ? WHERE id = ?").run(sqliteUtc(now - 100 * HOUR), target.id);
    const body = (db.prepare("SELECT data FROM snapshots WHERE id = ?").get(target.id) as { data: string }).data;
    // Without a restore orchestrator the attempt returns right after choosing its snapshot.
    const deps = { rigRepo, snapshotRepo, snapshotCapture };
    await restoreExistingRigUnattended(deps, rig.id, () => true);

    // Newer old captures and a crash-tier copy now outrank the target: only the attempt holds it back.
    for (const [id, age] of [["old-40h", 40], ["old-30h", 30]] as const) {
      db.prepare("INSERT INTO snapshots (id, rig_id, kind, data, created_at) VALUES (?, ?, 'auto-rehydrate', ?, ?)")
        .run(id, rig.id, body, sqliteUtc(now - age * HOUR));
    }
    db.prepare("INSERT INTO snapshots (id, rig_id, kind, data, created_at) VALUES ('pre-down', ?, 'auto-pre-down', ?, ?)")
      .run(rig.id, body, sqliteUtc(now - 200 * HOUR));

    snapshotRepo.pruneAutoRehydrate(rig.id, { keep: 1, batchSize: 50 });
    expect(autoRehydrate(rig.id)).toContain(target.id);
    expect(autoRehydrate(rig.id)).not.toContain("old-40h");

    // A newer attempt chooses another snapshot: the old one is an ordinary old capture again.
    await restoreExistingRigUnattended(deps, rig.id, () => true);
    snapshotRepo.pruneAutoRehydrate(rig.id, { keep: 1, batchSize: 50 });
    expect(autoRehydrate(rig.id)).not.toContain(target.id);
    expect(autoRehydrate(rig.id)).toContain("old-30h");
  });
});
