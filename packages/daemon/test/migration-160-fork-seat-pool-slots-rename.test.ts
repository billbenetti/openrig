// T-013304 (S3 of the REVISAO-P1-b of openrig#3395): the fork's 160 renames its pool table to fork_seat_pool_slots, so
// the upstream seat pool's 178 (#3480), which runs CREATE TABLE seat_pool_slots without IF NOT EXISTS, applies on a
// database the fork's pool already wrote, and the fork's rows stay where its pool reads them.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { migrate, type Migration } from "../src/db/migrate.js";
import { forkSeatPoolSlotsRenameSchema } from "../src/db/migrations/160_fork_seat_pool_slots_rename.js";
import { SeatPoolSlots } from "../src/domain/seat-pool-executor.js";

/** The upstream's 178 as in mvschwarz/openrig#3480 (head 2db98a4fd): the statements that collide with the fork. */
const UPSTREAM_178: Migration = {
  name: "178_seat_pool.sql",
  sql: `
    ALTER TABLE nodes ADD COLUMN pool TEXT;

    CREATE TABLE seat_pool_slots (
      slot_id TEXT PRIMARY KEY,
      model_node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
      slot_number INTEGER NOT NULL CHECK (typeof(slot_number) = 'integer' AND slot_number BETWEEN 1 AND 9007199254740991),
      node_id TEXT UNIQUE REFERENCES nodes(id) ON DELETE CASCADE,
      phase TEXT NOT NULL CHECK (phase IN ('adding', 'live', 'draining', 'drained')),
      phase_changed_at TEXT NOT NULL,
      attempt_id TEXT,
      attempt_kind TEXT CHECK (attempt_kind IN ('add', 'comeback')),
      attempt_state TEXT CHECK (attempt_state IN ('reserved', 'issued', 'settled')),
      attempt_basis TEXT,
      attempt_outcome TEXT,
      attempt_result TEXT,
      reuse_hold TEXT,
      created_at TEXT NOT NULL,
      UNIQUE (model_node_id, slot_number),
      CHECK (node_id IS NULL OR node_id <> model_node_id)
    );
  `,
};

const before160 = ALL_MIGRATIONS.filter((m) => m.name.localeCompare(forkSeatPoolSlotsRenameSchema.name) < 0);
const tableExists = (db: Database.Database, name: string) =>
  !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
const indexNames = (db: Database.Database, table: string) =>
  (db.prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name")
    .all(table) as Array<{ name: string }>).map((r) => r.name);
const slotRows = (db: Database.Database, table: string) => db.prepare(`SELECT * FROM ${table} ORDER BY id`).all();

describe("160_fork_seat_pool_slots_rename (T-013304)", () => {
  let db: Database.Database;
  beforeEach(() => {
    db = new Database(":memory:");
    // A database the fork's pool already wrote: every migration before the 160, as the VM's daemon has today.
    migrate(db, before160);
    const insert = db.prepare(`INSERT INTO seat_pool_slots (pool_job_id, node_id, logical_id, phase, intent, op_id, created_at,
      live_at, stopped_since, retired_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    insert.run("job-1", "node-a", "build.dev-1-s1", "live", null, "op-1", "2026-11-18T10:00:00.000Z", "2026-11-18T10:01:00.000Z", null, null);
    insert.run("job-1", "node-b", "build.dev-1-s2", "retired", null, "op-2", "2026-11-18T09:00:00.000Z", "2026-11-18T09:01:00.000Z",
      "2026-11-18T09:30:00.000Z", "2026-11-18T09:31:00.000Z");
    insert.run("job-1", null, "build.dev-1-s3", "adding", "add", "op-3", "2026-11-18T10:05:00.000Z", null, null, null);
  });
  afterEach(() => db.close());

  it("sorts after the 159s that add the table's columns and before the upstream's 178", () => {
    const names = ALL_MIGRATIONS.map((m) => m.name).sort((a, b) => a.localeCompare(b));
    const at = names.indexOf(forkSeatPoolSlotsRenameSchema.name);
    expect(at).toBeGreaterThan(names.indexOf("159_seat_pool_slots_live_at.sql"));
    expect(at).toBeGreaterThan(names.indexOf("159_seat_pool_slots_stopped_since.sql"));
    expect(forkSeatPoolSlotsRenameSchema.name.localeCompare(UPSTREAM_178.name)).toBeLessThan(0);
  });

  it("moves every row, the AUTOINCREMENT counter and both indexes to fork_seat_pool_slots, deleting nothing", () => {
    const rows = slotRows(db, "seat_pool_slots");
    const seq = db.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'seat_pool_slots'").get() as { seq: number };
    migrate(db, ALL_MIGRATIONS);
    expect(tableExists(db, "seat_pool_slots")).toBe(false);
    expect(slotRows(db, "fork_seat_pool_slots")).toEqual(rows);
    expect(db.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'fork_seat_pool_slots'").get()).toEqual(seq);
    expect(indexNames(db, "fork_seat_pool_slots")).toEqual(["idx_fork_seat_pool_slots_linked_node", "idx_fork_seat_pool_slots_pool"]);
    // The unique link of a live seat to its node still holds under the new index.
    expect(() => db.prepare(`INSERT INTO fork_seat_pool_slots (pool_job_id, node_id, logical_id, phase, created_at)
      VALUES ('job-2', 'node-a', 'other.s1', 'live', '2026-11-18T11:00:00.000Z')`).run()).toThrow(/UNIQUE/);
  });

  it("leaves the rows readable by the fork's pool", () => {
    migrate(db, ALL_MIGRATIONS);
    const slots = new SeatPoolSlots(db);
    expect(slots.listOpen("job-1").map((s) => s.logicalId)).toEqual(["build.dev-1-s1", "build.dev-1-s3"]);
  });

  it("then lets the upstream's 178 create its own seat_pool_slots, with the fork's rows untouched", () => {
    migrate(db, ALL_MIGRATIONS);
    const rows = slotRows(db, "fork_seat_pool_slots");
    expect(() => migrate(db, [...ALL_MIGRATIONS, UPSTREAM_178])).not.toThrow();
    const upstreamColumns = (db.prepare("PRAGMA table_info(seat_pool_slots)").all() as Array<{ name: string }>).map((c) => c.name);
    expect(upstreamColumns).toEqual(expect.arrayContaining(["slot_id", "model_node_id", "slot_number"]));
    expect(slotRows(db, "fork_seat_pool_slots")).toEqual(rows);
  });

  it("is the case it fixes: without the 160 the upstream's 178 fails on the fork's table", () => {
    expect(() => migrate(db, [...before160, UPSTREAM_178])).toThrow(/already exists/);
  });

  it("builds the same table on a fresh database", () => {
    const fresh = new Database(":memory:");
    try {
      migrate(fresh, ALL_MIGRATIONS);
      expect(tableExists(fresh, "seat_pool_slots")).toBe(false);
      const columns = (fresh.prepare("PRAGMA table_info(fork_seat_pool_slots)").all() as Array<{ name: string }>).map((c) => c.name);
      expect(columns).toEqual(expect.arrayContaining(["id", "pool_job_id", "node_id", "logical_id", "phase", "live_at", "stopped_since"]));
      expect(indexNames(fresh, "fork_seat_pool_slots")).toEqual(["idx_fork_seat_pool_slots_linked_node", "idx_fork_seat_pool_slots_pool"]);
    } finally {
      fresh.close();
    }
  });
});
