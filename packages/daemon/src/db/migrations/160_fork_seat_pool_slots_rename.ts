import type { Migration } from "../migrate.js";

/**
 * T-013304 (S3 of the REVISAO-P1-b of openrig#3395): the fork's pool table moves out of the name the upstream seat pool
 * takes. The upstream's `178_seat_pool.sql` (#3480) runs `CREATE TABLE seat_pool_slots` without IF NOT EXISTS, with
 * another shape (one row per slot of a model node); on a database where the fork's 158 had already created the table
 * it fails and the daemon does not start. This RENAMES the fork's table and its two indexes, so every row the pool
 * holds stays, and the upstream migration then finds the name free. Nothing is deleted: the indexes are recreated
 * under the new name with the same definition (SQLite has no ALTER INDEX ... RENAME), and the rename carries the
 * table's AUTOINCREMENT counter in sqlite_sequence.
 *
 * Its name sorts right after the two 159s, so a fresh database creates the table (158), adds its columns (159) and
 * renames it before any later migration; every later fork migration that touches the table uses the new name. The
 * 158 and 159 keep their SQL as it was applied: they are history, run once, by name.
 *
 * Going back to a build without this migration needs, in one step before that build starts:
 * `ALTER TABLE fork_seat_pool_slots RENAME TO seat_pool_slots`, the two indexes under the old names, and
 * `DELETE FROM schema_migrations WHERE name = '160_fork_seat_pool_slots_rename.sql'`. The old 158 is already applied
 * and will not create the table again; without the DELETE, coming back to this build skips the 160 (its name is
 * applied), the pool finds no fork_seat_pool_slots, and the upstream's 178, when present, fails on the old name.
 */
export const forkSeatPoolSlotsRenameSchema: Migration = {
  name: "160_fork_seat_pool_slots_rename.sql",
  sql: `
    ALTER TABLE seat_pool_slots RENAME TO fork_seat_pool_slots;
    DROP INDEX IF EXISTS idx_seat_pool_slots_pool;
    DROP INDEX IF EXISTS idx_seat_pool_slots_linked_node;
    CREATE INDEX IF NOT EXISTS idx_fork_seat_pool_slots_pool ON fork_seat_pool_slots(pool_job_id, retired_at);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_fork_seat_pool_slots_linked_node
      ON fork_seat_pool_slots(node_id) WHERE retired_at IS NULL AND node_id IS NOT NULL;
  `,
};
