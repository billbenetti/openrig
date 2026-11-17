import type { Migration } from "../migrate.js";

/**
 * The seats a `seat-pool` watchdog job created or adopted, one row per seat, kept after removal so a name is never
 * reused. The row is the only link between a pool and a seat: the pool never claims a node by its name. `intent`
 * records an effect before it runs and is cleared once its result is recorded, so a restart can see what was
 * interrupted. `op_id` is the Operation-Id of the add; an adopted seat has none.
 */
export const seatPoolSlotsSchema: Migration = {
  name: "158_seat_pool_slots.sql",
  sql: `
    CREATE TABLE IF NOT EXISTS seat_pool_slots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pool_job_id TEXT NOT NULL,
      node_id TEXT,
      logical_id TEXT NOT NULL,
      phase TEXT NOT NULL CHECK (phase IN ('adding', 'added', 'live', 'draining', 'retired')),
      intent TEXT,
      op_id TEXT,
      created_at TEXT NOT NULL,
      retired_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_seat_pool_slots_pool ON seat_pool_slots(pool_job_id, retired_at);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_seat_pool_slots_linked_node
      ON seat_pool_slots(node_id) WHERE retired_at IS NULL AND node_id IS NOT NULL;
  `,
};
