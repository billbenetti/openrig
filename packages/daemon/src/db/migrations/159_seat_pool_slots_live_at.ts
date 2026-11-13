import type { Migration } from "../migrate.js";

/**
 * T-007545 (B1 of its review): when the pool last started a seat (its launch or relaunch worked). A seat the pool
 * started counts as one the pool had only once it ran SEAT_POOL_MIN_RUN_MS from then, so a crash loop (launch, the
 * session dies, reap, add) never renews the stuck sweep's grace over the pool's queue. Its own migration, apart from
 * 159_seat_pool_slots_stopped_since, so a database that already applied that one still gets the column.
 */
export const seatPoolSlotsLiveAtSchema: Migration = {
  name: "159_seat_pool_slots_live_at.sql",
  sql: `
    ALTER TABLE seat_pool_slots ADD COLUMN live_at TEXT;
  `,
};
