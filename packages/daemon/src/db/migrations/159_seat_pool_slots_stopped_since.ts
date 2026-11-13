import type { Migration } from "../migrate.js";

/**
 * T-007545 (SD1 and SD2 of the v0.7.13 review): when a pool's seat stopped running, kept on its slot so the stuck
 * sweep's grace over the pool's queue survives a daemon restart (it was kept in the process and a restart gave every
 * stopped seat a whole grace again), and so a retired seat counts until it stopped running, not until its reap.
 * Set by the sweep the first time it sees a live seat stopped and by the reap before it cleans the seat (from the
 * session's record of the stop when there is one); cleared when the seat runs again.
 * `live_at`: when the pool last started the seat (its launch or relaunch worked). A pool seat counts as one the pool had
 * only once it ran for a sweep period from then, so a crash loop (launch, the session dies, reap, add) never renews the
 * grace (B1 of the REVISAO-T-007545).
 */
export const seatPoolSlotsStoppedSinceSchema: Migration = {
  name: "159_seat_pool_slots_stopped_since.sql",
  sql: `
    ALTER TABLE seat_pool_slots ADD COLUMN stopped_since TEXT;
    ALTER TABLE seat_pool_slots ADD COLUMN live_at TEXT;
  `,
};
