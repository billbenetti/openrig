import type { Migration } from "../migrate.js";

/**
 * T-012748 (fork): the gate a row was blocked on at each blocked transition. queue_items.blocked_on holds only the
 * current one, so a reader of the history (the seat pool's `return_long_gates` age, which counts only the time on a
 * listed gate) could not tell a re-park on another gate from a note on the same one. Written on every transition
 * whose state is `blocked` (QueueTransitionLog.append), NULL on the others. Additive and nullable: absent on every
 * earlier row (no backfill; a reader treats NULL as unknown). The transitions archive gets it too, as migration 140
 * gave it the principal, so retention keeps a row's gates with the row.
 */
export const queueTransitionsBlockedOnSchema: Migration = {
  name: "174_queue_transitions_blocked_on.sql",
  sql: `
    ALTER TABLE queue_transitions ADD COLUMN blocked_on TEXT;
    ALTER TABLE queue_transitions_archive ADD COLUMN blocked_on TEXT;
  `,
};
