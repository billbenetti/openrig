import type { Migration } from "../migrate.js";

/** The delivery receipt notes a listing reads for its rows (receiptNotesFor), as one predicate: the partial index below
 *  and the query share this text, so the planner always sees the query's WHERE imply the index's. */
export const RECEIPT_NOTE_PREDICATE = `(transition_note LIKE 'slack-owner-notification-posted %'
  OR transition_note LIKE 'relay-owner-notification-accepted %'
  OR transition_note LIKE 'slack-owner-notification-transport-failed %'
  OR transition_note LIKE 'relay-owner-notification-transport-failed %')`;

/** The quiet-hold note a row's quiet custody starts from (quietHoldCustody), as an index predicate: LIKE is ASCII
 *  case-insensitive, so a row it finds nothing for has no such note, and one it finds is read exactly as before. */
export const QUIET_HOLD_NOTE_PREDICATE = "transition_note LIKE 'delivery-decision: quiet-hold %'";

/** The closed states, as a listing's WHERE term: the partial index below holds only their rows, and only a query that
 *  carries this term can use it (a listing of closed states adds it, a redundant term), so the other reads by state, the
 *  stuck sweep's oldest-first pending one included, keep their plans and the order they give rows created in the same ms. */
export const CLOSED_STATE_PREDICATE = "state NOT IN ('pending', 'in-progress', 'blocked')";

/** T-009903: GET /api/queue/list held the daemon's event loop ~1 s per call. Two of its reads scanned what an index
 *  answers: the receipt notes of the listed rows (every transition of those rows, ~40 000 for the ~570 active ones, to
 *  find the few receipts) and a listing of a closed state newest first (every row of that state sorted in a temporary
 *  B-tree, ~85 000 for "done"); and every blocked row's waiting view read all its notes (~120 each) to find a quiet hold
 *  that almost no row has. */
export const queueListReceiptAndStateIndexesSchema: Migration = {
  name: "170_queue_list_receipt_and_state_indexes.sql",
  sql: `
    CREATE INDEX IF NOT EXISTS idx_queue_transitions_receipt_notes
      ON queue_transitions(qitem_id, transition_id DESC) WHERE ${RECEIPT_NOTE_PREDICATE};
    CREATE INDEX IF NOT EXISTS idx_queue_transitions_quiet_hold
      ON queue_transitions(qitem_id) WHERE ${QUIET_HOLD_NOTE_PREDICATE};
    CREATE INDEX IF NOT EXISTS idx_queue_items_closed_created
      ON queue_items(state, ts_created DESC) WHERE ${CLOSED_STATE_PREDICATE};
  `,
};
