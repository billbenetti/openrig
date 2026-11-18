import type { Migration } from "../migrate.js";

/** The wake ladder's prompt-refusal note (REFUSED_PREFIX of the parked-owner consumer), as an index predicate. The
 *  partial index below and the candidates read share this text, so the planner always sees the read's WHERE imply the
 *  index's. A literal, like the 170's predicates: a migration does not import from domain/ (the test pins it to the
 *  prefix). LIKE is ASCII case-insensitive, as the read always was. */
export const PROMPT_REFUSAL_NOTE_PREDICATE = "transition_note LIKE 'parked-owner wake delivery refused:%'";

/** T-010230: queue.wake_ladder.prompt_candidates, run every ladder tick, read every transition of each open row (its
 *  note text, ~40 000 rows for the ~570 active ones) to find its first refusal: 82 ms normally, 4 s under IO pressure
 *  (16/11 16:39:10Z). The partial index holds only the refusal notes, written by SQLite in the same insert as the
 *  transition, so the first refusal of a row is one entry of a small index and no note text is read. Retention and
 *  erasure delete the transition and the entry goes with it, so the read returns exactly what it returned before. */
export const queueTransitionsPromptRefusalIndexSchema: Migration = {
  name: "171_queue_transitions_prompt_refusal_index.sql",
  sql: `
    CREATE INDEX IF NOT EXISTS idx_queue_transitions_prompt_refusal
      ON queue_transitions(qitem_id, transition_id) WHERE ${PROMPT_REFUSAL_NOTE_PREDICATE};
  `,
};
