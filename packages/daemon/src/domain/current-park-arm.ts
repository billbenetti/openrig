import type Database from "better-sqlite3";

// T-007715 (the redesign of the openrig#9): THE rule for "the arm of a row's current park", in one place.
//
// A park's wake is the `armed` record written on the transition that parked the row (queue_transition_wakes). It
// belongs to the row's CURRENT park only while it is the row's latest arm AND the row has not left `blocked` since:
// a row that went back to in-progress (or anywhere else) and was parked again with no wake of its own (a gate, a
// person) wrote no arm, so the earlier arm is a past park's. Note-only transitions (a wake receipt, a ladder marker,
// a blocker custody move) stay `blocked` and keep the arm current; so does a re-park blocked → blocked with no wake.
//
// This rule says only which arm is current. What consumes it (a fire, a retained or failed attempt) stays each
// reader's own rule: a repeating job and an attachment are consumed differently on purpose.
//
// Two forms of the same rule: an SQL predicate for the readers that select by set (by job, by seat), and a per-row
// lookup. The mechanisms that run AFTER a row's exit transition is appended (the retire of a park timer, the
// supersede on a re-park, the retire inside routeInTransaction) read the raw latest arm instead
// (QueueWakeRepository.getStatus): with this rule they would find nothing, and a one-shot timer would never be
// retired. Anything that asks while the row is still parked (rerouteParked, the reuse of a wait on a re-park) reads
// this rule.
//
// An arm whose transition is no longer in queue_transitions (retention archived the closed row's history, and a
// reopen parked it again with no wake) is a past park's too.

const SQL_ALIAS = /^[A-Za-z_][A-Za-z0-9_]*$/;

function alias(w: string): string {
  if (!SQL_ALIAS.test(w)) throw new Error(`current park arm: invalid SQL alias ${JSON.stringify(w)}`);
  return w;
}

/** SQL predicate: the queue_transition_wakes row aliased `w` is its row's latest arm. */
export function latestParkArmSql(w: string): string {
  const a = alias(w);
  return `${a}.transition_id = (
    SELECT MAX(${a}_cpa_latest.transition_id) FROM queue_transition_wakes ${a}_cpa_latest
     WHERE ${a}_cpa_latest.qitem_id = ${a}.qitem_id AND ${a}_cpa_latest.phase = 'armed'
  )`;
}

/** SQL predicate: the row of the queue_transition_wakes row aliased `w` has not left `blocked` since that record. */
export function parkUnbrokenSinceSql(w: string): string {
  const a = alias(w);
  return `NOT EXISTS (
    SELECT 1 FROM queue_transitions ${a}_cpa_left
     WHERE ${a}_cpa_left.qitem_id = ${a}.qitem_id AND ${a}_cpa_left.transition_id > ${a}.transition_id
       AND ${a}_cpa_left.state <> 'blocked'
  )`;
}

/** SQL predicate: the queue_transition_wakes row aliased `w` is the arm of its row's current park. */
export function currentParkArmSql(w: string): string {
  const a = alias(w);
  return `${a}.phase = 'armed' AND ${latestParkArmSql(a)} AND ${parkUnbrokenSinceSql(a)}
    AND EXISTS (SELECT 1 FROM queue_transitions ${a}_cpa_own WHERE ${a}_cpa_own.transition_id = ${a}.transition_id)`;
}

export interface CurrentParkArmRow {
  transition_id: number;
  qitem_id: string;
  phase: string;
  wake_kind: string;
  wake_ref: string;
  delivery_status: string | null;
}

/** The arm of the row's current park, or undefined (never parked with a wake, or parked again since with none). */
export function currentParkArm(db: Database.Database, qitemId: string): CurrentParkArmRow | undefined {
  return db.prepare(`SELECT * FROM queue_transition_wakes w WHERE w.qitem_id = ? AND ${currentParkArmSql("w")}`)
    .get(qitemId) as CurrentParkArmRow | undefined;
}

/** The blocked rows whose current park is attached to `jobId` (`--wake-watchdog`) and has not consumed it yet. A
 *  retained attempt never consumes an attachment; a failed one consumes it only when `repeating` is false (a one-shot
 *  timer; a repeating wait retries). This is the attachment's consumption rule, not the current-arm rule above. */
export function liveWatchdogAttachments(db: Database.Database, jobId: string, repeating: boolean): Array<{ qitemId: string; state: string }> {
  return db.prepare(
    `SELECT DISTINCT w.qitem_id, q.state
       FROM queue_transition_wakes w
       JOIN queue_items q ON q.qitem_id = w.qitem_id
      WHERE w.wake_ref = ? AND w.wake_kind = 'watchdog' AND q.state = 'blocked'
        AND ${currentParkArmSql("w")}
        AND NOT EXISTS (
          SELECT 1 FROM queue_transition_wakes f
           WHERE f.qitem_id = w.qitem_id AND f.phase = 'fired'
             AND f.wake_ref = w.wake_ref AND f.transition_id > w.transition_id
             AND (f.delivery_status IS NULL OR f.delivery_status != 'retained')
             AND (? = 0 OR f.delivery_status IS NULL OR (
               f.delivery_status != 'failed' AND f.delivery_status NOT LIKE 'failed:%'
             ))
        )`,
  ).all(jobId, repeating ? 1 : 0).map((row) => {
    const r = row as { qitem_id: string; state: string };
    return { qitemId: r.qitem_id, state: r.state };
  });
}
