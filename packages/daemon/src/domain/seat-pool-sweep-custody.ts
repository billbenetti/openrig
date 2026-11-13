import type Database from "better-sqlite3";
import { parse as parseYaml } from "yaml";
import { parseSeatPoolSpec, SEAT_POOL_NO_SEAT_GRACE_DEFAULT_SECONDS } from "./policies/seat-pool.js";
import { nodeExecutorConfirmedStopped } from "./node-inventory.js";

/** Per database: when the sweep first saw each open `live` seat of a pool not running (by slot id). The grace of a seat
 *  that stopped counts from there; a daemon restart starts it again, which delays the finding, never holds it forever. */
const stoppedSinceByDb = new WeakMap<Database.Database, Map<number, number>>();

/** What the stuck sweep reads of one active seat pool to tell whether a row waiting for its model is the pool's to move. */
interface PoolQueueFacts {
  dispatchTag: string;
  /** A seat of the pool is live as the planner counts it: phase live, its node present and its session running (the
   *  planner's seatStopped, inverted). An adopted seat that stopped doesn't count, nor a seat still being added: either
   *  would hold the queue forever. */
  hasSeat: boolean;
  /** When the pool last had a seat: the latest of its registration, the retirement of a seat that was live (or
   *  adopted), and when the sweep first saw a live seat of it stopped. A seat retired before it was ever live never
   *  counts. */
  lastSeatMs: number;
  graceMs: number;
}

/**
 * The rows waiting in an active seat pool's queue (T-007029, mvschwarz/openrig#2988): a row addressed to the pool's
 * model seat and carrying its dispatch_tag is the pool's to move to a seat, so the stuck sweep files no
 * unclaimed-obligation finding for it while the pool is active, not draining, and has a live seat (its session
 * running, as the planner counts it) or had one within `no_seat_grace_s`; its rows are matched on every address of the model, as the pool's dispatch reads them. A pool without a dispatch_tag moves every row to its model and can't tell its
 * queue from the model's own work, so it holds none. Read once per sweep pass; anything unreadable holds nothing, so
 * the sweep's finding stays.
 */
export function seatPoolQueueCustody(
  db: Database.Database, nowMs: number, addressesOf: (sessionName: string) => string[] = (name) => [name],
): (row: { destinationSession: string | null; tags?: string[] | null }) => boolean {
  const pools = new Map<string, PoolQueueFacts[]>();
  try {
    const tables = new Set((db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('watchdog_jobs', 'seat_pool_slots', 'nodes')")
      .all() as Array<{ name: string }>).map((t) => t.name));
    if (tables.size === 3) {
      const jobs = db.prepare("SELECT job_id, spec_yaml, target_session, registered_at FROM watchdog_jobs WHERE policy = 'seat-pool' AND state = 'active'")
        .all() as Array<{ job_id: string; spec_yaml: string; target_session: string; registered_at: string }>;
      const liveSlots = db.prepare(
        `SELECT s.id, s.node_id FROM seat_pool_slots s JOIN nodes n ON n.id = s.node_id
          WHERE s.pool_job_id = ? AND s.retired_at IS NULL AND s.phase = 'live'`,
      );
      const latestStatus = db.prepare("SELECT status FROM sessions WHERE node_id = ? ORDER BY id DESC LIMIT 1");
      // The planner's seatStopped (startup), inverted: the node's latest session running, idle or unknown, and its
      // executor not confirmed stopped.
      const running = (nodeId: string): boolean =>
        ["running", "idle", "unknown"].includes((latestStatus.get(nodeId) as { status: string } | undefined)?.status ?? "")
        && !nodeExecutorConfirmedStopped(db, nodeId);
      let stoppedSince = stoppedSinceByDb.get(db);
      if (!stoppedSince) stoppedSinceByDb.set(db, stoppedSince = new Map());
      const stillStopped = new Set<number>();
      // A retired seat counts as one the pool had unless it was retired before it was ever live (intent 'never_live',
      // SeatPoolSlots.retire: an add or a launch that failed) or before it got a node. The slot's own row, not the job's
      // history (whose retention keeps only the newest evaluations), so the grace holds as long as the row does.
      const lastRetired = db.prepare(
        `SELECT MAX(s.retired_at) AS at FROM seat_pool_slots s
          WHERE s.pool_job_id = ? AND s.retired_at IS NOT NULL
            AND (s.op_id IS NULL OR (s.node_id IS NOT NULL AND COALESCE(s.intent, '') <> 'never_live'))`,
      );
      for (const job of jobs) {
        const parsed = parseYaml(job.spec_yaml) as { context?: unknown } | null;
        const context = parsed && typeof parsed.context === "object" && parsed.context !== null ? parsed.context as Record<string, unknown> : {};
        const pool = parseSeatPoolSpec(context);
        if (!("spec" in pool) || pool.spec.drain || !pool.spec.dispatchTag) continue;
        const retired = Date.parse((lastRetired.get(job.job_id) as { at: string | null }).at ?? "");
        const registered = Date.parse(job.registered_at);
        let hasSeat = false;
        let lastStoppedMs = -Infinity;
        for (const slot of liveSlots.all(job.job_id) as Array<{ id: number; node_id: string }>) {
          if (running(slot.node_id)) { hasSeat = true; continue; }
          const since = stoppedSince.get(slot.id) ?? nowMs;
          stoppedSince.set(slot.id, since);
          stillStopped.add(slot.id);
          lastStoppedMs = Math.max(lastStoppedMs, since);
        }
        const facts: PoolQueueFacts = {
          dispatchTag: pool.spec.dispatchTag,
          hasSeat,
          lastSeatMs: Math.max(Number.isFinite(retired) ? retired : -Infinity, Number.isFinite(registered) ? registered : -Infinity, lastStoppedMs),
          graceMs: (pool.spec.noSeatGraceSeconds ?? SEAT_POOL_NO_SEAT_GRACE_DEFAULT_SECONDS) * 1000,
        };
        // Every address of the model, as the pool's dispatch reads them (readPendingRows).
        for (const address of new Set([job.target_session, ...addressesOf(job.target_session)])) {
          pools.set(address, [...(pools.get(address) ?? []), facts]);
        }
      }
      // A seat seen stopped that runs again, or left the pool, starts afresh the next time it stops.
      for (const id of [...stoppedSince.keys()]) if (!stillStopped.has(id)) stoppedSince.delete(id);
    }
  } catch {
    pools.clear();
  }
  return (row) => {
    if (!row.destinationSession) return false;
    const tags = row.tags ?? [];
    return (pools.get(row.destinationSession) ?? []).some((pool) =>
      tags.includes(pool.dispatchTag) && (pool.hasSeat || nowMs - pool.lastSeatMs <= pool.graceMs));
  };
}
