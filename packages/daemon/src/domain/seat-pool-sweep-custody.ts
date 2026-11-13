import type Database from "better-sqlite3";
import { parse as parseYaml } from "yaml";
import { parseSeatPoolSpec, SEAT_POOL_NO_SEAT_GRACE_DEFAULT_SECONDS } from "./policies/seat-pool.js";
import { nodeExecutorConfirmedStopped } from "./node-inventory.js";
import { SeatPoolSlots } from "./seat-pool-executor.js";

/** T-007545 (B1 of its review): a seat the pool started counts as one the pool had only once it ran this long from its
 *  start (live_at), a sweep period (the stuck sweep runs every 300 s): a crash loop (the launch works, the session dies
 *  within a minute or two, the reap, the next add) never renews the grace, however often it turns. A seat started
 *  before live_at was kept, and an adopted one, count as before. */
export const SEAT_POOL_MIN_RUN_MS = 5 * 60_000;

/** What the stuck sweep reads of one active seat pool to tell whether a row waiting for its model is the pool's to move. */
interface PoolQueueFacts {
  dispatchTag: string;
  /** A seat of the pool is live as the planner counts it: phase live, its node present and its session running (the
   *  planner's seatStopped, inverted). An adopted seat that stopped doesn't count, nor a seat still being added: either
   *  would hold the queue forever. */
  hasSeat: boolean;
  /** When the pool last had a seat: the latest of its registration, when a retired seat that was live (or adopted)
   *  stopped running (its retirement when it was reaped still running), and when a live seat of it stopped. A seat
   *  retired before it was ever live never counts. T-007545: both stops are kept on the slot (stopped_since), so a
   *  daemon restart doesn't renew the grace (SD1); and a seat the pool started counts only once it ran for
   *  SEAT_POOL_MIN_RUN_MS from its start (live_at), so a crash loop's turns don't renew it either (SD2, B1). */
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
        `SELECT s.id, s.node_id, s.op_id, s.live_at FROM seat_pool_slots s JOIN nodes n ON n.id = s.node_id
          WHERE s.pool_job_id = ? AND s.retired_at IS NULL AND s.phase = 'live'`,
      );
      const latestStatus = db.prepare("SELECT status FROM sessions WHERE node_id = ? ORDER BY id DESC LIMIT 1");
      // The planner's seatStopped (startup), inverted: the node's latest session running, idle or unknown, and its
      // executor not confirmed stopped.
      const running = (nodeId: string): boolean =>
        ["running", "idle", "unknown"].includes((latestStatus.get(nodeId) as { status: string } | undefined)?.status ?? "")
        && !nodeExecutorConfirmedStopped(db, nodeId);
      const slots = new SeatPoolSlots(db);
      const nowIso = new Date(nowMs).toISOString();
      const stoppedSince = db.prepare("SELECT stopped_since FROM seat_pool_slots WHERE id = ?");
      // A retired seat counts as one the pool had unless it was retired before it was ever live (intent 'never_live',
      // SeatPoolSlots.retire: an add or a launch that failed) or before it got a node. The slot's own row, not the job's
      // history (whose retention keeps only the newest evaluations), so the grace holds as long as the row does.
      // T-007545 (SD2): it counts until it stopped running (stopped_since), not until its reap, when that came first;
      // and (B1) a seat the pool started counts only when it ran for SEAT_POOL_MIN_RUN_MS from its start.
      const stopOf = "CASE WHEN s.stopped_since IS NOT NULL AND s.stopped_since < s.retired_at THEN s.stopped_since ELSE s.retired_at END";
      const lastRetired = db.prepare(
        `SELECT MAX(${stopOf}) AS at FROM seat_pool_slots s
          WHERE s.pool_job_id = ? AND s.retired_at IS NOT NULL
            AND (s.op_id IS NULL OR (s.node_id IS NOT NULL AND COALESCE(s.intent, '') <> 'never_live'))
            AND (s.op_id IS NULL OR s.live_at IS NULL OR (julianday(${stopOf}) - julianday(s.live_at)) * 86400000.0 >= ?)`,
      );
      for (const job of jobs) {
        const parsed = parseYaml(job.spec_yaml) as { context?: unknown } | null;
        const context = parsed && typeof parsed.context === "object" && parsed.context !== null ? parsed.context as Record<string, unknown> : {};
        const pool = parseSeatPoolSpec(context);
        if (!("spec" in pool) || pool.spec.drain || !pool.spec.dispatchTag) continue;
        const retired = Date.parse((lastRetired.get(job.job_id, SEAT_POOL_MIN_RUN_MS) as { at: string | null }).at ?? "");
        const registered = Date.parse(job.registered_at);
        let hasSeat = false;
        let lastStoppedMs = -Infinity;
        for (const slot of liveSlots.all(job.job_id) as Array<{ id: number; node_id: string; op_id: string | null; live_at: string | null }>) {
          // A seat seen stopped that runs again starts afresh the next time it stops.
          if (running(slot.node_id)) { hasSeat = true; slots.clearStopped(slot.id); continue; }
          // T-007545 (SD1): kept on the slot the first time (from the session's record of the stop when there is one),
          // so a daemon restart, or the adoption of a seat already stopped, doesn't give it a whole grace again
          slots.markStopped(slot.id, nowIso);
          const since = Date.parse((stoppedSince.get(slot.id) as { stopped_since: string | null } | undefined)?.stopped_since ?? "");
          const stopMs = Number.isFinite(since) ? since : nowMs;
          // T-007545 (B1): a seat the pool started that stopped before it ran for SEAT_POOL_MIN_RUN_MS never counts
          const liveAt = slot.op_id !== null && slot.live_at ? Date.parse(slot.live_at) : NaN;
          if (Number.isFinite(liveAt) && stopMs - liveAt < SEAT_POOL_MIN_RUN_MS) continue;
          lastStoppedMs = Math.max(lastStoppedMs, stopMs);
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
