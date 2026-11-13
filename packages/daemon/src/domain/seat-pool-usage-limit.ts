/**
 * The seats a seat pool treats as out of provider usage. The provider's reactive signal is fresh for 5 minutes only,
 * while the wake ladder (#2917) parks a usage-limited seat until the reset its message states, which can be hours away.
 * The pool remembers, per seat, the latest reset it read, so a seat stays limited for the pool until that reset: the
 * pool doesn't relaunch it, add or launch seats on its account, or give it a row in the meantime. The memory lives in
 * the daemon process; after a restart inside the window the pool reads the signal again as it comes.
 */
export class SeatPoolUsageLimitMemory {
  private readonly until = new Map<string, number>();

  /** The seats limited now: the ones in `pools` (read now) and the ones whose remembered reset is still ahead. */
  limited(pools: ReadonlyArray<{ seatSessions: readonly string[]; expiresAt: string }>, nowMs: number): ReadonlySet<string> {
    const now = new Set<string>();
    for (const pool of pools) {
      const resetMs = Date.parse(pool.expiresAt);
      for (const session of pool.seatSessions) {
        now.add(session);
        // An unreadable reset limits the seat for this read only.
        if (Number.isFinite(resetMs) && resetMs > nowMs) this.until.set(session, Math.max(this.until.get(session) ?? 0, resetMs));
      }
    }
    for (const [session, resetMs] of this.until) {
      if (resetMs > nowMs) now.add(session);
      else this.until.delete(session);
    }
    return now;
  }
}
