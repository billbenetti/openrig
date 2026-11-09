import { availableParallelism, freemem, loadavg, platform } from "node:os";

/**
 * The host's room for seat launches, one per daemon and shared by every seat pool it runs.
 *
 * Every pool reads the same load and free memory, so pools evaluated close together would each see room and launch
 * at once. A launch therefore holds a reservation from before it starts until it settles, and every reservation in
 * flight counts against the next one: no more than `maxInFlight` at a time, and each one counts as a runnable process
 * on top of the measured load. A launch also spends one of its watchdog tick's `maxPerTick`, which settling does not
 * give back: the load average and free memory lag a launch that has just settled, so launches one after another, even
 * within one pool's plan, are bounded too. The limits are the daemon's settings (`policies.seat_pool.*`), read at each
 * reservation, not a pool's spec: a host has one budget however many pools share it.
 */

export interface LaunchBudgetLimits {
  /** The most seat launches in flight at once on this daemon, across every pool. */
  maxInFlight: number;
  /** The most seat launches admitted in one watchdog tick, across every pool; 0 admits none. */
  maxPerTick: number;
  /** The free memory below which no launch starts; 0 sets no floor. */
  minFreeMemoryBytes: number;
  /** The highest 1-minute load per CPU a launch may bring the host to, counting each launch in flight, and the one
   *  asked for, as one more runnable process. */
  maxLoadPerCpu: number;
}

export interface HostSample {
  cpuCount: number;
  /** The 1-minute load average; null where the platform has none (Windows), and then load is not checked. */
  load1: number | null;
  /** On Linux the kernel's MemAvailable estimate, as `rig ps --resources` reports it. */
  freeMemoryBytes: number;
}

/** The launches admitted in one watchdog tick. A plan's launches all count against the tick it was planned in. */
export interface LaunchTick {
  admitted: number;
}

export type LaunchRoom =
  | { ok: true; release(): void }
  | { ok: false; why: "launches_per_tick" | "launches_in_flight" | "low_memory" | "high_load"; notes: Record<string, number> };

export const LAUNCH_BUDGET_SETTINGS = {
  maxInFlight: "policies.seat_pool.max_launches_in_flight",
  maxPerTick: "policies.seat_pool.max_launches_per_tick",
  minFreeMemoryMb: "policies.seat_pool.min_free_memory_mb",
  maxLoadPerCpuPercent: "policies.seat_pool.max_load_per_cpu_percent",
} as const;

type LaunchBudgetSetting = typeof LAUNCH_BUDGET_SETTINGS[keyof typeof LAUNCH_BUDGET_SETTINGS];

/** The limits from the daemon's settings, given a reader that returns a key's resolved value. */
export function launchBudgetLimits(setting: (key: LaunchBudgetSetting) => number): LaunchBudgetLimits {
  return {
    maxInFlight: setting(LAUNCH_BUDGET_SETTINGS.maxInFlight),
    maxPerTick: setting(LAUNCH_BUDGET_SETTINGS.maxPerTick),
    minFreeMemoryBytes: setting(LAUNCH_BUDGET_SETTINGS.minFreeMemoryMb) * 1024 * 1024,
    maxLoadPerCpu: setting(LAUNCH_BUDGET_SETTINGS.maxLoadPerCpuPercent) / 100,
  };
}

/** The serving host's CPU count, 1-minute load and free memory, read as `/api/ps/resources` reads them. */
export function sampleHost(): HostSample {
  return { cpuCount: availableParallelism(), load1: platform() === "win32" ? null : loadavg()[0]!, freeMemoryBytes: freemem() };
}

export class LaunchBudget {
  private inFlight = 0;
  private tick: LaunchTick = { admitted: 0 };

  constructor(private readonly deps: { limits(): LaunchBudgetLimits; sample?(): HostSample }) {}

  /** Launches holding a reservation now. */
  get launchesInFlight(): number {
    return this.inFlight;
  }

  /** Starts a new watchdog tick, with none of its launches admitted yet. */
  nextTick(): void {
    this.tick = { admitted: 0 };
  }

  /** The tick a plan evaluated now belongs to; its run passes it to each reservation, however long the run takes. */
  currentTick(): LaunchTick {
    return this.tick;
  }

  /**
   * A reservation for one launch charged to `tick`, or why the host has no room for it. The caller releases it once
   * the launch has settled, however it ended; a second release does nothing. Releasing frees the place in flight, not
   * the tick's admission.
   */
  reserve(tick: LaunchTick = this.tick): LaunchRoom {
    const limits = this.deps.limits();
    if (tick.admitted >= limits.maxPerTick) {
      return { ok: false, why: "launches_per_tick", notes: { thisTick: tick.admitted, maxPerTick: limits.maxPerTick } };
    }
    if (this.inFlight >= limits.maxInFlight) {
      return { ok: false, why: "launches_in_flight", notes: { inFlight: this.inFlight, maxInFlight: limits.maxInFlight } };
    }
    const host = (this.deps.sample ?? sampleHost)();
    if (host.freeMemoryBytes < limits.minFreeMemoryBytes) {
      return { ok: false, why: "low_memory", notes: { freeMemoryBytes: host.freeMemoryBytes, minFreeMemoryBytes: limits.minFreeMemoryBytes } };
    }
    if (host.load1 !== null) {
      const loadPerCpu = (host.load1 + this.inFlight + 1) / host.cpuCount;
      if (loadPerCpu > limits.maxLoadPerCpu) {
        return { ok: false, why: "high_load", notes: { loadPerCpu: Math.round(loadPerCpu * 100) / 100, maxLoadPerCpu: limits.maxLoadPerCpu, inFlight: this.inFlight } };
      }
    }
    tick.admitted++;
    this.inFlight++;
    let held = true;
    return {
      ok: true,
      release: () => {
        if (!held) return;
        held = false;
        this.inFlight--;
      },
    };
  }
}
