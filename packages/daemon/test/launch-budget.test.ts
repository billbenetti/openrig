import { describe, it, expect } from "vitest";
import { LaunchBudget, launchBudgetLimits, type HostSample, type LaunchBudgetLimits } from "../src/domain/launch-budget.js";

const GiB = 1024 * 1024 * 1024;

describe("LaunchBudget", () => {
  const make = (limits: Partial<LaunchBudgetLimits> = {}, host: Partial<HostSample> = {}) => {
    const sample: HostSample = { cpuCount: 4, load1: 0, freeMemoryBytes: 8 * GiB, ...host };
    const budget = new LaunchBudget({
      limits: () => ({ maxInFlight: 2, maxPerTick: 10, minFreeMemoryBytes: GiB, maxLoadPerCpu: 1, ...limits }),
      sample: () => sample,
    });
    return { budget, sample };
  };

  it("counts reservations in flight, refuses past the limit, and frees a place on release, once", () => {
    const { budget } = make();
    const first = budget.reserve();
    const second = budget.reserve();
    expect([first.ok, second.ok]).toEqual([true, true]);
    expect(budget.reserve()).toEqual({ ok: false, why: "launches_in_flight", notes: { inFlight: 2, maxInFlight: 2 } });
    if (!first.ok) throw new Error("unreachable");
    first.release();
    first.release();
    expect(budget.launchesInFlight).toBe(1);
    expect(budget.reserve().ok).toBe(true);
    expect(budget.reserve().ok).toBe(false);
  });

  it("admits a tick's launches up to its limit across releases, renews it with the next tick, and admits none at 0", () => {
    const { budget } = make({ maxPerTick: 2 });
    for (let i = 0; i < 2; i++) {
      const room = budget.reserve();
      if (!room.ok) throw new Error("expected room");
      room.release();
    }
    // Nothing is in flight, but launches one after another spent the tick.
    expect(budget.launchesInFlight).toBe(0);
    expect(budget.reserve()).toEqual({ ok: false, why: "launches_per_tick", notes: { thisTick: 2, maxPerTick: 2 } });
    budget.nextTick();
    expect(budget.reserve().ok).toBe(true);
    expect(make({ maxPerTick: 0 }).budget.reserve()).toEqual({ ok: false, why: "launches_per_tick", notes: { thisTick: 0, maxPerTick: 0 } });
  });

  it("charges a reservation to the tick it is given, so a plan from an earlier tick gets no fresh launches", () => {
    const { budget } = make({ maxPerTick: 1 });
    const planned = budget.currentTick();
    expect(budget.reserve(planned).ok).toBe(true);
    budget.nextTick();
    expect(budget.reserve(planned)).toMatchObject({ ok: false, why: "launches_per_tick" });
    // A refused reservation spends nothing: the new tick still has its launch.
    expect(budget.reserve().ok).toBe(true);
    expect(budget.reserve()).toMatchObject({ ok: false, why: "launches_per_tick" });
  });

  it("refuses below the free-memory floor, and sets no floor at 0", () => {
    expect(make({}, { freeMemoryBytes: GiB - 1 }).budget.reserve())
      .toEqual({ ok: false, why: "low_memory", notes: { freeMemoryBytes: GiB - 1, minFreeMemoryBytes: GiB } });
    expect(make({ minFreeMemoryBytes: 0 }, { freeMemoryBytes: 0 }).budget.reserve().ok).toBe(true);
  });

  it("projects the load with each launch in flight and the one asked for, and skips the check without a load average", () => {
    // 4 CPUs at load 2: the first launch projects 3/4, the second 4/4, the third 5/4.
    const { budget } = make({ maxInFlight: 5 }, { load1: 2 });
    expect(budget.reserve().ok).toBe(true);
    expect(budget.reserve().ok).toBe(true);
    expect(budget.reserve()).toEqual({ ok: false, why: "high_load", notes: { loadPerCpu: 1.25, maxLoadPerCpu: 1, inFlight: 2 } });
    expect(make({}, { load1: null }).budget.reserve().ok).toBe(true);
  });

  it("reads the limits at each reservation, so a changed setting applies to the next launch", () => {
    let maxInFlight = 1;
    const budget = new LaunchBudget({
      limits: () => ({ maxInFlight, maxPerTick: 10, minFreeMemoryBytes: 0, maxLoadPerCpu: 10 }),
      sample: () => ({ cpuCount: 1, load1: 0, freeMemoryBytes: GiB }),
    });
    expect(budget.reserve().ok).toBe(true);
    expect(budget.reserve().ok).toBe(false);
    maxInFlight = 2;
    expect(budget.reserve().ok).toBe(true);
  });

  it("takes its limits from the daemon settings, in megabytes and percent", () => {
    const settings: Record<string, number> = {
      "policies.seat_pool.max_launches_in_flight": 3,
      "policies.seat_pool.max_launches_per_tick": 2,
      "policies.seat_pool.min_free_memory_mb": 512,
      "policies.seat_pool.max_load_per_cpu_percent": 150,
    };
    expect(launchBudgetLimits((key) => settings[key]!)).toEqual({ maxInFlight: 3, maxPerTick: 2, minFreeMemoryBytes: 512 * 1024 * 1024, maxLoadPerCpu: 1.5 });
  });
});
