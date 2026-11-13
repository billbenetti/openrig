import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// c1c of T-005481 (T-005758), N8 of REVISAO-c1b: WIRING PIN of S1 and S2. The seat pool's reader and executor take the
// host's load (hostSample) and the usage-limited sessions (usageLimited) as OPTIONAL deps: without them they fall back,
// silently, to the c1 behaviour (a parked seat over the load limit, or out of usage, still "receives"). The behaviour is
// proved in seat-pool-c1b.test.ts on a harness wired by hand; both objects are built deep in startup.ts (not
// unit-constructable), so, as watchdog-target-gen-wiring.test.ts does, this pins the production wiring at source: a
// refactor that drops either line fails HERE instead of bringing S1 or S2 back with a green CI.
describe("c1c wiring pin: startup gives the seat pool the host's load and the usage-limited sessions", () => {
  const src = readFileSync(fileURLToPath(new URL("../src/startup.ts", import.meta.url)), "utf-8");

  /** The text from `start` to the first `end` after it (both must exist, once). */
  const block = (start: string, end: string): string => {
    const i = src.indexOf(start);
    expect(i, `startup.ts has ${start}`).toBeGreaterThanOrEqual(0);
    expect(src.indexOf(start, i + 1), `${start} appears once`).toBe(-1);
    const j = src.indexOf(end, i);
    expect(j, `${start} ... ${end}`).toBeGreaterThan(i);
    return src.slice(i, j + end.length);
  };

  const reader = () => block("const readPoolState = (", "}, job);");
  const executor = () => block("new SeatPoolExecutor({", "effects: {");

  it("S2: the pool's reader samples the host's load with the launch budget's sampleHost", () => {
    expect(src).toMatch(/import \{[^}]*\bsampleHost\b[^}]*\} from "\.\/domain\/launch-budget\.js";/);
    expect(reader()).toMatch(/\breadSeatPoolState\(\{/);
    expect(reader()).toMatch(/^\s*hostSample: sampleHost,\s*$/m);
  });

  it("S1: the executor reads the usage-limited sessions the plan reads", () => {
    expect(executor()).toMatch(/^\s*usageLimited: usageLimitedSessions,\s*$/m);
    // The plan's read, the same function: the recheck and receives see the limit the plan saw.
    // T-006090: the plan reads the usage, then the busy seats with that same set, and gives both to the reader.
    expect(src).toMatch(/makeSeatPoolPolicy\(\{\s*readState: async \(job\) => \{\s*const usageLimited = await usageLimitedSessions\(\);\s*return readPoolState\(job, usageLimited, await busyPoolSessions\(job, usageLimited\)\);/);
    // The reader turns that set into its usageLimited dep (without it, no seat is ever out of usage).
    expect(reader()).toMatch(/^\s*usageLimited: usageLimited \? \(sessionName\) => usageLimited\.has\(sessionName\) : undefined,\s*$/m);
  });

  it("S1: usageLimitedSessions reads the provider's read model, as the wake ladder does", () => {
    const fn = block("const usageLimitedSessions = async (", "\n    };");
    expect(fn).toMatch(/deps\.providerService\.getReadModel\(\)/);
    expect(fn).toMatch(/deriveUsageLimitPools\(/);
    // The pools read now go through the memory that keeps a seat limited until its stated reset (#2917, 0.7.13).
    expect(fn).toMatch(/seatPoolUsageLimitMemory\.limited\(pools, /);
  });
});
