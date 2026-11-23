import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// T-018090 (P04-v2 PR-A): WIRING PIN of the refused release's backoff. The reader takes the executor's backoff as an
// OPTIONAL dep (releaseRefusedUntilMs): without it no seat is ever held, and the plan asks a refusing seat again each
// tick (the head-of-line block) with every test of the behaviour still green, since those wire the reader by hand
// (seat-pool-release-refusal-backoff.test.ts). Both objects are built deep in startup.ts, so, as
// seat-pool-c1c-wiring.test.ts does, this pins the production wiring at source.
describe("wiring pin: startup gives the seat pool's reader the executor's release-refusal backoff", () => {
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

  it("the reader asks the pool's executor for each seat's backoff", () => {
    const reader = block("const readPoolState = (", "}, job);");
    expect(reader).toMatch(/\breadSeatPoolState\(\{/);
    expect(reader).toMatch(/^\s*releaseRefusedUntilMs: \(jobId, slot\) => seatPoolExecutor\.releaseRefusedUntilMs\(jobId, slot\),\s*$/m);
  });

  it("the executor tells the daemon's log each refusal (the spec's 1.2.6)", () => {
    const executor = block("new SeatPoolExecutor({", "effects: {");
    expect(executor).toMatch(/^\s*releaseRefusedLog: \(jobId, event\) => console\.warn\(`\[seat-pool\] release_refused job=\$\{jobId\} \$\{JSON\.stringify\(event\)\}`\),\s*$/m);
  });
});
