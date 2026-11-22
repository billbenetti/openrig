import { Hono } from "hono";
import type { PsProjectionService } from "../domain/ps-projection.js";
import { projectionLane } from "../domain/projection-lane.js";

import { availableParallelism, freemem, loadavg, platform, totalmem } from "node:os";
import { getTranscriptCaptureStats } from "../domain/transcript-rotation.js";

export const psRoutes = new Hono();

// Separate opt-in payload: the ordinary /api/ps bare-array contract is unchanged.
psRoutes.get("/resources", (c) => {
  const psService = c.get("psProjectionService" as never) as PsProjectionService;
  return projectionLane.run(async () => {
    // T-010231: the seats' database read runs on the read worker (off the event loop); the rest is in-memory. Same sum
    // as the upstream's countRunningSeats (#3636, pinned equal in ps-projection.test.ts), without the inventory fold on
    // the loop: on this setup nearly every rig has a running seat, so skipping idle rigs saves almost nothing.
    const entries = await psService.getEntriesOffLoop({ includeArchived: true, archivedOnly: false });
    const cpuCount = availableParallelism();
    const load = platform() === "win32" ? null : loadavg();
    return c.json({
      sampledAt: new Date().toISOString(), cpuCount,
      loadAverage: load, loadPerCpu: load ? load.map((value) => value / cpuCount) : null,
      // On Linux, libuv reports MemAvailable (reclaimable cache counts as free)
      // and falls back to MemFree only on kernels without it.
      memory: { totalBytes: totalmem(), freeBytes: freemem() },
      // Forced archival can hide a rig without stopping its seats. Host load
      // measurements must count those running processes as well.
      runningSeats: entries.reduce((n, entry) => n + entry.runningCount, 0),
      capture: getTranscriptCaptureStats(),
    });
  });
});

psRoutes.get("/", (c) => {
  const psService = c.get("psProjectionService" as never) as PsProjectionService;
  // OPR.0.3.3.19 - default excludes archived; ?includeArchived=true / ?archived=only opt in.
  const includeArchived = c.req.query("includeArchived") === "true";
  const archivedOnly = c.req.query("archived") === "only";
  // slice-04: the whole projection + JSON serialization runs as ONE cooperative
  // lane job (shared with /api/rigs/summary) so a concurrent burst yields the event
  // loop between jobs and /healthz stays responsive. Only query-flag parsing is here.
  // T-010231: the projection's database reads run on the read worker, so the job
  // holds the loop only for the in-memory fold and the JSON.
  return projectionLane.run(async () => {
    const entries = await psService.getEntriesOffLoop({ includeArchived, archivedOnly });
    // #3636: the archived count rides beside the list as a header, so `rig ps` can print its count line from the default
    // (active-only) list instead of folding every archived rig. The bare-array body is unchanged. It's read after the
    // list, so the list stays the route's first read, and a failure in either is the route's existing 500.
    if (typeof psService.countArchivedRigs === "function") c.header("X-OpenRig-Archived-Count", String(psService.countArchivedRigs()));
    return c.json(entries);
  });
});
