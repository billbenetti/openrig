// The read worker as the installed daemon runs it (T-010440, SH-1 of REVISAO-T-010374): from dist, the worker loads
// read-off-loop-jobs.js, never the .ts the rest of the suite runs through tsx. A worker that cannot load that file
// falls back to the synchronous read in silence, so this read fails the test instead of falling back: its `sync`
// throws. It needs the build (`npm run build`); the CI builds before the package tests, and there a missing dist is a
// failure, not a skip.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { readRigSummaryEntries } from "../src/domain/rig-summary-projection.js";
import { seedHostShaped } from "./helpers/seed-host-shaped.js";

const DIST = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../dist/domain/read-off-loop.js");
const built = fs.existsSync(DIST) && fs.existsSync(path.join(path.dirname(DIST), "read-off-loop-jobs.js"));

describe.skipIf(!built && !process.env.CI)("the read worker from dist", () => {
  let dir: string;
  let db: Database.Database;
  let dist: typeof import("../src/domain/read-off-loop.js");

  beforeAll(async () => {
    expect(built, `the build is missing: ${DIST} (run npm run build)`).toBe(true);
    dist = await import(pathToFileURL(DIST).href);
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "read-off-loop-dist-"));
    db = createDb(path.join(dir, "openrig.sqlite"));
    migrate(db, ALL_MIGRATIONS);
    seedHostShaped(db);
  });

  afterAll(async () => {
    await dist?.stopReadOffLoopWorker();
    db?.close();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  });

  it("loads read-off-loop-jobs.js in the worker and answers there, without the synchronous fallback", async () => {
    const filter = { includeArchived: true };
    const value = await dist.readOffLoop(db, "rigSummaries", { filter }, () => {
      throw new Error("the read fell back to the main thread: the worker did not load from dist");
    }, { site: "test.dist.read", record: "always" });
    expect(value).toEqual(readRigSummaryEntries(db, filter));
    expect(value.length).toBeGreaterThan(0);
  });
});
