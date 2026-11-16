// V0.3.1 slice 12 walk-item 1 — mission scope data layer.
//
// GET /api/missions/:missionId returns aggregated mission metadata:
//   - missionId: echo of the requested id
//   - missionPath: absolute filesystem path of the mission folder
//                  (the parent of the slices folder containing the
//                  matched slices)
//   - slices: SliceListEntry[] filtered to entries with this missionId
//
// The route does NOT itself read README.md / PROGRESS.md content —
// the UI fetches those via the existing /api/files/read route through
// the generalized useScopeMarkdown hook. This route is the mission
// METADATA layer; the file content layer is reused.

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Hono } from "hono";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { eventsSchema } from "../src/db/migrations/003_events.js";
import { streamItemsSchema } from "../src/db/migrations/023_stream_items.js";
import { queueItemsSchema } from "../src/db/migrations/024_queue_items.js";
import { SliceIndexer } from "../src/domain/slices/slice-indexer.js";
import { WorkflowSpecCache } from "../src/domain/workflow-spec-cache.js";
import { workflowSpecsSchema } from "../src/db/migrations/033_workflow_specs.js";
import { missionsRoutes } from "../src/routes/missions.js";
import { slicesRoutes } from "../src/routes/slices.js";

function buildApp(
  indexer: SliceIndexer,
  specCache?: WorkflowSpecCache,
): Hono {
  const app = new Hono();
  app.use("*", async (c, next) => {
    c.set("sliceIndexer" as never, indexer);
    c.set("workflowSpecCache" as never, specCache);
    await next();
  });
  app.route("/api/missions", missionsRoutes());
  return app;
}

function writeMissionReadme(
  missionsRoot: string,
  missionId: string,
  frontmatter: Record<string, string> = {},
): void {
  const dir = path.join(missionsRoot, missionId);
  fs.mkdirSync(dir, { recursive: true });
  const fm = Object.entries(frontmatter)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\n");
  fs.writeFileSync(
    path.join(dir, "README.md"),
    `---\n${fm}\n---\n# ${missionId}\n`,
  );
}

function writeSliceInMission(
  missionsRoot: string,
  missionId: string,
  sliceName: string,
  frontmatter: Record<string, string> = {},
): void {
  const dir = path.join(missionsRoot, missionId, "slices", sliceName);
  fs.mkdirSync(dir, { recursive: true });
  const fm = Object.entries(frontmatter)
    .map(([k, v]) => `${k}: ${v}`)
    .join("\n");
  fs.writeFileSync(
    path.join(dir, "README.md"),
    `---\n${fm}\n---\n# ${sliceName}\n`,
  );
}

let db: Database.Database;
let cleanupRoot: string;
let missionsRoot: string;
let indexer: SliceIndexer;
let app: Hono;

beforeEach(() => {
  db = createDb();
  migrate(db, [coreSchema, eventsSchema, streamItemsSchema, queueItemsSchema]);
  cleanupRoot = fs.mkdtempSync(path.join(os.tmpdir(), "missions-routes-"));
  missionsRoot = path.join(cleanupRoot, "missions");
  fs.mkdirSync(missionsRoot, { recursive: true });
  indexer = new SliceIndexer({ slicesRoot: missionsRoot, dogfoodEvidenceRoot: null, db });
  app = buildApp(indexer);
});

afterEach(() => {
  db.close();
  fs.rmSync(cleanupRoot, { recursive: true, force: true });
});

describe("GET /api/missions/:missionId", () => {
  it("returns 200 with missionPath + slices filtered to the requested mission", async () => {
    writeSliceInMission(missionsRoot, "getting-started", "first-slice", { status: "active" });
    writeSliceInMission(missionsRoot, "getting-started", "second-slice", { status: "done" });
    writeSliceInMission(missionsRoot, "other-mission", "third-slice", { status: "active" });

    const res = await app.request("/api/missions/getting-started");
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      missionId: string;
      missionPath: string;
      slices: Array<{ name: string; missionId: string }>;
    };
    expect(body.missionId).toBe("getting-started");
    expect(body.missionPath).toBe(path.join(missionsRoot, "getting-started"));
    expect(body.slices.map((s) => s.name).sort()).toEqual(["first-slice", "second-slice"]);
    expect(body.slices.every((s) => s.missionId === "getting-started")).toBe(true);
  });

  it("returns 404 when no slices exist for the missionId", async () => {
    writeSliceInMission(missionsRoot, "getting-started", "first-slice", { status: "active" });

    const res = await app.request("/api/missions/unknown-mission");
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("mission_not_found");
  });

  it("returns 503 when the indexer isn't wired", async () => {
    const bareApp = new Hono();
    bareApp.route("/api/missions", missionsRoutes());
    const res = await bareApp.request("/api/missions/anything");
    expect(res.status).toBe(503);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("slices_indexer_unavailable");
  });

  it("returns 503 when the indexer is not ready (slices root not configured)", async () => {
    const emptyIndexer = new SliceIndexer({
      slicesRoot: "",
      dogfoodEvidenceRoot: null,
      db,
    });
    const emptyApp = buildApp(emptyIndexer);
    const res = await emptyApp.request("/api/missions/getting-started");
    expect(res.status).toBe(503);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("slices_root_not_configured");
  });

  it("only counts slices with the exact missionId (no substring match)", async () => {
    writeSliceInMission(missionsRoot, "release-0.3.1", "slice-a", { status: "active" });
    writeSliceInMission(missionsRoot, "release-0.3.1-followup", "slice-b", { status: "active" });

    const res = await app.request("/api/missions/release-0.3.1");
    expect(res.status).toBe(200);
    const body = (await res.json()) as { slices: Array<{ name: string }> };
    expect(body.slices.map((s) => s.name)).toEqual(["slice-a"]);
  });

  // V0.3.1 slice 13 walk-item 7 — mission frontmatter workflow_spec
  // declaration + projected topology.
  describe("workflow_spec + topology (slice 13)", () => {
    it("returns workflow_spec from mission README frontmatter when declared", async () => {
      writeMissionReadme(missionsRoot, "getting-started", {
        status: "active",
        workflow_spec: "openrig-velocity@1.0",
      });
      writeSliceInMission(missionsRoot, "getting-started", "first-slice", { status: "active" });

      const res = await app.request("/api/missions/getting-started");
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        workflow_spec: { name: string; version: string } | null;
      };
      expect(body.workflow_spec).toEqual({ name: "openrig-velocity", version: "1.0" });
    });

    it("returns workflow_spec: null when mission README has no declaration", async () => {
      writeMissionReadme(missionsRoot, "plain-mission", { status: "active" });
      writeSliceInMission(missionsRoot, "plain-mission", "child-slice", { status: "active" });

      const res = await app.request("/api/missions/plain-mission");
      const body = (await res.json()) as { workflow_spec: unknown };
      expect(body.workflow_spec).toBeNull();
    });

    it("returns topology.specGraph when workflow_spec declared AND the spec is in the cache", async () => {
      // Migrate workflow_specs table for the spec cache to use.
      migrate(db, [workflowSpecsSchema]);
      const specCache = new WorkflowSpecCache(db);

      // Hand-author a minimal spec file + cache it through.
      const specPath = path.join(cleanupRoot, "openrig-velocity.workflow.yaml");
      fs.writeFileSync(specPath, [
        "workflow:",
        "  id: openrig-velocity",
        "  version: \"1.0\"",
        "  objective: \"Demo\"",
        "  target:",
        "    rig: openrig-velocity",
        "  roles:",
        "    role-a: {}",
        "  entry:",
        "    role: role-a",
        "  steps:",
        "    - id: step-a",
        "      actor_role: role-a",
        "      next_hop:",
        "        suggested_roles: []",
        "",
      ].join("\n"));
      specCache.readThrough(specPath);

      writeMissionReadme(missionsRoot, "mission-with-spec", {
        status: "active",
        workflow_spec: "openrig-velocity@1.0",
      });
      writeSliceInMission(missionsRoot, "mission-with-spec", "slice-a", { status: "active" });
      const appWithCache = buildApp(indexer, specCache);

      const res = await appWithCache.request("/api/missions/mission-with-spec");
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        topology: { specGraph: { specName: string; specVersion: string; nodes: unknown[] } | null };
      };
      expect(body.topology).not.toBeNull();
      expect(body.topology.specGraph).not.toBeNull();
      expect(body.topology.specGraph!.specName).toBe("openrig-velocity");
      expect(body.topology.specGraph!.specVersion).toBe("1.0");
      expect(body.topology.specGraph!.nodes.length).toBeGreaterThan(0);
    });

    it("returns topology: { specGraph: null } when workflow_spec declared but spec NOT in cache", async () => {
      writeMissionReadme(missionsRoot, "unbound-mission", {
        status: "active",
        workflow_spec: "ghost-spec@9.9",
      });
      writeSliceInMission(missionsRoot, "unbound-mission", "child-slice", { status: "active" });

      // No spec cache wired; the route falls through to specGraph: null.
      const res = await app.request("/api/missions/unbound-mission");
      const body = (await res.json()) as {
        workflow_spec: { name: string } | null;
        topology: { specGraph: unknown } | null;
      };
      expect(body.workflow_spec?.name).toBe("ghost-spec");
      expect(body.topology?.specGraph).toBeNull();
    });
  });
});

describe("cold reads right after a complete", () => {
  // The listing cache lives 60 s, so the reads that follow a complete usually
  // rebuild the listing. The rebuild's fallback membership scan reads every
  // queue row; it must stay linear in the queue, whatever the number of slice
  // and mission names it looks for. Names and bodies follow a real workspace:
  // task-numbered mission, slice and rail names (about 1,200 terms, more than
  // a real workspace's ~800), and bodies full of near-misses.
  const missionName = (m: number) => `t${String(9000 + m).padStart(6, "0")}-task-${m}`;
  const sliceName = (m: number, s: number) => `0${s + 1}-${missionName(m)}`;
  const railItem = (m: number, s: number) => `t${String(9000 + m).padStart(6, "0")}-rail-${s + 1}`;

  function seedLegacyWorkspace(root: string, missions: number): number {
    let files = 0;
    const write = (rel: string, content: string) => {
      const full = path.join(root, rel);
      fs.mkdirSync(path.dirname(full), { recursive: true });
      fs.writeFileSync(full, content);
      files++;
    };
    for (let m = 0; m < missions; m++) {
      const id = missionName(m);
      write(`${id}/README.md`, `---\nstatus: active\n---\n# ${id}\n`);
      write(`${id}/PROGRESS.md`, `# ${id} progress\n`);
      write(`${id}/NOTES.md`, `# ${id} notes\n`);
      for (let s = 0; s < 3; s++) {
        const slice = `${id}/slices/${sliceName(m, s)}`;
        write(`${slice}/README.md`, `---\nstatus: active\nrail-item: ${railItem(m, s)}\n---\n# ${sliceName(m, s)}\n`);
        for (const extra of ["PROGRESS.md", "NOTES.md", "design.md", "proof.md", "plan.md", "log.md", "review.md", "handoff.md", "risks.md", "checks.md"]) {
          write(`${slice}/${extra}`, `# ${extra}\n`);
        }
      }
    }
    return files;
  }

  it("GET /api/missions/:id and GET /api/slices answer in under 1 s each with ~1,200 names, ~6,000 files and 20,000 queue rows of ~1 KB", async () => {
    const missions = 168;
    expect(seedLegacyWorkspace(missionsRoot, missions)).toBeGreaterThanOrEqual(5500);
    const filler = "T-009123 to review: the t00 total is 10 tasks, see t009-notes and 01-t0 later; ".repeat(6);
    const insert = db.prepare(
      `INSERT INTO queue_items (qitem_id, ts_created, ts_updated, source_session, destination_session, state, priority, body)
       VALUES (?, ?, ?, 'src@r', 'dst@r', 'pending', 'routine', ?)`,
    );
    const rows = 20_000;
    db.transaction(() => {
      for (let q = 0; q < rows; q++) {
        const ts = new Date(Date.UTC(2026, 10, 1, 0, 0, q)).toISOString();
        // One row in 50 names a slice, in upper case (LIKE folds ASCII).
        const k = q / 50;
        const mention = Number.isInteger(k) ? `Slice ${sliceName(k % missions, k % 3).toUpperCase()} is done.` : "Routine status.";
        insert.run(`q-${String(q).padStart(5, "0")}`, ts, ts, `${filler}${mention}\n${filler}`);
      }
    })();
    const app = new Hono();
    app.use("*", async (c, next) => {
      c.set("sliceIndexer" as never, indexer);
      c.set("sliceDetailProjector" as never, {} as never);
      await next();
    });
    app.route("/api/missions", missionsRoutes());
    app.route("/api/slices", slicesRoutes());

    const target = missionName(80);
    expect((await app.request(`/api/missions/${target}/complete`, { method: "POST" })).status).toBe(200);
    indexer.invalidate();
    let started = performance.now();
    const detail = await app.request(`/api/missions/${target}`);
    const detailMs = performance.now() - started;
    indexer.invalidate();
    started = performance.now();
    const listing = await app.request("/api/slices?filter=all");
    const listingMs = performance.now() - started;

    expect(detail.status).toBe(200);
    const body = await detail.json() as { status: string; slices: Array<{ name: string; qitemCount: number }> };
    expect(body.status).toBe("complete");
    // Legacy mission: each slice also matches on the mission id, so every row
    // that names any slice of the mission belongs to all three of its slices.
    const expected = Array.from({ length: rows / 50 }, (_, k) => k).filter((k) => k % missions === 80).length;
    expect(expected).toBe(2);
    expect(body.slices.map((s) => [s.name, s.qitemCount])).toEqual(
      [0, 1, 2].map((s) => [sliceName(80, s), expected]),
    );
    expect(listing.status).toBe(200);
    const listed = await listing.json() as { slices: Array<{ name: string; qitemCount: number }> };
    expect(listed.slices.filter((s) => s.name.endsWith(target)).map((s) => [s.name, s.qitemCount]).sort()).toEqual(
      [0, 1, 2].map((s) => [sliceName(80, s), expected]),
    );
    expect(detailMs).toBeLessThan(1000);
    expect(listingMs).toBeLessThan(1000);
  });
});
