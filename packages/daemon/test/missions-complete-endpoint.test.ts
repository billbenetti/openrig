// Slice 18 §3.5 — POST /api/missions/:missionId/complete tests.
//
// The endpoint writes `status: complete` to the mission's README.md
// frontmatter. Powers the Mark-complete action on storytelling cards
// (Getting Started complete-and-hide flow). Behavior:
//  - 200 + { missionId, status: "complete" } on success
//  - Creates the frontmatter block when README has no frontmatter
//  - Updates an existing status: X line in place
//  - Adds a status line when frontmatter exists but lacks status
//  - 404 when mission doesn't exist
//  - Idempotent: calling complete twice still succeeds

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import YAML from "yaml";
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
import { missionsRoutes } from "../src/routes/missions.js";
import { slicesRoutes } from "../src/routes/slices.js";

function buildApp(indexer: SliceIndexer): Hono {
  const app = new Hono();
  app.use("*", async (c, next) => {
    c.set("sliceIndexer" as never, indexer);
    await next();
  });
  app.route("/api/missions", missionsRoutes());
  return app;
}

function writeMissionReadme(
  missionsRoot: string,
  missionId: string,
  body: string,
): void {
  const dir = path.join(missionsRoot, missionId);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "README.md"), body);
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

let cleanupRoot: string;
let missionsRoot: string;
let indexer: SliceIndexer;
let db: Database.Database;

beforeEach(() => {
  cleanupRoot = fs.mkdtempSync(path.join(os.tmpdir(), "missions-complete-"));
  missionsRoot = path.join(cleanupRoot, "missions");
  fs.mkdirSync(missionsRoot, { recursive: true });
  // SliceIndexerOpts declares `db` (and dogfoodEvidenceRoot) as REQUIRED and
  // startup always supplies them; this fixture previously omitted db and was
  // only tolerated because the indexer swallowed the resulting error. A real
  // in-memory DB satisfies the constructor contract. It is deliberately NOT
  // migrated: an empty database exercises the structurally-absent
  // queue_items degradation, which is the state this endpoint expects.
  db = createDb(":memory:");
  indexer = new SliceIndexer({ slicesRoot: missionsRoot, dogfoodEvidenceRoot: null, db });
});

afterEach(() => {
  db.close();
  fs.rmSync(cleanupRoot, { recursive: true, force: true });
});

describe("POST /api/missions/:missionId/complete", () => {
  it("completes the authored manifest and reads it back through hot mission and slice APIs", async () => {
    const mission = path.join(missionsRoot, "authored"), slice = path.join(mission, "slices", "intro");
    fs.mkdirSync(slice, { recursive: true });
    const node = "---\nstatus: active\nowner: founder\n---\n# Mission narrative\n";
    fs.writeFileSync(path.join(mission, "SPEC.md"), node);
    fs.writeFileSync(path.join(slice, "slice.yaml"), "kind: slice\nmetadata:\n  id: intro\n  status: draft\n");
    fs.writeFileSync(path.join(slice, "SPEC.md"), "# Intro\n");
    const manifestPath = path.join(mission, "mission.yaml");
    fs.writeFileSync(manifestPath, "# Author comment\nkind: mission\nmetadata:\n  name: authored\n  status: active # Preserve status comment\n  owner: founder\ncomposition:\n  slices:\n    - ref: slices/intro/slice.yaml\n      order: 1\n");
    const beforeManifest = YAML.parse(fs.readFileSync(manifestPath, "utf8"));
    const app = buildAppWithSlices(indexer);
    const primed = await (await app.request("/api/slices?filter=all")).json() as SidecarBody;
    expect(primed.missions.authored!.authoredStatus).toBe("active");
    expect((await app.request("/api/missions/authored/complete", { method: "POST" })).status).toBe(200);
    const detail = await (await app.request("/api/missions/authored")).json();
    expect(detail.status).toBe("complete");
    expect(detail.readiness.historicalStatus).toBe("complete");
    const hot = await (await app.request("/api/slices?filter=all")).json() as SidecarBody;
    expect(hot.missions.authored!.authoredStatus).toBe("complete");
    const persisted = fs.readFileSync(manifestPath, "utf8");
    expect(YAML.parse(persisted)).toEqual({ ...beforeManifest, metadata: { ...beforeManifest.metadata, status: "complete" } });
    expect(persisted).toContain("# Author comment");
    expect(persisted).toContain("# Preserve status comment");
    expect(fs.readFileSync(path.join(mission, "SPEC.md"), "utf8")).toBe(node);
    expect((await app.request("/api/missions/authored/complete", { method: "POST" })).status).toBe(200);
    expect(fs.readFileSync(manifestPath, "utf8")).toBe(persisted);
  });

  it("updates existing status: active to status: complete in README frontmatter", async () => {
    writeMissionReadme(
      missionsRoot,
      "getting-started",
      "---\nid: getting-started\nstatus: active\n---\n# Getting Started\n",
    );
    writeSliceInMission(missionsRoot, "getting-started", "intro");

    const app = buildApp(indexer);
    const res = await app.request("/api/missions/getting-started/complete", { method: "POST" });

    expect(res.status).toBe(200);
    const body = (await res.json()) as { missionId: string; status: string };
    expect(body.missionId).toBe("getting-started");
    expect(body.status).toBe("complete");

    const readme = fs.readFileSync(path.join(missionsRoot, "getting-started", "README.md"), "utf-8");
    expect(readme).toContain("status: complete");
    expect(readme).not.toContain("status: active");
  });

  it("adds status: complete to frontmatter when status field absent", async () => {
    writeMissionReadme(
      missionsRoot,
      "demo-mission",
      "---\nid: demo-mission\n---\n# Demo Mission\n",
    );
    writeSliceInMission(missionsRoot, "demo-mission", "first");

    const app = buildApp(indexer);
    const res = await app.request("/api/missions/demo-mission/complete", { method: "POST" });

    expect(res.status).toBe(200);
    const readme = fs.readFileSync(path.join(missionsRoot, "demo-mission", "README.md"), "utf-8");
    expect(readme).toContain("status: complete");
    expect(readme).toContain("id: demo-mission");
  });

  it("creates a frontmatter block when README has no frontmatter at all", async () => {
    writeMissionReadme(missionsRoot, "no-fm", "# A mission with no frontmatter\n");
    writeSliceInMission(missionsRoot, "no-fm", "only-slice");

    const app = buildApp(indexer);
    const res = await app.request("/api/missions/no-fm/complete", { method: "POST" });

    expect(res.status).toBe(200);
    const readme = fs.readFileSync(path.join(missionsRoot, "no-fm", "README.md"), "utf-8");
    expect(readme.startsWith("---\n")).toBe(true);
    expect(readme).toContain("status: complete");
    expect(readme).toContain("# A mission with no frontmatter");
  });

  it("is idempotent — calling complete twice still returns 200 and status stays complete", async () => {
    writeMissionReadme(
      missionsRoot,
      "idempotent-mission",
      "---\nid: idempotent-mission\nstatus: active\n---\n# Body\n",
    );
    writeSliceInMission(missionsRoot, "idempotent-mission", "s");

    const app = buildApp(indexer);
    const res1 = await app.request("/api/missions/idempotent-mission/complete", { method: "POST" });
    expect(res1.status).toBe(200);
    const res2 = await app.request("/api/missions/idempotent-mission/complete", { method: "POST" });
    expect(res2.status).toBe(200);

    const readme = fs.readFileSync(path.join(missionsRoot, "idempotent-mission", "README.md"), "utf-8");
    const occurrences = (readme.match(/status: complete/g) ?? []).length;
    expect(occurrences).toBe(1);
  });

  it("GET /api/missions/:missionId returns status from frontmatter (slice 18 status surfacing)", async () => {
    writeMissionReadme(
      missionsRoot,
      "has-status",
      "---\nid: has-status\nstatus: complete\n---\n# body\n",
    );
    writeSliceInMission(missionsRoot, "has-status", "s");
    const app = buildApp(indexer);
    const res = await app.request("/api/missions/has-status", { method: "GET" });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string | null };
    expect(body.status).toBe("complete");
  });

  it("GET /api/missions/:missionId returns status=null when frontmatter has no status field", async () => {
    writeMissionReadme(
      missionsRoot,
      "no-status",
      "---\nid: no-status\n---\n# body\n",
    );
    writeSliceInMission(missionsRoot, "no-status", "s");
    const app = buildApp(indexer);
    const res = await app.request("/api/missions/no-status", { method: "GET" });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string | null };
    expect(body.status).toBeNull();
  });

  it("after POST complete, subsequent GET returns status=complete (durable round-trip)", async () => {
    writeMissionReadme(
      missionsRoot,
      "round-trip",
      "---\nid: round-trip\nstatus: active\n---\n# body\n",
    );
    writeSliceInMission(missionsRoot, "round-trip", "s");
    const app = buildApp(indexer);

    const before = await app.request("/api/missions/round-trip", { method: "GET" });
    const beforeBody = (await before.json()) as { status: string | null };
    expect(beforeBody.status).toBe("active");

    const post = await app.request("/api/missions/round-trip/complete", { method: "POST" });
    expect(post.status).toBe(200);

    const after = await app.request("/api/missions/round-trip", { method: "GET" });
    const afterBody = (await after.json()) as { status: string | null };
    expect(afterBody.status).toBe("complete");
  });

  it("returns 404 when mission does not exist", async () => {
    const app = buildApp(indexer);
    const res = await app.request("/api/missions/nonexistent-mission/complete", { method: "POST" });
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("mission_not_found");
  });

  it("returns 503 when SliceIndexer is unavailable", async () => {
    const app = new Hono();
    app.route("/api/missions", missionsRoutes());
    const res = await app.request("/api/missions/anything/complete", { method: "POST" });
    expect(res.status).toBe(503);
  });

  it("preserves unrelated frontmatter fields when updating status", async () => {
    writeMissionReadme(
      missionsRoot,
      "preserves",
      "---\nid: preserves\nworkflow_spec: my-workflow@1\nstatus: active\nlabel: keep me\n---\n# Body\n",
    );
    writeSliceInMission(missionsRoot, "preserves", "s");

    const app = buildApp(indexer);
    const res = await app.request("/api/missions/preserves/complete", { method: "POST" });
    expect(res.status).toBe(200);

    const readme = fs.readFileSync(path.join(missionsRoot, "preserves", "README.md"), "utf-8");
    expect(readme).toContain("workflow_spec: my-workflow@1");
    expect(readme).toContain("label: keep me");
    expect(readme).toContain("status: complete");
    expect(readme).toContain("id: preserves");
  });
});

// ---------------------------------------------------------------------------
// VM-005 B1 — write-side cache coherence (the narrow C-vii exception;
// arch ruling b8d91aee…, plan v1.6.1 §J-1d, guard's repro shape verbatim).
// Out-of-band file writes remain the 60s TTL regime by design — this seam
// covers the daemon's OWN write path only (no watchers, no write-through).
// ---------------------------------------------------------------------------

type SidecarBody = { missions: Record<string, { authoredStatus: string | null; readiness: unknown }> };

const legacyReadiness = {
  name: "relx", state: "legacy", historicalStatus: null, slices: [], issues: [],
  revision: expect.stringMatching(/^[a-f0-9]{64}$/),
};

function buildAppWithSlices(ix: SliceIndexer): Hono {
  const app = new Hono();
  app.use("*", async (c, next) => {
    c.set("sliceIndexer" as never, ix);
    // the list path touches only the indexer; a stub satisfies getDeps
    c.set("sliceDetailProjector" as never, {} as never);
    await next();
  });
  app.route("/api/missions", missionsRoutes());
  app.route("/api/slices", slicesRoutes());
  return app;
}

describe("VM-005 B1 — read-after-write coherence at the daemon's own API", () => {
  it("hot GET /api/slices → POST complete → IMMEDIATE detail + slices BOTH read complete; second POST idempotent", async () => {
    writeMissionReadme(missionsRoot, "relx", "---\nid: relx\nstatus: active\n---\n# Relx\n");
    writeSliceInMission(missionsRoot, "relx", "target");
    const app = buildAppWithSlices(indexer);

    // Prime the sidecar HOT — deliberately NO ?refresh=1 (refresh would
    // full-invalidate and mask the seam; the defect lives on the hot path).
    const primed = await app.request("/api/slices?filter=all");
    expect(primed.status).toBe(200);
    expect(((await primed.json()) as SidecarBody).missions["relx"]).toEqual({ authoredStatus: "active", readiness: legacyReadiness });

    const post = await app.request("/api/missions/relx/complete", { method: "POST" });
    expect(post.status).toBe(200);

    // IMMEDIATELY (no refresh, no TTL wait): both payloads carry the new word.
    const detail = await app.request("/api/missions/relx");
    expect(detail.status).toBe(200);
    expect(((await detail.json()) as { status?: string | null }).status).toBe("complete");
    const hot = await app.request("/api/slices?filter=all");
    expect(((await hot.json()) as SidecarBody).missions["relx"]).toEqual({ authoredStatus: "complete", readiness: legacyReadiness });

    // Idempotent second POST: still 200, sidecar still coherent.
    const again = await app.request("/api/missions/relx/complete", { method: "POST" });
    expect(again.status).toBe(200);
    const hot2 = await app.request("/api/slices?filter=all");
    expect(((await hot2.json()) as SidecarBody).missions["relx"]).toEqual({ authoredStatus: "complete", readiness: legacyReadiness });
  });

  it("NEGATIVE: listing + detail caches SURVIVE the complete-write invalidation (drop-the-blob, never full-flush)", async () => {
    writeMissionReadme(missionsRoot, "relx", "---\nstatus: active\n---\n# Relx\n");
    writeSliceInMission(missionsRoot, "relx", "target");
    const app = buildAppWithSlices(indexer);
    const listBefore = indexer.list(); // primes the listing cache
    const recordBefore = indexer.get("target"); // primes the detail cache
    indexer.missionAuthoredStatuses(); // primes the sidecar
    const post = await app.request("/api/missions/relx/complete", { method: "POST" });
    expect(post.status).toBe(200);
    // Reference equality: the SAME cached instances = those caches survived.
    expect(indexer.list()).toBe(listBefore);
    expect(indexer.get("target")).toBe(recordBefore);
    // ...while the sidecar rebuilt from disk with the new word.
    expect(indexer.missionAuthoredStatuses()["relx"]).toEqual({ authoredStatus: "complete" });
  });
});

describe("POST complete with a cold slice index", () => {
  // A daemon's listing cache expires after 60 s, so a complete usually meets a
  // cold index. Rebuilding the whole listing reads every queue row's body to
  // compute per-slice membership; with tens of thousands of rows that held the
  // event loop for ~30 s per complete. The write only needs the mission folder.
  function seedWorkspace(root: string, missions: number): number {
    let files = 0;
    const write = (rel: string, content: string) => {
      const full = path.join(root, rel);
      fs.mkdirSync(path.dirname(full), { recursive: true });
      fs.writeFileSync(full, content);
      files++;
    };
    for (let m = 0; m < missions; m++) {
      const id = `m-${String(m).padStart(3, "0")}`;
      write(`${id}/mission.yaml`, `kind: mission\nmetadata:\n  name: ${id}\n  status: active\n`);
      write(`${id}/SPEC.md`, `---\nstatus: active\n---\n# ${id}\n`);
      write(`${id}/PROGRESS.md`, `# ${id} progress\n`);
      for (let s = 0; s < 5; s++) {
        const slice = `${id}/slices/${id}-s${s}`;
        write(`${slice}/README.md`, `---\nstatus: active\n---\n# ${id}-s${s}\n`);
        for (const extra of ["PROGRESS.md", "NOTES.md", "proof.md", "plan.md", "log.md"]) {
          write(`${slice}/${extra}`, `# ${extra}\n`);
        }
      }
    }
    return files;
  }

  it("resolves the mission without reading the queue, under 1 s on a ~5,500-file workspace", async () => {
    const queueDb = createDb(":memory:");
    try {
      migrate(queueDb, [coreSchema, eventsSchema, streamItemsSchema, queueItemsSchema]);
      expect(seedWorkspace(missionsRoot, 168)).toBeGreaterThanOrEqual(5500);
      const insert = queueDb.prepare(
        `INSERT INTO queue_items (qitem_id, ts_created, ts_updated, source_session, destination_session, state, priority, body)
         VALUES (?, ?, ?, 'src@r', 'dst@r', 'pending', 'routine', ?)`,
      );
      queueDb.transaction(() => {
        for (let q = 0; q < 2000; q++) {
          const ts = new Date(Date.UTC(2026, 10, 1, 0, 0, q)).toISOString();
          insert.run(`q-${q}`, ts, ts, `work on m-${String(q % 168).padStart(3, "0")}-s${q % 5} `.repeat(20));
        }
      })();
      const cold = new SliceIndexer({ slicesRoot: missionsRoot, dogfoodEvidenceRoot: null, db: queueDb });
      cold.list();
      cold.invalidate();
      const prepare = vi.spyOn(queueDb, "prepare");

      const started = performance.now();
      const res = await buildApp(cold).request("/api/missions/m-080/complete", { method: "POST" });
      const elapsedMs = performance.now() - started;

      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ missionId: "m-080", status: "complete" });
      const queueReads = prepare.mock.calls.map(([sql]) => String(sql)).filter((sql) => /queue_items/.test(sql));
      expect(queueReads).toEqual([]);
      expect(elapsedMs).toBeLessThan(1000);
      const manifest = YAML.parse(fs.readFileSync(path.join(missionsRoot, "m-080", "mission.yaml"), "utf8"));
      expect(manifest.metadata.status).toBe("complete");

      const missing = await buildApp(cold).request("/api/missions/m-999/complete", { method: "POST" });
      expect(missing.status).toBe(404);
      expect(prepare.mock.calls.map(([sql]) => String(sql)).filter((sql) => /queue_items/.test(sql))).toEqual([]);
    } finally {
      queueDb.close();
    }
  });

  it("finds the mission the listing would show when a slice name repeats across missions", async () => {
    writeSliceInMission(missionsRoot, "alpha", "shared");
    writeSliceInMission(missionsRoot, "beta", "shared");
    writeSliceInMission(missionsRoot, "beta", "own");
    writeMissionReadme(missionsRoot, "alpha", "---\nstatus: active\n---\n# Alpha\n");
    writeMissionReadme(missionsRoot, "beta", "---\nstatus: active\n---\n# Beta\n");
    const listed = new Set(indexer.list().map((s) => s.missionId));
    indexer.invalidate();
    const app = buildApp(indexer);
    for (const missionId of ["alpha", "beta"]) {
      const res = await app.request(`/api/missions/${missionId}/complete`, { method: "POST" });
      expect(res.status).toBe(listed.has(missionId) ? 200 : 404);
    }
    expect(fs.readFileSync(path.join(missionsRoot, "beta", "README.md"), "utf8")).toContain("status: complete");
  });
});
