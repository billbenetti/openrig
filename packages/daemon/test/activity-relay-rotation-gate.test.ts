// After the install-wide hook token rotates, a live seat whose env froze the old token gets its activity back, through
// the real relay script, the real ingest route, store and oracle. The relay runs as the hook runs it (node, the payload
// on stdin, the seat's env), against the route served on a local port. The daemon now expects the new token and wrote it
// to activity-endpoint.json; the seat still sends the old one. The seat's activity reaching idle-at-prompt is what the
// gate wake reads (domain/policies/idle-gate-qitem.ts): without it the seat stays unknown and the gate is skipped.
//
// The tokens are random fake values made per test; the new one lives only in a 600 file of this test's own temp dir,
// removed at the end; the relay's env is built from scratch, so no real token of the host reaches it.

import { afterEach, describe, expect, it } from "vitest";
import { Hono } from "hono";
import { serve, type ServerType } from "@hono/node-server";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import nodePath from "node:path";
import { fileURLToPath } from "node:url";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { AgentActivityStore } from "../src/domain/agent-activity-store.js";
import { SeatActivityService } from "../src/domain/seat-activity-service.js";
import { activityRoutes, restoredRunnerHookEvidence } from "../src/routes/activity.js";
import { createFullTestDb } from "./helpers/test-app.js";

const RELAY = nodePath.resolve(nodePath.dirname(fileURLToPath(import.meta.url)),
  "../assets/plugins/openrig-core/hooks/scripts/activity-relay.cjs");
const SEAT = "dev-giro@giro-rig";
const fake = () => `fake-${randomBytes(16).toString("hex")}`;
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

/** The daemon after the rotation: the ingest route expects `daemonToken`, served on a local port. */
async function daemonAfterRotation(daemonToken: string) {
  const db = createFullTestDb();
  cleanups.push(() => db.close());
  const rigs = new RigRepository(db);
  const node = rigs.addNode(rigs.createRig("giro-rig").id, "dev.giro", { runtime: "synth" });
  const sessions = new SessionRegistry(db);
  const session = sessions.registerSession(node.id, SEAT);
  sessions.updateStatus(session.id, "running");
  sessions.recordCommunityHarnessSelection(session.id, "synth", true);
  const store = new AgentActivityStore({ db, eventBus: new EventBus(db),
    resolveOccupantGeneration: id => sessions.currentOccupantTenure(id)?.generationUuid ?? null,
    isRegisteredOccupantGeneration: (id, generation) => sessions.isOccupantGenerationRegistered(id, generation) });
  const oracle = new SeatActivityService({ tmux: { readPaneLastActivity: async () => null }, defaultWindowSeconds: 3,
    restoreHookEvidence: (seat, seatRuntime) => restoredRunnerHookEvidence(store, seat, seatRuntime) });
  const app = new Hono();
  app.use("*", async (c, next) => {
    c.set("agentActivityStore" as never, store as never);
    c.set("activityHookToken" as never, daemonToken as never);
    c.set("sessionRegistry" as never, sessions as never);
    c.set("seatActivityService" as never, oracle as never);
    await next();
  });
  app.route("/api/activity", activityRoutes);
  const { server, port } = await new Promise<{ server: ServerType; port: number }>((resolve) => {
    const s = serve({ fetch: app.fetch, port: 0, hostname: "127.0.0.1" }, (info) => resolve({ server: s, port: info.port }));
  });
  cleanups.push(() => new Promise<void>((resolve) => server.close(() => resolve())));
  return {
    url: `http://127.0.0.1:${port}`,
    node,
    generation: sessions.currentOccupantTenure(node.id)!.generationUuid,
    activity: () => oracle.getSeatState(node.id)?.activity ?? "unknown",
  };
}

/** OPENRIG_HOME with activity-endpoint.json as the daemon writes it at start: {baseUrl, token}, mode 600. */
function homeWithEndpoint(baseUrl: string, token: string) {
  const home = fs.mkdtempSync(nodePath.join(os.tmpdir(), "openrig-giro-gate-"));
  cleanups.push(() => fs.rmSync(home, { recursive: true, force: true }));
  fs.writeFileSync(nodePath.join(home, "activity-endpoint.json"), JSON.stringify({ baseUrl, token }), { mode: 0o600 });
  return home;
}

/** The hook as the seat runs it: `node activity-relay.cjs`, the provider payload on stdin, the seat's frozen env. */
function runRelay(env: Record<string, string>, payload: Record<string, unknown>) {
  return new Promise<number | null>((resolve, reject) => {
    const child = spawn(process.execPath, [RELAY], { env, stdio: ["pipe", "ignore", "ignore"] });
    child.on("error", reject);
    child.on("exit", (code) => resolve(code));
    child.stdin.end(JSON.stringify(payload));
  });
}

describe("a live seat after the hook token rotation", () => {
  async function seatWithOldEnv(fileToken: (newToken: string, oldToken: string) => string) {
    const oldToken = fake(), newToken = fake();
    const daemon = await daemonAfterRotation(newToken);
    const home = homeWithEndpoint(daemon.url, fileToken(newToken, oldToken));
    const env = {
      PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: home, OPENRIG_HOME: home, OPENRIG_URL: daemon.url,
      OPENRIG_ACTIVITY_HOOK_TOKEN: oldToken, // frozen at launch, before the rotation
      OPENRIG_SESSION_NAME: SEAT, OPENRIG_NODE_ID: daemon.node.id, OPENRIG_RUNTIME: "synth",
      OPENRIG_OCCUPANT_GENERATION: daemon.generation,
    };
    return { daemon, env };
  }

  it("its Stop hook is accepted again with the rotated token, and the seat reads idle-at-prompt for the gate", async () => {
    const { daemon, env } = await seatWithOldEnv((newToken) => newToken);
    expect(daemon.activity()).toBe("unknown");
    expect(await runRelay(env, { hookEvent: "Stop", subtype: "end_turn" })).toBe(0);
    expect(daemon.activity()).toBe("idle-at-prompt");
  });

  it("control: with the file still holding the old token, the post stays refused and the seat stays unknown", async () => {
    const { daemon, env } = await seatWithOldEnv((_newToken, oldToken) => oldToken);
    expect(await runRelay(env, { hookEvent: "Stop", subtype: "end_turn" })).toBe(0);
    expect(daemon.activity()).toBe("unknown");
  });
});
