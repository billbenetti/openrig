// After the install-wide hook token rotates, a live seat whose env froze the old token gets its activity back, through
// the real relay script, the real ingest route, store and oracle. The relay runs as the hook runs it (node, the payload
// on stdin, the seat's env), against the route served on a local port. The daemon now expects the new token and wrote it
// to activity-endpoint.json; the seat still sends the old one. The seat's activity reaching idle-at-prompt is what the
// gate wake reads (domain/policies/idle-gate-qitem.ts): without it the seat stays unknown and the gate is skipped.
//
// The tokens are random fake values made per test; the new one lives only in a 600 file of this test's own temp dir,
// removed at the end; the relay's env is built from scratch, so no real token of the host reaches it.
//
// T-007930 (rotation 1/2, criterion (3) of T-007931): the same live seat, after both rotations (the hook token and the
// operator bearer), resolves its external: gate with `rig queue resolve`, the real command in a process of its own with
// the seat's frozen env: the old OPENRIG_AUTH_BEARER_TOKEN gets 401 at the mission-control gate, and the CLI retries once
// with the bearer read from OPENRIG_AUTH_BEARER_FILE (a 600 file of the temp dir, never the real default path).

import { afterEach, describe, expect, it } from "vitest";
import { Hono } from "hono";
import { serve, type ServerType } from "@hono/node-server";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import nodePath from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { AgentActivityStore } from "../src/domain/agent-activity-store.js";
import { SeatActivityService } from "../src/domain/seat-activity-service.js";
import { activityRoutes, restoredRunnerHookEvidence } from "../src/routes/activity.js";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { MissionControlActionLog } from "../src/domain/mission-control/mission-control-action-log.js";
import { MissionControlWriteContract } from "../src/domain/mission-control/mission-control-write-contract.js";
import { missionControlRoutes } from "../src/routes/mission-control.js";

const RELAY = nodePath.resolve(nodePath.dirname(fileURLToPath(import.meta.url)),
  "../assets/plugins/openrig-core/hooks/scripts/activity-relay.cjs");
const QUEUE_CLI = pathToFileURL(nodePath.resolve(nodePath.dirname(fileURLToPath(import.meta.url)),
  "../../cli/src/commands/queue.ts")).href;
const SEAT = "dev-giro@giro-rig";
const GATE = "external:ci-giro";
const fake = () => `fake-${randomBytes(16).toString("hex")}`;
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });

/** The daemon after the rotation: the ingest route expects `daemonToken`, the mission-control write gate expects
 *  `operatorBearer`, served on a local port; the seat owns a row blocked on an external: gate. */
async function daemonAfterRotation(daemonToken: string, operatorBearer = fake()) {
  // all the production migrations: the mission-control action log (037/065) is outside createFullTestDb
  const db = createDb();
  migrate(db, ALL_MIGRATIONS);
  cleanups.push(() => db.close());
  const bus = new EventBus(db);
  const queue = new QueueRepository(db, bus, { validateRig: () => true });
  const contract = new MissionControlWriteContract({ db, eventBus: bus, queueRepo: queue, actionLog: new MissionControlActionLog(db) });
  const operatorAuth: string[] = [];
  const rigs = new RigRepository(db);
  const node = rigs.addNode(rigs.createRig("giro-rig").id, "dev.giro", { runtime: "synth" });
  const sessions = new SessionRegistry(db);
  const session = sessions.registerSession(node.id, SEAT);
  sessions.updateStatus(session.id, "running");
  sessions.recordCommunityHarnessSelection(session.id, "synth", true);
  const store = new AgentActivityStore({ db, eventBus: bus,
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
    c.set("eventBus" as never, bus as never);
    c.set("db" as never, db as never);
    c.set("queueRepo" as never, queue as never);
    c.set("missionControlWriteContract" as never, contract as never);
    await next();
  });
  app.get("/healthz", (c) => c.json({}));
  // records each write's Authorization so the test can name the bearer's role without printing it
  app.use("/api/mission-control/action", async (c, next) => { operatorAuth.push(c.req.header("authorization") ?? ""); await next(); });
  app.route("/api/activity", activityRoutes);
  app.route("/api/mission-control", missionControlRoutes({ bearerToken: operatorBearer }));
  const { server, port } = await new Promise<{ server: ServerType; port: number }>((resolve) => {
    const s = serve({ fetch: app.fetch, port: 0, hostname: "127.0.0.1" }, (info) => resolve({ server: s, port: info.port }));
  });
  cleanups.push(() => new Promise<void>((resolve) => server.close(() => resolve())));
  const { qitemId } = await queue.create({ sourceSession: "ci@giro-rig", destinationSession: SEAT, body: "espera o CI", nudge: false });
  queue.update({ qitemId, actorSession: SEAT, state: "blocked", blockedOn: GATE });
  return {
    url: `http://127.0.0.1:${port}`,
    node,
    generation: sessions.currentOccupantTenure(node.id)!.generationUuid,
    activity: () => oracle.getSeatState(node.id)?.activity ?? "unknown",
    qitemId,
    operatorAuth,
    row: () => queue.getById(qitemId),
    resolves: () => (db.prepare("SELECT actor_session FROM mission_control_actions WHERE qitem_id = ? AND action_verb = 'resolve'")
      .all(qitemId) as Array<{ actor_session: string }>).map((r) => r.actor_session),
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

/** `rig queue resolve` as the seat runs it: a process of its own with the seat's frozen env, the real command (its
 *  DaemonClient and withOperatorBearerRetry), loaded from the CLI's source; stdout and stderr are kept to check that no
 *  bearer value reaches them. */
function runResolve(env: Record<string, string>, qitemId: string) {
  const argv = ["node", "queue", "resolve", qitemId, "--decision", "CI verde no sha", "--no-notify", "--json"];
  return new Promise<{ code: number | null; output: string }>((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e",
      `const { queueCommand } = await import(${JSON.stringify(QUEUE_CLI)}); await queueCommand().parseAsync(${JSON.stringify(argv)});`],
    { env, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.on("data", (d) => { output += d; });
    child.stderr.on("data", (d) => { output += d; });
    child.on("error", reject);
    child.on("exit", (code) => resolve({ code: code ?? process.exitCode ?? null, output }));
  });
}

describe("a live seat after both rotations (the hook token and the operator bearer)", () => {
  async function seatWithOldEnv(files: { hook: "new" | "old"; operator: "new" | "old" | "neither" }) {
    const oldHook = fake(), newHook = fake(), oldOperator = fake(), newOperator = fake(), neither = fake();
    const daemon = await daemonAfterRotation(newHook, newOperator);
    const home = homeWithEndpoint(daemon.url, files.hook === "new" ? newHook : oldHook);
    const bearerFile = nodePath.join(home, "openrig-bearer.token");
    fs.writeFileSync(bearerFile, `${{ new: newOperator, old: oldOperator, neither }[files.operator]}\n`, { mode: 0o600 });
    const env = {
      PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: home, OPENRIG_HOME: home, OPENRIG_URL: daemon.url, NODE_NO_WARNINGS: "1",
      OPENRIG_ACTIVITY_HOOK_TOKEN: oldHook, OPENRIG_AUTH_BEARER_TOKEN: oldOperator, // both frozen at launch, before the rotation
      OPENRIG_AUTH_BEARER_FILE: bearerFile, // always explicit: never the default path under a real HOME
      OPENRIG_SESSION_NAME: SEAT, OPENRIG_NODE_ID: daemon.node.id, OPENRIG_RUNTIME: "synth",
      OPENRIG_OCCUPANT_GENERATION: daemon.generation,
    };
    const role = (h: string) => (h === `Bearer ${oldOperator}` ? "env" : h === `Bearer ${newOperator}` ? "file" : "other");
    return { daemon, env, roles: () => daemon.operatorAuth.map(role), secrets: [oldHook, newHook, oldOperator, newOperator, neither] };
  }

  it("the seat reads idle-at-prompt again and resolves its external: gate with rig queue resolve, through the one retry", async () => {
    const { daemon, env, roles, secrets } = await seatWithOldEnv({ hook: "new", operator: "new" });
    expect(await runRelay(env, { hookEvent: "Stop", subtype: "end_turn" })).toBe(0);
    expect(daemon.activity()).toBe("idle-at-prompt");
    const { code, output } = await runResolve(env, daemon.qitemId);
    expect(code).toBe(0);
    expect(roles()).toEqual(["env", "file"]); // 401 with the frozen bearer, then once with the file's
    expect(daemon.row()).toMatchObject({ state: "in-progress", destinationSession: SEAT });
    expect(daemon.resolves()).toEqual([SEAT]);
    for (const s of secrets) expect(output.includes(s)).toBe(false);
  }, 60_000);

  it("authz negative: the file's bearer is refused too: two requests and no more, a 401 reported, the gate stays", async () => {
    const { daemon, env, roles, secrets } = await seatWithOldEnv({ hook: "new", operator: "neither" });
    const { code, output } = await runResolve(env, daemon.qitemId);
    expect(code).not.toBe(0);
    expect(roles()).toEqual(["env", "other"]);
    expect(daemon.row()).toMatchObject({ state: "blocked", blockedOn: GATE });
    expect(daemon.resolves()).toEqual([]);
    for (const s of secrets) expect(output.includes(s)).toBe(false);
  }, 60_000);

  it("control: both files still hold the old values: the seat stays unknown, one refused request, nothing resolved", async () => {
    const { daemon, env, roles } = await seatWithOldEnv({ hook: "old", operator: "old" });
    expect(await runRelay(env, { hookEvent: "Stop", subtype: "end_turn" })).toBe(0);
    expect(daemon.activity()).toBe("unknown");
    const { code } = await runResolve(env, daemon.qitemId);
    expect(code).not.toBe(0);
    expect(roles()).toEqual(["env"]); // the file holds the refused value: no retry
    expect(daemon.row()).toMatchObject({ state: "blocked", blockedOn: GATE });
    expect(daemon.resolves()).toEqual([]);
  }, 60_000);
});
