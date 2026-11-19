// A seat pool's seats are listed under their model seat in `rig ps --nodes`, by the daemon's `seatPoolModel` link.
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import http from "node:http";
import { Command } from "commander";
import { groupSeatPoolSeats, psCommand } from "../src/commands/ps.js";
import { DaemonClient } from "../src/client.js";
import { STATE_FILE, type LifecycleDeps } from "../src/daemon-lifecycle.js";
import type { StatusDeps } from "../src/commands/status.js";

function lifecycleDeps(port: number): LifecycleDeps {
  return {
    spawn: vi.fn(() => ({ pid: 1, unref: vi.fn() }) as never),
    fetch: vi.fn(async () => ({ ok: true })),
    kill: vi.fn(() => true),
    readFile: vi.fn((p: string) => p === STATE_FILE ? JSON.stringify({ pid: 123, port, db: "t.sqlite", startedAt: "2026-11-09T00:00:00Z" }) : null),
    writeFile: vi.fn(),
    removeFile: vi.fn(),
    exists: vi.fn((p: string) => p === STATE_FILE),
    mkdirp: vi.fn(),
    openForAppend: vi.fn(() => 3),
    isProcessAlive: vi.fn(() => true),
  };
}

const PS_DATA = [
  { rigId: "rig-a", name: "delivery", rigName: "delivery", nodeCount: 4, runningCount: 4, status: "running", lifecycleState: "running", uptime: "2h", latestSnapshot: null },
];

const node = (logicalId: string, over: Record<string, unknown> = {}) => ({
  rigId: "rig-a", rigName: "delivery", logicalId, podId: "pod-1", podNamespace: logicalId.split(".")[0],
  canonicalSessionName: `${logicalId.replace(".", "-")}@delivery`, nodeKind: "agent", runtime: "claude-code",
  sessionStatus: "running", startupStatus: "ready", restoreOutcome: "n-a", lifecycleState: "running",
  tmuxAttachCommand: null, resumeCommand: null, latestError: null, hasAssignedWork: false,
  assignedWorkCount: 0, pendingWorkCount: 0, inProgressWorkCount: 0, blockedWorkCount: 0, needsAttention: false,
  ...over,
});

// The daemon lists nodes by creation: the pool's first seat came after another member.
const NODES = [
  node("dev.impl"),
  node("dev.qa"),
  node("dev.impl-s1", { seatPoolModel: "dev.impl" }),
  node("dev.impl-s2", { seatPoolModel: "dev.impl" }),
];

describe("rig ps --nodes groups a seat pool's seats under the model", () => {
  let server: http.Server;
  let port: number;

  beforeAll(async () => {
    server = http.createServer((req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      if (req.url?.startsWith("/api/ps")) res.end(JSON.stringify(PS_DATA));
      else if (req.url?.startsWith("/api/rigs/rig-a/nodes")) res.end(JSON.stringify(NODES));
      else res.end(JSON.stringify([]));
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    port = (server.address() as { port: number }).port;
  });
  afterAll(() => server.close());

  async function ps(...args: string[]): Promise<string[]> {
    const lines: string[] = [];
    const log = vi.spyOn(console, "log").mockImplementation((...a: unknown[]) => { lines.push(a.join(" ")); });
    const err = vi.spyOn(console, "error").mockImplementation((...a: unknown[]) => { lines.push(a.join(" ")); });
    const saved = process.env.OPENRIG_SESSION_NAME;
    delete process.env.OPENRIG_SESSION_NAME;
    try {
      const program = new Command();
      program.exitOverride();
      program.addCommand(psCommand({ lifecycleDeps: lifecycleDeps(port), clientFactory: (url) => new DaemonClient(url) } as StatusDeps));
      await program.parseAsync(["node", "rig", "ps", ...args]);
      return lines;
    } finally {
      log.mockRestore();
      err.mockRestore();
      if (saved !== undefined) process.env.OPENRIG_SESSION_NAME = saved;
      process.exitCode = undefined;
    }
  }

  const sessions = (lines: string[]) => lines.slice(1, 5).map((line) => line.slice(23, 61).trimEnd());

  it("lists each seat right after its model, marked, in the compact table", async () => {
    expect(sessions(await ps("--nodes", "--rig", "delivery"))).toEqual([
      "dev-impl@delivery", "└ dev-impl-s1@delivery", "└ dev-impl-s2@delivery", "dev-qa@delivery",
    ]);
  });

  it("marks the member in the --full table", async () => {
    const members = (await ps("--nodes", "--rig", "delivery", "--full")).slice(1, 5).map((line) => line.slice(42, 56).trimEnd());
    expect(members).toEqual(["impl", "└ impl-s1", "└ impl-s2", "qa"]);
  });

  it("keeps the daemon's order in --json and carries the link on the pool's seats only", async () => {
    const entries = JSON.parse((await ps("--nodes", "--rig", "delivery", "--json"))[0]!) as Array<Record<string, unknown>>;
    expect(entries.map((e) => [e.logicalId, e.seatPoolModel])).toEqual([
      ["dev.impl", undefined], ["dev.qa", undefined], ["dev.impl-s1", "dev.impl"], ["dev.impl-s2", "dev.impl"],
    ]);
  });

  it("leaves a seat unmarked in place when its model isn't listed", async () => {
    const lines = await ps("--nodes", "--rig", "delivery", "--session", "dev-impl-s1@delivery");
    expect(lines.slice(1, 2).map((line) => line.slice(23, 61).trimEnd())).toEqual(["dev-impl-s1@delivery"]);
  });
});

describe("groupSeatPoolSeats", () => {
  const seat = (rigId: string, logicalId: string, seatPoolModel?: string) =>
    ({ rigId, logicalId, ...(seatPoolModel ? { seatPoolModel } : {}) }) as never as Parameters<typeof groupSeatPoolSeats>[0][number];
  const ids = (nodes: ReturnType<typeof groupSeatPoolSeats>) => nodes.map((n) => `${n.rigId}/${n.logicalId}`);

  it("groups by rig as well as name, so two rigs' pools with the same model name stay apart", () => {
    expect(ids(groupSeatPoolSeats([
      seat("a", "dev.impl"), seat("b", "dev.impl"), seat("a", "dev.qa"),
      seat("b", "dev.impl-s1", "dev.impl"), seat("a", "dev.impl-s1", "dev.impl"),
    ]))).toEqual(["a/dev.impl", "a/dev.impl-s1", "b/dev.impl", "b/dev.impl-s1", "a/dev.qa"]);
  });

  it("drops no node when two pools hold each other's models", () => {
    expect(ids(groupSeatPoolSeats([seat("a", "dev.x", "dev.y"), seat("a", "dev.y", "dev.x"), seat("a", "dev.z")])))
      .toEqual(["a/dev.z", "a/dev.x", "a/dev.y"]);
  });
});
