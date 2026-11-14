// T-007930 (token rotation, 1/2): a seat started before the operator bearer rotated still carries the old value in its env
// (OPENRIG_AUTH_BEARER_TOKEN) or in a --bearer it was told. On a 401 the CLI tries again ONCE with the bearer read from a
// file (OPENRIG_AUTH_BEARER_FILE, default ~/.config/setup/openrig-bearer.token), only when that file holds a different
// value. The values here are synthetic; none of them may reach stdout or stderr.
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { DaemonClient, withOperatorBearerRetry, operatorBearerFilePath, isLocalOperatorTarget, type DaemonResponse } from "../src/client.js";
import { allowFetchTarget, resetFetchAllowlist } from "./fetch-guard.js";
import { queueCommand, type QueueDeps } from "../src/commands/queue.js";
import { daemonTeamQueue } from "../src/commands/acp.js";

vi.mock("../src/daemon-lifecycle.js", async () => ({
  ...await vi.importActual("../src/daemon-lifecycle.js"),
  getDaemonStatus: vi.fn(async () => ({ state: "running", healthy: true, port: 7433 })),
  getDaemonUrl: vi.fn(() => "http://selected-daemon:7433"),
}));

const OLD = "synthetic-rotated-away-bearer", NEW = "synthetic-current-bearer-from-file";
/** This home's own daemon as the suite resolves it (no OPENRIG_URL, no daemon.json: the configured 127.0.0.1:7433). */
const LOCAL = "http://127.0.0.1:7433";

describe("withOperatorBearerRetry: one retry on a 401 with the bearer from the file", () => {
  let dir: string;
  let file: string;
  let saved: Record<string, string | undefined>;
  let printed: string[];

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "operator-bearer-retry-"));
    file = path.join(dir, "openrig-bearer.token");
    saved = {};
    for (const k of ["OPENRIG_AUTH_BEARER_TOKEN", "OPENRIG_AUTH_BEARER_FILE", "OPENRIG_TERMINAL_BEARER_TOKEN"]) { saved[k] = process.env[k]; delete process.env[k]; }
    process.env.OPENRIG_AUTH_BEARER_FILE = file;
    printed = [];
    for (const m of ["log", "error", "warn", "info"] as const) {
      vi.spyOn(console, m).mockImplementation((...a: unknown[]) => { printed.push(a.map(String).join(" ")); });
    }
    vi.spyOn(process.stdout, "write").mockImplementation((c: string | Uint8Array) => { printed.push(String(c)); return true; });
    vi.spyOn(process.stderr, "write").mockImplementation((c: string | Uint8Array) => { printed.push(String(c)); return true; });
  });
  afterEach(() => {
    vi.restoreAllMocks();
    fs.rmSync(dir, { recursive: true, force: true });
    for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    // the values never reach any output
    expect(printed.join("\n")).not.toContain(OLD);
    expect(printed.join("\n")).not.toContain(NEW);
  });

  const writeFile = (value: string) => fs.writeFileSync(file, `${value}\n`, { mode: 0o600 });
  /** A daemon whose gate takes only `accepted`; records the Authorization of each call. */
  function gate(accepted: string | null, status = 401) {
    const seen: Array<string | undefined> = [];
    const send = async (headers: Record<string, string>): Promise<DaemonResponse<{ ok?: boolean; error?: string }>> => {
      seen.push(headers.Authorization);
      return headers.Authorization === `Bearer ${accepted}` ? { status: 200, data: { ok: true } } : { status, data: { error: "unauthorized" } };
    };
    return { seen, send };
  }

  it("the env's bearer is refused (401) and the file holds another: retried once with the file's, and it goes through", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    writeFile(NEW);
    const g = gate(NEW);
    const res = await withOperatorBearerRetry(LOCAL, undefined, g.send);
    expect(res.status).toBe(200);
    expect(g.seen).toEqual([`Bearer ${OLD}`, `Bearer ${NEW}`]);
  });

  it("an explicit --bearer refused: the same single retry with the file's bearer", async () => {
    writeFile(NEW);
    const g = gate(NEW);
    expect((await withOperatorBearerRetry(LOCAL, OLD, g.send)).status).toBe(200);
    expect(g.seen).toEqual([`Bearer ${OLD}`, `Bearer ${NEW}`]);
  });

  it("no file: the original 401, one request", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    const g = gate(NEW);
    const res = await withOperatorBearerRetry(LOCAL, undefined, g.send);
    expect(res).toEqual({ status: 401, data: { error: "unauthorized" } });
    expect(g.seen).toEqual([`Bearer ${OLD}`]);
  });

  it("an empty file, or one that can't be read (a directory in its place): the original 401, one request", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    fs.writeFileSync(file, "\n");
    const g = gate(NEW);
    expect((await withOperatorBearerRetry(LOCAL, undefined, g.send)).status).toBe(401);
    fs.rmSync(file);
    fs.mkdirSync(file);
    expect((await withOperatorBearerRetry(LOCAL, undefined, g.send)).status).toBe(401);
    expect(g.seen).toEqual([`Bearer ${OLD}`, `Bearer ${OLD}`]);
  });

  it("the file holds the same value that was refused: no retry", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    writeFile(OLD);
    const g = gate(NEW);
    expect((await withOperatorBearerRetry(LOCAL, undefined, g.send)).status).toBe(401);
    expect(g.seen).toEqual([`Bearer ${OLD}`]);
  });

  it("authz negative: the retry with the file's bearer also gets 401: that 401 comes back, two requests and no loop", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    writeFile(NEW);
    const g = gate("synthetic-neither-of-them");
    const res = await withOperatorBearerRetry(LOCAL, undefined, g.send);
    expect(res.status).toBe(401);
    expect(g.seen).toEqual([`Bearer ${OLD}`, `Bearer ${NEW}`]);
  });

  it("authz negative: a 403 never triggers the retry (only a 401 means the bearer was refused)", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    writeFile(NEW);
    const g = gate(NEW, 403);
    const res = await withOperatorBearerRetry(LOCAL, undefined, g.send);
    expect(res.status).toBe(403);
    expect(g.seen).toEqual([`Bearer ${OLD}`]);
  });

  it("no bearer from --bearer or the env: the call keeps its fallback and a 401 is not retried from the file", async () => {
    writeFile(NEW);
    const none = gate(NEW);
    expect((await withOperatorBearerRetry(LOCAL, undefined, none.send)).status).toBe(401);
    expect(none.seen).toEqual([undefined]);
    process.env.OPENRIG_TERMINAL_BEARER_TOKEN = "synthetic-terminal-token";
    const terminal = gate(NEW);
    expect((await withOperatorBearerRetry(LOCAL, undefined, terminal.send, { fallback: "terminal" })).status).toBe(401);
    expect(terminal.seen).toEqual(["Bearer synthetic-terminal-token"]);
  });

  // S1 of REVISAO-T-008351: a file that is not one line of token characters is no file; its content never reaches a
  // header (fetch would reject it quoting the value), an error or the output.
  it.each([
    ["a second line", `${NEW}\nsynthetic-second-line-of-the-file\n`],
    ["a comment above", `# synthetic comment\n${NEW}\n`],
    ["a space inside", `${NEW} synthetic-tail\n`],
    ["an .env of several keys", `OPENRIG_AUTH_BEARER_TOKEN=${NEW}\nOTHER=synthetic-other\n`],
  ])("authz negative: a file with %s is no file: the original 401, one request", async (_caso, content) => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    fs.writeFileSync(file, content, { mode: 0o600 });
    const g = gate(NEW);
    expect(await withOperatorBearerRetry(LOCAL, undefined, g.send)).toEqual({ status: 401, data: { error: "unauthorized" } });
    expect(g.seen).toEqual([`Bearer ${OLD}`]);
    expect(printed.join("\n")).not.toMatch(/synthetic-(second|comment|tail|other)/);
  });

  // S2 of REVISAO-T-008351: the file's bearer belongs to this home's daemon; a remote OPENRIG_URL (rig-em, --host) or
  // another local daemon never receives it.
  it.each([
    ["a remote host", "http://studio.example:7433"],
    ["a tailnet address", "http://100.64.0.7:7433"],
    ["another local daemon (other port)", "http://127.0.0.1:7434"],
    ["another loopback listener on the same port", "http://127.0.0.2:7433"],
    ["the IPv6 loopback on the same port", "http://[::1]:7433"],
    ["https on the local port", "https://127.0.0.1:7433"],
    ["no URL at all", "not a url"],
  ])("authz negative: a 401 from %s is not retried: one request, the file's value never sent", async (_caso, target) => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    writeFile(NEW);
    const g = gate(NEW);
    expect((await withOperatorBearerRetry(target, undefined, g.send)).status).toBe(401);
    expect(g.seen).toEqual([`Bearer ${OLD}`]);
  });

  it("a daemon recorded on a wildcard address (0.0.0.0): no target counts as its own, so no retry (fails closed)", async () => {
    vi.stubEnv("OPENRIG_HOST", "0.0.0.0");
    try {
      for (const target of [LOCAL, "http://[::1]:7433", "http://0.0.0.0:7433"]) expect(isLocalOperatorTarget(target)).toBe(false);
      process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
      writeFile(NEW);
      const g = gate(NEW);
      expect((await withOperatorBearerRetry("http://[::1]:7433", undefined, g.send)).status).toBe(401);
      expect(g.seen).toEqual([`Bearer ${OLD}`]);
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("isLocalOperatorTarget: the host and port of this home's daemon, and a loopback host", () => {
    for (const ok of [LOCAL, "http://127.0.0.1:7433/", "http://127.0.0.1:7433/api/x"]) expect(isLocalOperatorTarget(ok)).toBe(true);
    for (const no of ["http://127.0.0.1:7434", "http://127.0.0.2:7433", "http://localhost:7433", "http://[::1]:7433",
      "http://studio.example:7433", "http://0.0.0.0:7433", ""]) expect(isLocalOperatorTarget(no)).toBe(false);
  });

  it("the file's path: OPENRIG_AUTH_BEARER_FILE, else ~/.config/setup/openrig-bearer.token", () => {
    expect(operatorBearerFilePath()).toBe(file);
    delete process.env.OPENRIG_AUTH_BEARER_FILE;
    expect(operatorBearerFilePath()).toBe(path.join(os.homedir(), ".config", "setup", "openrig-bearer.token"));
  });
});

// The writes that carry the operator bearer and take the retry, end to end through each command's own client calls (rig
// host approve/deny, rig mode and rig network are covered in their own test files, against the daemon's routes).
describe("the operator writes of rig queue resolve and of the acp answer take the retry", () => {
  let dir: string;
  let saved: Record<string, string | undefined>;
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "operator-bearer-writes-"));
    fs.writeFileSync(path.join(dir, "bearer"), `${NEW}\n`, { mode: 0o600 });
    saved = {};
    for (const k of ["OPENRIG_AUTH_BEARER_TOKEN", "OPENRIG_AUTH_BEARER_FILE", "OPENRIG_SESSION_NAME"]) saved[k] = process.env[k];
    process.env.OPENRIG_AUTH_BEARER_FILE = path.join(dir, "bearer");
    process.env.OPENRIG_SESSION_NAME = "seat@rig";
    process.exitCode = undefined;
  });
  afterEach(() => {
    vi.restoreAllMocks();
    process.exitCode = undefined;
    fs.rmSync(dir, { recursive: true, force: true });
    for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  });
  /** A client whose mission-control gate takes only the file's bearer; records each action call's Authorization. */
  function client() {
    const seen: Array<string | undefined> = [];
    const post = vi.fn(async (p: string, _body?: unknown, options?: { headers?: Record<string, string> }) => {
      if (p !== "/api/mission-control/action") return { status: 404, data: {} };
      seen.push(options?.headers?.Authorization);
      return options?.headers?.Authorization === `Bearer ${NEW}` ? { status: 200, data: { ok: true, qitemId: "qitem-1" } } : { status: 401, data: { error: "unauthorized", message: "bearer token does not match" } };
    });
    const get = vi.fn(async () => ({ status: 200, data: { selfHostId: "selected-host" } }));
    return { seen, c: { baseUrl: LOCAL, post, get } };
  }

  it("rig queue resolve with a rotated-away --bearer: two posts, the 2nd with the file's bearer, and nothing printed with either value", async () => {
    const { seen, c } = client();
    const printed: string[] = [];
    vi.spyOn(console, "log").mockImplementation((...a: unknown[]) => { printed.push(a.map(String).join(" ")); });
    vi.spyOn(console, "error").mockImplementation((...a: unknown[]) => { printed.push(a.map(String).join(" ")); });
    const deps: QueueDeps = { lifecycleDeps: {} as QueueDeps["lifecycleDeps"], clientFactory: () => c as unknown as ReturnType<QueueDeps["clientFactory"]> };
    await queueCommand(deps).parseAsync(["node", "queue", "resolve", "qitem-1", "--decision", "gate passed", "--bearer", OLD, "--json"]);
    expect(seen).toEqual([`Bearer ${OLD}`, `Bearer ${NEW}`]);
    expect(process.exitCode).toBeUndefined();
    expect(printed.join("\n")).not.toMatch(new RegExp(`${OLD}|${NEW}`));
  });

  it("rig queue resolve, authz negative: the file's bearer refused too: one retry, then the 401 is reported", async () => {
    fs.writeFileSync(path.join(dir, "bearer"), "synthetic-neither-of-them\n", { mode: 0o600 });
    const { seen, c } = client();
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    const deps: QueueDeps = { lifecycleDeps: {} as QueueDeps["lifecycleDeps"], clientFactory: () => c as unknown as ReturnType<QueueDeps["clientFactory"]> };
    await queueCommand(deps).parseAsync(["node", "queue", "resolve", "qitem-1", "--decision", "gate passed", "--bearer", OLD, "--json"]);
    expect(seen).toEqual([`Bearer ${OLD}`, "Bearer synthetic-neither-of-them"]);
    expect(process.exitCode).toBe(1);
  });

  it("the acp answer (rig acp serve keeps the env it started with): the rotated-away env bearer is retried with the file's, and the answer lands", async () => {
    process.env.OPENRIG_AUTH_BEARER_TOKEN = OLD;
    const { seen, c } = client();
    expect(await daemonTeamQueue(c as unknown as DaemonClient).answer("qitem-1", "q1", "yes")).toEqual({ ok: true });
    expect(seen).toEqual([`Bearer ${OLD}`, `Bearer ${NEW}`]);
  });

  it("the acp answer with no operator bearer at all: sent without one, as before, and a 401 is not retried", async () => {
    delete process.env.OPENRIG_AUTH_BEARER_TOKEN;
    const { seen, c } = client();
    expect(await daemonTeamQueue(c as unknown as DaemonClient).answer("qitem-1", "q1", "yes")).toEqual({ ok: false, reason: "bearer token does not match" });
    expect(seen).toEqual([undefined]);
  });
});

// S1 of REVISAO-T-008351, end to end with the real fetch: a 2-line bearer file used to reach the Authorization header,
// fetch rejected it quoting the whole value, the DaemonClient wrapped that in "Cannot connect…" and rig queue resolve
// printed it. Now the file is no file: one request, the daemon's 401 reported, nothing of the file in the output.
describe("rig queue resolve against a real loopback daemon that answers 401, with a 2-line bearer file", () => {
  let dir: string;
  let saved: Record<string, string | undefined>;
  let server: http.Server;
  let seen: Array<string | undefined>;
  beforeEach(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "operator-bearer-two-lines-"));
    fs.writeFileSync(path.join(dir, "bearer"), `${NEW}\nsynthetic-second-line-of-the-file\n`, { mode: 0o600 });
    seen = [];
    server = http.createServer((req, res) => {
      if (req.method === "POST" && req.url === "/api/mission-control/action") {
        seen.push(req.headers.authorization);
        res.writeHead(401, { "content-type": "application/json" }).end(JSON.stringify({ error: "unauthorized", message: "bearer token does not match" }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ok: true, selfHostId: "selected-host" }));
    });
    await new Promise<void>((ok) => server.listen(0, "127.0.0.1", ok));
    const port = (server.address() as { port: number }).port;
    allowFetchTarget(`http://127.0.0.1:${port}`);
    saved = {};
    for (const k of ["OPENRIG_AUTH_BEARER_TOKEN", "OPENRIG_AUTH_BEARER_FILE", "OPENRIG_SESSION_NAME", "OPENRIG_PORT", "OPENRIG_URL"]) saved[k] = process.env[k];
    process.env.OPENRIG_AUTH_BEARER_FILE = path.join(dir, "bearer");
    process.env.OPENRIG_SESSION_NAME = "seat@rig";
    process.env.OPENRIG_PORT = String(port); // this server is this home's own daemon (the configured port)
    process.env.OPENRIG_URL = `http://127.0.0.1:${port}`; // and the seat's explicit OPENRIG_URL, as on the VM
    process.exitCode = undefined;
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    process.exitCode = undefined;
    resetFetchAllowlist();
    await new Promise<void>((ok) => server.close(() => ok()));
    fs.rmSync(dir, { recursive: true, force: true });
    for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  });

  it("one request with the --bearer, the 401 reported, and no character of the file in stdout or stderr", async () => {
    const port = (server.address() as { port: number }).port;
    const printed: string[] = [];
    for (const m of ["log", "error", "warn", "info"] as const) {
      vi.spyOn(console, m).mockImplementation((...a: unknown[]) => { printed.push(a.map(String).join(" ")); });
    }
    vi.spyOn(process.stdout, "write").mockImplementation((c: string | Uint8Array) => { printed.push(String(c)); return true; });
    vi.spyOn(process.stderr, "write").mockImplementation((c: string | Uint8Array) => { printed.push(String(c)); return true; });
    const c = new DaemonClient();
    expect(c.baseUrl).toBe(`http://127.0.0.1:${port}`);
    expect(isLocalOperatorTarget(c.baseUrl)).toBe(true);
    const deps: QueueDeps = { lifecycleDeps: {} as QueueDeps["lifecycleDeps"], clientFactory: () => c };
    await queueCommand(deps).parseAsync(["node", "queue", "resolve", "qitem-1", "--decision", "gate passed", "--bearer", OLD, "--json"]);
    expect(seen).toEqual([`Bearer ${OLD}`]);
    expect(process.exitCode).toBe(1);
    const out = printed.join("\n");
    expect(out).not.toContain(NEW);
    expect(out).not.toContain("synthetic-second-line");
    expect(out).not.toContain(OLD);
  });
});
