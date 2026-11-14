// The activity relay after the install-wide hook token rotates. A seat's process keeps the token it was launched with
// (its env froze it), while the daemon rewrites activity-endpoint.json with the token in force. On a 401 for an env token
// that is not a seat grant (oag_), the relay reads the file and posts once more with the token there, only when it is
// a different value and the file names the same daemon. Everything else gets no retry: a grant, a 403, an absent or
// identical file token, a file of another origin, and a 401 on the retry itself (one retry, never a loop). Both
// attempts share the post's 1.5 s budget, so the hook's own timeout holds.
//
// The tokens are random fake values made per test, written only to a 600 file in this test's own temp dir (removed at
// the end); the assertions compare them by role, so no token value is printed even when a test fails.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createRequire } from "node:module";
import { createServer, type Server } from "node:http";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import nodePath from "node:path";

const require = createRequire(import.meta.url);
const relay = require("../assets/plugins/openrig-core/hooks/scripts/activity-relay.cjs") as {
  postHookPayload: (payload: Record<string, unknown>, env: Record<string, string | undefined>) => Promise<void>;
};

const fake = (prefix = "fake-") => `${prefix}${randomBytes(16).toString("hex")}`;

describe("activity relay: the token rotated under a live seat", () => {
  let home: string;
  let server: Server;
  let url: string;
  let sent: string[];
  let answer: (token: string) => { status: number; delayMs?: number };
  let envToken: string, fileToken: string;

  beforeEach(async () => {
    home = fs.mkdtempSync(nodePath.join(os.tmpdir(), "openrig-relay-giro-"));
    envToken = fake();
    fileToken = fake();
    sent = [];
    answer = (token) => ({ status: token === fileToken ? 200 : 401 });
    server = createServer((req, res) => {
      const auth = req.headers.authorization ?? "";
      const token = auth.startsWith("Bearer ") ? auth.slice("Bearer ".length) : "";
      sent.push(token);
      req.resume();
      const { status, delayMs } = answer(token);
      setTimeout(() => { res.writeHead(status, { "content-type": "application/json" }); res.end("{}"); }, delayMs ?? 0);
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    url = `http://127.0.0.1:${typeof address === "object" && address ? address.port : 0}`;
  });

  afterEach(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    fs.rmSync(home, { recursive: true, force: true });
  });

  /** activity-endpoint.json as the daemon writes it: {baseUrl, token}, mode 600. */
  const endpointFile = (token: string | null) => {
    const file = nodePath.join(home, "activity-endpoint.json");
    if (token === null) { fs.rmSync(file, { force: true }); return; }
    fs.writeFileSync(file, JSON.stringify({ baseUrl: url, token }), { mode: 0o600 });
  };
  /** The seat's frozen env; null = launched without a hook token in the env. */
  const seatEnv = (token: string | null = envToken) =>
    ({ OPENRIG_URL: url, OPENRIG_HOME: home, ...(token === null ? {} : { OPENRIG_ACTIVITY_HOOK_TOKEN: token }) });
  /** Which token each request carried, by role: the seat's env token, the file's, or another value. */
  const roles = () => sent.map((t) => (t === envToken ? "env" : t === fileToken ? "file" : "other"));
  const post = (env = seatEnv()) => relay.postHookPayload({ hookEvent: "Stop" }, env);

  it("a 401 for the env token posts once more with the token in activity-endpoint.json", async () => {
    endpointFile(fileToken);
    await post();
    expect(roles()).toEqual(["env", "file"]);
  });

  it("a seat grant (oag_) gets no retry: it is the seat's own credential, not the install-wide token", async () => {
    envToken = fake("oag_");
    endpointFile(fileToken);
    await post(seatEnv(envToken));
    expect(roles()).toEqual(["env"]);
  });

  it("no retry when activity-endpoint.json is absent, has no token, or holds the same value", async () => {
    endpointFile(null);
    await post();
    fs.writeFileSync(nodePath.join(home, "activity-endpoint.json"), JSON.stringify({ baseUrl: url }), { mode: 0o600 });
    await post();
    endpointFile(envToken);
    await post();
    expect(roles()).toEqual(["env", "env", "env"]);
  });

  it("the file's token goes only to the daemon that wrote it: another origin in the seat's env gets no retry", async () => {
    // A process that inherited a seat's env with OPENRIG_URL at another daemon: that daemon's 401 never brings out
    // this host's token.
    fs.writeFileSync(nodePath.join(home, "activity-endpoint.json"),
      JSON.stringify({ baseUrl: "http://127.0.0.1:1", token: fileToken }), { mode: 0o600 });
    await post();
    fs.writeFileSync(nodePath.join(home, "activity-endpoint.json"), JSON.stringify({ token: fileToken }), { mode: 0o600 });
    await post();
    expect(roles()).toEqual(["env", "env"]);
  });

  it("a 401 on the retry ends there: one retry, never a loop", async () => {
    answer = () => ({ status: 401 });
    endpointFile(fileToken);
    await post();
    expect(roles()).toEqual(["env", "file"]);
  });

  it("a 403 gets no retry (only a 401 means the token was not recognized)", async () => {
    answer = () => ({ status: 403 });
    endpointFile(fileToken);
    await post();
    expect(roles()).toEqual(["env"]);
  });

  it("an accepted post reads nothing more", async () => {
    answer = () => ({ status: 200 });
    endpointFile(fileToken);
    await post();
    expect(roles()).toEqual(["env"]);
  });

  it("a token that already came from the file (no env token) is not sent twice", async () => {
    answer = () => ({ status: 401 });
    endpointFile(fileToken);
    await post(seatEnv(null));
    expect(roles()).toEqual(["file"]);
  });

  it("both attempts share the post's 1.5 s budget, so the hook's own timeout still holds", async () => {
    answer = (token) => ({ status: token === fileToken ? 200 : 401, delayMs: token === fileToken ? 5_000 : 1_000 });
    endpointFile(fileToken);
    const started = performance.now();
    await post();
    const took = performance.now() - started;
    expect(roles()).toEqual(["env", "file"]);
    expect(took).toBeLessThan(2_500);
  });
});
