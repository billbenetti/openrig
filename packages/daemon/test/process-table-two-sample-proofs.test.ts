// Upstream #3146 (billbenetti/openrig#8, B2 of its review): the process table is cached for a short TTL, and a proof
// that compares two samples must still compare two reads. These scenarios keep the cache ON (the hermetic setup turns
// it off) and change the pane's processes between the two samples, through the pane-pid read each sample makes.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readProcessTable } from "../src/domain/process-table.js";
import { claudeHookFromLaunchedProcess, observeClaudeDelivery, verifyClaudePaneProcess, verifyClaudePaneRuntime, verifyCodexPaneProcess } from "../src/domain/native-process-lineage.js";
import { CodexRuntimeAdapter } from "../src/adapters/codex-runtime-adapter.js";
import { restoreProc, serveProcRoot } from "./helpers/fake-proc.js";

const TOKEN = "00000000-0000-7000-8000-00000000000a";
let root = "";

const stat = (pid: number, comm: string, ppid: number, pgrp: number, tpgid: number, ticks = 0) =>
  `${pid} (${comm}) S ${ppid} ${pgrp} ${pgrp} 34816 ${tpgid} 4194560 1 0 0 0 0 0 0 0 20 0 1 0 ${ticks} 1000 200\n`;
const put = (pid: number, line: string, cmdline: string) => {
  fs.mkdirSync(path.join(root, String(pid)), { recursive: true });
  fs.writeFileSync(path.join(root, String(pid), "stat"), line);
  fs.writeFileSync(path.join(root, String(pid), "cmdline"), cmdline);
};
const del = (pid: number) => fs.rmSync(path.join(root, String(pid)), { recursive: true, force: true });

/** A pane whose `change` runs right before the second sample reads the pane's pid. */
const pane = (pid: number, change: () => void) => {
  let calls = 0;
  return { getPanePid: async () => { calls += 1; if (calls === 2) change(); return pid; } };
};

beforeEach(() => {
  vi.stubEnv("OPENRIG_PROCESS_TABLE_TTL_MS", "60000");
  root = fs.mkdtempSync(path.join(os.tmpdir(), "two-samples-"));
  fs.writeFileSync(path.join(root, "stat"), "cpu  1 2 3\nbtime 0\n");
  serveProcRoot(root);
});
afterEach(() => {
  restoreProc();
  vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});

describe("two-sample proofs with the process-table cache on", () => {
  it("verifyCodexPaneProcess: the Codex replaced between the samples is not verified", async () => {
    put(40, stat(40, "bash", 1, 40, 41), "-bash\0");
    put(41, stat(41, "codex", 40, 41, 41, 100), `codex\0resume\0${TOKEN}\0`);
    await readProcessTable(); // the table with Codex 41 is cached
    const tmux = pane(40, () => {
      del(41);
      put(42, stat(42, "codex", 40, 42, 42, 200), `codex\0resume\0${TOKEN}\0`);
      fs.writeFileSync(path.join(root, "40", "stat"), stat(40, "bash", 1, 40, 42));
    });
    expect(await verifyCodexPaneProcess({ target: "%1", tmux, expectedToken: TOKEN, requireResume: true })).toBeNull();
  });

  it("verifyClaudePaneProcess: the Claude replaced between the samples is not verified", async () => {
    put(50, stat(50, "bash", 1, 50, 51), "-bash\0");
    put(51, stat(51, "claude", 50, 51, 51, 100), "claude\0--resume\0tok-1\0");
    await readProcessTable();
    const tmux = pane(50, () => {
      del(51);
      put(52, stat(52, "claude", 50, 52, 52, 300), "claude\0--resume\0tok-1\0");
      fs.writeFileSync(path.join(root, "50", "stat"), stat(50, "bash", 1, 50, 52));
    });
    expect(await verifyClaudePaneProcess({ target: "%1", tmux, expectedToken: "tok-1" })).toBeNull();
  });

  it("verifyClaudePaneRuntime: the runtime replaced between the samples is not stable", async () => {
    put(55, stat(55, "bash", 1, 55, 56), "-bash\0");
    put(56, stat(56, "claude", 55, 56, 56, 100), "claude\0");
    await readProcessTable();
    const tmux = pane(55, () => {
      del(56);
      put(57, stat(57, "claude", 55, 57, 57, 300), "claude\0");
      fs.writeFileSync(path.join(root, "55", "stat"), stat(55, "bash", 1, 55, 57));
    });
    expect(await verifyClaudePaneRuntime({ target: "%1", tmux })).toBeNull();
  });

  it("observeClaudeDelivery at launch: an idle shell whose Claude starts before the 2nd sample is no idle shell", async () => {
    put(60, stat(60, "bash", 1, 60, 60), "-bash\0");
    await readProcessTable(); // the idle shell is cached
    const tmux = pane(60, () => {
      put(61, stat(61, "claude", 60, 61, 61, 100), "claude\0");
      fs.writeFileSync(path.join(root, "60", "stat"), stat(60, "bash", 1, 60, 61));
    });
    const result = await observeClaudeDelivery({ target: "%1", tmux }, { unknownKeepsIdle: false });
    expect(result.state).not.toBe("idle_shell");
  });

  it("captureForkIdentity: the fork child replaced while its thread id is read is not taken", async () => {
    const PARENT = "00000000-0000-7000-8000-00000000000b";
    const CHILD = "00000000-0000-7000-8000-00000000000c";
    put(80, stat(80, "bash", 1, 80, 81), "-bash\0");
    put(81, stat(81, "codex", 80, 81, 81, 100), `codex\0fork\0${PARENT}\0`);
    await readProcessTable(); // the table with child 81 is cached
    const adapter = new CodexRuntimeAdapter({ tmux: { getPanePid: async () => 80 } as never,
      readThreadIdByPid: () => {
        // between the two observations: child 81 exits and child 82 takes the foreground
        del(81);
        put(82, stat(82, "codex", 80, 82, 82, 200), `codex\0fork\0${PARENT}\0`);
        fs.writeFileSync(path.join(root, "80", "stat"), stat(80, "bash", 1, 80, 82));
        return CHILD;
      } });
    expect(await adapter.captureForkIdentity({ tmuxPane: "%1", tmuxSession: "s" } as never, PARENT)).toBeNull();
  });

  it("captureForkIdentity control: the same child before and after the read is taken", async () => {
    const PARENT = "00000000-0000-7000-8000-00000000000b";
    const CHILD = "00000000-0000-7000-8000-00000000000c";
    put(85, stat(85, "bash", 1, 85, 86), "-bash\0");
    put(86, stat(86, "codex", 85, 86, 86, 100), `codex\0fork\0${PARENT}\0`);
    const adapter = new CodexRuntimeAdapter({ tmux: { getPanePid: async () => 85 } as never, readThreadIdByPid: () => CHILD });
    expect(await adapter.captureForkIdentity({ tmuxPane: "%1", tmuxSession: "s" } as never, PARENT))
      .toEqual({ resumeToken: CHILD, resumeType: "codex_id" });
  });

  it("a one-call verify reads after it began (SH-A): a Codex that took the pane after the cached table is seen", async () => {
    put(90, stat(90, "bash", 1, 90, 91), "-bash\0");
    put(91, stat(91, "codex", 90, 91, 91, 100), `codex\0resume\0${TOKEN}\0`);
    await readProcessTable(); // cached: Codex 91
    del(91);
    put(92, stat(92, "codex", 90, 92, 92, 200), `codex\0resume\0${TOKEN}\0`);
    fs.writeFileSync(path.join(root, "90", "stat"), stat(90, "bash", 1, 90, 92));
    const verified = await verifyCodexPaneProcess({ target: "%1", tmux: pane(90, () => {}), expectedToken: TOKEN, requireResume: true });
    expect(verified?.process.pid).toBe(92); // never the cached Codex 91, which has exited
  });

  it("the SessionStart hook reads after it arrived (SH-A): a Claude born after the cached table is its launched process", async () => {
    put(95, stat(95, "bash", 1, 95, 95), "-bash\0");
    await readProcessTable(); // cached before the Claude was born
    put(96, stat(96, "claude", 95, 96, 96, 100), `claude\0--resume\0${TOKEN}\0`);
    put(97, stat(97, "bash", 96, 96, 96, 110), "/bin/sh\0-c\0hook\0");
    fs.writeFileSync(path.join(root, "95", "stat"), stat(95, "bash", 1, 95, 96));
    const launched = await claudeHookFromLaunchedProcess({ target: "%1", tmux: { getPanePid: async () => 95 },
      launchToken: TOKEN, hookPid: 97 });
    expect(launched?.pid).toBe(96);
  });

  it("control: nothing changes between the samples, and the Codex is verified", async () => {
    put(70, stat(70, "bash", 1, 70, 71), "-bash\0");
    put(71, stat(71, "codex", 70, 71, 71, 100), `codex\0resume\0${TOKEN}\0`);
    await readProcessTable();
    const verified = await verifyCodexPaneProcess({ target: "%1", tmux: pane(70, () => {}), expectedToken: TOKEN, requireResume: true });
    expect(verified?.process.pid).toBe(71);
  });
});
