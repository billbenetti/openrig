import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../src/domain/process-table.js", () => ({ readProcessTable: async () => [] }));

import { currentProcessCaller, withProcessCaller } from "../src/domain/process-caller.js";
import { listNativeProcessesOrThrow, verifyPaneIdleShell } from "../src/domain/native-process-lineage.js";
import { configureSyncSiteRecorder } from "../src/domain/sync-site-wrap.js";
import type { StageFields } from "../src/domain/slow-op-recorder.js";

afterEach(() => configureSyncSiteRecorder(undefined));

function stageRecorder(): Array<{ site: string; caller: StageFields["caller"] }> {
  const stages: Array<{ site: string; caller: StageFields["caller"] }> = [];
  configureSyncSiteRecorder({
    runStage: async (site, fn, _classify, fields) => {
      stages.push({ site, caller: fields?.caller });
      return fn();
    },
  });
  return stages;
}

describe("process caller attribution (telemetry only)", () => {
  it("keeps the outermost caller across awaits and nested entry points, and none outside", async () => {
    expect(currentProcessCaller()).toBeUndefined();
    const seen = await withProcessCaller("reconciler", async () => {
      await new Promise((resolve) => setImmediate(resolve));
      const inner = await withProcessCaller("codex_runtime", async () => currentProcessCaller());
      return [currentProcessCaller(), inner];
    });
    expect(seen).toEqual(["reconciler", "reconciler"]);
    expect(await withProcessCaller("codex_runtime", async () => currentProcessCaller())).toBe("codex_runtime");
    expect(currentProcessCaller()).toBeUndefined();
  });

  it("records the process-table read under its old site name with the entry point that asked", async () => {
    const stages = stageRecorder();
    const tmux = { getPanePid: async () => 4242 };
    await withProcessCaller("claude_delivery", () => listNativeProcessesOrThrow());
    await withProcessCaller("reconciler", () => verifyPaneIdleShell({ target: "%1", tmux }));
    await listNativeProcessesOrThrow();
    expect(stages).toEqual([
      { site: "codex.runtime.list_processes", caller: "claude_delivery" },
      // The idle proof stops at its first sample here (an empty table has no idle shell).
      { site: "codex.runtime.list_processes", caller: "reconciler" },
      { site: "codex.runtime.list_processes", caller: undefined },
    ]);
  });
});
