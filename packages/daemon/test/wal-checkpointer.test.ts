// T-010230 volta 1 (REVISAO-openrig-25): the WAL checkpointer only copies ahead of the commit-time autocheckpoint, so
// the WAL stays bounded under continuous writes on the main thread (B1), and a worker that cannot start leaves the
// daemon on the commit-time checkpoint, without throwing (B2). The database lives in a mkdtemp of the TMPDIR.
import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test, vi } from "vitest";
import { createDb, WAL_AUTOCHECKPOINT_PAGES } from "../src/db/connection.js";
import { startWalCheckpointer } from "../src/db/wal-checkpointer.js";

const dirs: string[] = [];
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function novoBanco(): { arq: string; db: ReturnType<typeof createDb> } {
  const dir = mkdtempSync(join(tmpdir(), "walcp-"));
  dirs.push(dir);
  const arq = join(dir, "x.sqlite");
  const db = createDb(arq);
  db.exec("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)");
  return { arq, db };
}

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("wal checkpointer (T-010230 volta 1)", () => {
  test("B1: under continuous writes on the main thread the WAL stays bounded and the commits keep their autocheckpoint", async () => {
    const { arq, db } = novoBanco();
    const ins = db.prepare("INSERT INTO t (v) VALUES (?)");
    const cp = startWalCheckpointer(db, { intervalMs: 200 });
    expect(cp).not.toBeNull();
    const payload = "x".repeat(2000);
    const t0 = Date.now();
    let maxWal = 0;
    let n = 0;
    while (Date.now() - t0 < 8000) {
      ins.run(payload); n++;
      if (n % 20 === 0) {
        const w = existsSync(`${arq}-wal`) ? statSync(`${arq}-wal`).size : 0;
        if (w > maxWal) maxWal = w;
        await sleep(3);
      }
    }
    // ~16 MB of data written; the log resets around 1000 pages (~4 MB), as without the worker (head of #25: linear)
    expect(n * 2000).toBeGreaterThan(12 * 1024 * 1024);
    expect(maxWal).toBeLessThan(8 * 1024 * 1024);
    expect(cp!.offLoop).toBe(true);   // the worker answered its checkpoints
    expect(db.pragma("wal_autocheckpoint", { simple: true })).toBe(WAL_AUTOCHECKPOINT_PAGES);
    await cp!.stop();
    // after the writes, a checkpoint copies the whole log back: nothing left behind
    const row = db.pragma("wal_checkpoint(PASSIVE)")[0] as { busy: number; log: number; checkpointed: number };
    expect(row.busy).toBe(0);
    expect(row.checkpointed).toBe(row.log);
    db.close();
  }, 30_000);

  test("B2: a worker that cannot start returns null, logs once and leaves the autocheckpoint as it was", () => {
    const { db } = novoBanco();
    const erro = vi.spyOn(console, "error").mockImplementation(() => {});
    const cp = startWalCheckpointer(db, {
      createWorker: () => { throw Object.assign(new Error("Worker initialization failure"), { code: "ERR_WORKER_INIT_FAILED" }); },
    });
    expect(cp).toBeNull();
    expect(erro).toHaveBeenCalledTimes(1);
    expect(String(erro.mock.calls[0]?.[0])).toContain("not started");
    expect(db.pragma("wal_autocheckpoint", { simple: true })).toBe(WAL_AUTOCHECKPOINT_PAGES);
    db.prepare("INSERT INTO t (v) VALUES (?)").run("ok");
    db.close();
  });
});
