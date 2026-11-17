import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { Worker } from "node:worker_threads";
import { lstart, procRows, type ProcFile, type ProcessTableRow } from "./process-table-rows.js";
import { markProcessTableRead } from "./process-table-reads.js";
import { recordSlowDiagnostic } from "./sync-site-wrap.js";

const execFileAsync = promisify(execFile);

export type { ProcessTableRow } from "./process-table-rows.js";

/** Permission-hidden Linux processes make absence unprovable, without changing the rows a listing exposes. */
export interface ProcessTableCensus {
  rows: ProcessTableRow[];
  permissionHiddenPids: number[];
}

let inFlight: Promise<ProcessTableRow[]> | null = null;
let censusInFlight: Promise<ProcessTableCensus> | null = null;
let procRoot = "/proc";

/**
 * The OS process table, every observer's one source: read from /proc on Linux, where a `ps` would cost the daemon a
 * fork of its whole process, and from one `ps` elsewhere. Callers asking while a read is in flight share it; a later
 * call reads again, so two samples taken one after the other stay two observations. Rejects when the table cannot be
 * read.
 */
export function readProcessTable(): Promise<ProcessTableRow[]> {
  inFlight ??= readProcessTableCensus().then(census => census.rows);
  return inFlight;
}

/** The same shared read, with the omissions relevant to a caller using absence as evidence. */
export function readProcessTableCensus(): Promise<ProcessTableCensus> {
  censusInFlight ??= (process.platform === "linux" ? readProcCensus(procRoot)
    : readPsTable().then(rows => ({ rows, permissionHiddenPids: [] }))).finally(() => {
      censusInFlight = null;
      inFlight = null;
    });
  return censusInFlight;
}

/** Points the Linux read at a /proc of a test's own, so a scenario's processes reach the observers through it; no
 *  argument puts the real /proc back. */
export function setProcRootForTest(root = "/proc"): void {
  procRoot = root;
}

export { isProcessTableRead } from "./process-table-reads.js";

/** A table read: each row frozen (with a Codex row's argv), so the readers that share one read share its rows and
 *  what they derive from them without copying (T-010555). The array stays the caller's, as before (a caller may sort
 *  it); only its rows are shared. */
function frozenTable(rows: ProcessTableRow[]): ProcessTableRow[] {
  for (const row of rows) {
    if (row.argv) Object.freeze(row.argv);
    Object.freeze(row);
  }
  markProcessTableRead(rows);
  return rows;
}

const PS_ROW = /^(\d+)\s+(\d+)\s+(-?\d+)\s+(-?\d+)\s+(.+?)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+(.+)$/;

async function readPsTable(): Promise<ProcessTableRow[]> {
  // lstart is locale-formatted; the child-only C locale keeps the English date the parser expects.
  const { stdout } = await execFileAsync("ps", ["-Ao", "pid,ppid,pgid,tpgid,ucomm,lstart,command"], { encoding: "utf-8", maxBuffer: 8 * 1024 * 1024, env: { ...process.env, LC_ALL: "C" } });
  return frozenTable(stdout.split("\n").slice(1).flatMap((line) => {
    // ucomm may contain spaces on every platform: macOS app helpers (`Slack Helper`), and on
    // Linux task names set by prctl(PR_SET_NAME) or process.title (`tmux: server`,
    // `node (vitest 1)`). lstart always begins with a weekday word and runs to the year, and
    // ucomm (16 bytes at most) is too short to contain such a date, so matching ucomm lazily
    // up to the first date is exact.
    const match = line.trim().match(PS_ROW);
    return match ? [{ pid: Number(match[1]), ppid: Number(match[2]), pgid: Number(match[3]), tpgid: Number(match[4]), executableName: match[5]!, startedAt: match[6]!, command: match[7]! }] : [];
  }));
}

/** Part of a /proc's table, as the reader sends it: the boot time, then the rows of up to PIDS_PER_PART pids, parsed on
 *  the reader's thread. Only when that thread could not load the rows module does a part carry the files instead
 *  (pids with their stat and cmdline, null once the stat could not be read), with why the load failed, and the loop
 *  parses them with the same code. Either kind names the part's pids whose stat /proc hid from this user (EPERM,
 *  EACCES: `hidden`), which the census keeps apart from the processes that exited (v0.7.18). The last part is `done`;
 *  a read that failed whole sends its error instead. */
type ProcPart =
  | { id: number; bootTime: number; rows: ProcessTableRow[]; starts: number[]; hidden: number[]; done: boolean }
  | { id: number; bootTime: number; files: Array<[pid: string, stat: ProcFile, cmdline: ProcFile | null]>; hidden: number[]; loadError: string; done: boolean }
  | { id: number; error: string };

// The reader's thread. It reads every file synchronously, the cheapest way: as many asynchronous reads cost ten times
// that in the thread pool, as much CPU as the `ps` they replace. A file is read as UTF-8, Node's one native call, and
// read again one character a byte only when it holds a byte that is no UTF-8 (rare). It also parses the rows
// (process-table-rows.ts), so the event loop receives finished rows and only joins the parts (T-010555: with the
// parse on the loop, a table read cost the loop a pass over every command line). The table is sent in parts of
// PIDS_PER_PART pids. A pid whose stat cannot be read is left out only when ps leaves it out too: its process is gone
// (ENOENT, ESRCH), or hidden from this user by /proc's hidepid (EPERM, EACCES). Any other failure (out of file
// descriptors, say) fails the read whole, as a `ps` that cannot run does, rather than giving a shorter table that
// looks complete.
const READER_SOURCE = new URL(`data:text/javascript,${encodeURIComponent(String.raw`
  import fs from "node:fs";
  import { parentPort, workerData } from "node:worker_threads";
  const PIDS_PER_PART = 100;
  const STAT_LEFT_OUT = new Set(["ENOENT", "ESRCH", "EPERM", "EACCES"]);
  let rows;

  function read(file) {
    try {
      const text = fs.readFileSync(file, "utf8");
      if (!/[^\x00-\x7f]/.test(text)) return text;
      return text.includes("�") ? fs.readFileSync(file, "latin1") : Buffer.from(text, "utf8").toString("latin1");
    } catch (error) {
      return { code: String(error.code) };
    }
  }

  parentPort.on("message", async ({ id, root }) => {
    // The rows module, loaded once; a failed load sends the files for the loop to parse, with why (the loop names it,
    // T-011108), and the next read tries again. A tsx loader the loop could not resolve fails the load the same way.
    let parse = null;
    let loadError = "";
    try {
      if (workerData.loaderError) throw new Error(workerData.loaderError);
      rows ??= (workerData.url.endsWith(".ts")
        ? import(workerData.loaderUrl).then((api) => api.tsImport(workerData.url, workerData.parent))
        : import(workerData.url)).catch((error) => { rows = undefined; throw error; });
      parse = (await rows).procRowsForLoop;
      if (typeof parse !== "function") throw new Error(workerData.url + " has no procRowsForLoop");
    } catch (error) {
      parse = null;
      loadError = error instanceof Error ? error.message : String(error);
    }
    try {
      const bootTime = Number(/^btime (\d+)$/m.exec(fs.readFileSync(root + "/stat", "utf8"))?.[1]);
      if (!Number.isFinite(bootTime)) throw new Error(root + "/stat has no btime");
      const pids = fs.readdirSync(root).filter((entry) => /^\d+$/.test(entry));
      for (let start = 0; start === 0 || start < pids.length; start += PIDS_PER_PART) {
        const files = pids.slice(start, start + PIDS_PER_PART).map((pid) => {
          const stat = read(root + "/" + pid + "/stat");
          if (typeof stat === "string") return [pid, stat, read(root + "/" + pid + "/cmdline")];
          if (!STAT_LEFT_OUT.has(stat.code)) throw new Error("cannot read " + root + "/" + pid + "/stat: " + stat.code);
          return [pid, stat, null];
        });
        const hidden = files.filter(([, stat]) => typeof stat !== "string" && (stat.code === "EPERM" || stat.code === "EACCES"))
          .map(([pid]) => Number(pid));
        const done = start + PIDS_PER_PART >= pids.length;
        parentPort.postMessage(parse ? { id, bootTime, ...parse(files, bootTime), hidden, done } : { id, bootTime, files, hidden, loadError, done });
      }
    } catch (error) {
      parentPort.postMessage({ id, error: error instanceof Error ? error.message : String(error) });
    }
  });
`)}`);

let reader: Worker | null = null;
let lastAsk = 0;
/** The rows module the reader loads and how the tsx loader is found (for a .ts source); a test replaces them. */
let rowsUrlOverride: string | undefined;
let resolveLoader = (): string => createRequire(import.meta.url).resolve("tsx/esm/api");
const defaultResolveLoader = resolveLoader;
/** Parts received since the reader started, by kind: a test reads that the rows came parsed (T-011108). */
const partsSeen = { rows: 0, files: 0 };
// The parse on the loop is named once a minute at most (a diagnostic record) and once on stderr (T-011108,
// SH-1 of the openrig#27 review: the reader that could not load the rows module sent the files with no sign).
const PARSE_ON_LOOP_RECORD_INTERVAL_MS = 60_000;
let lastParseOnLoopRecord = -Infinity;
let parseOnLoopReported = false;

function parseOnLoop(loadError: string): void {
  const error = loadError.split("\n")[0]!.slice(0, 200);
  if (!parseOnLoopReported) {
    parseOnLoopReported = true;
    console.error(`[process-table] the /proc reader could not load the rows module; the event loop parses the table instead (reported once): ${error}`);
  }
  const at = performance.now();
  if (at - lastParseOnLoopRecord < PARSE_ON_LOOP_RECORD_INTERVAL_MS) return;
  lastParseOnLoopRecord = at;
  recordSlowDiagnostic(Number.POSITIVE_INFINITY, { site: "process-table.parse-on-loop", error });
}

/** Test seam (T-011108): stops the reader, idle (the next read starts another), and forgets the parts and the
 *  parse-on-loop record, so the next one reads with `rowsUrl` as its rows module and `resolveLoader` finding the tsx
 *  loader; no option puts the real ones back. */
export async function restartProcReaderForTest(options: { rowsUrl?: string; resolveLoader?: () => string } = {}): Promise<void> {
  const worker = reader;
  reader = null;
  if (worker) await worker.terminate();
  rowsUrlOverride = options.rowsUrl;
  resolveLoader = options.resolveLoader ?? defaultResolveLoader;
  partsSeen.rows = 0;
  partsSeen.files = 0;
  lastParseOnLoopRecord = -Infinity;
  parseOnLoopReported = false;
}

/** Test seam (T-011108): the parts received since the reader started, by kind. */
export function procTablePartsForTest(): { rows: number; files: number } {
  return { ...partsSeen };
}
const asked = new Map<number, { worker: Worker; census: ProcessTableCensus; resolve: (census: ProcessTableCensus) => void; reject: (error: Error) => void }>();

/** The process table read from /proc (`root` for tests): the rows ps would print, without a process of its own. The
 *  files are read on the reader's thread: a read that waits (a cmdline waits on its process's memory lock) holds that
 *  thread, never the daemon's event loop, which only parses what it is sent. */
export function readProcTable(root = "/proc"): Promise<ProcessTableRow[]> {
  return readProcCensus(root).then(census => census.rows);
}

/** A /proc read whose permission omissions stay distinct from processes that have exited. */
export function readProcCensus(root = "/proc"): Promise<ProcessTableCensus> {
  let worker: Worker;
  try {
    worker = reader ??= startReader();
  } catch (error) {
    // A reader that cannot start rejects the read, as every other failure does, never a throw the callers' .catch
    // misses (T-011108, SH-2 of the openrig#27 review).
    return Promise.reject(error instanceof Error ? error : new Error(String(error)));
  }
  const id = ++lastAsk;
  // Idle, the reader keeps no process up; while an answer is owed it does.
  worker.ref();
  return new Promise((resolve, reject) => {
    asked.set(id, { worker, census: { rows: [], permissionHiddenPids: [] }, resolve, reject });
    worker.postMessage({ id, root });
  });
}

function startReader(): Worker {
  const extension = import.meta.url.endsWith(".ts") ? "ts" : "js";
  const url = rowsUrlOverride ?? new URL(`./process-table-rows.${extension}`, import.meta.url).href;
  // A .ts source (dev, tests) loads the rows module through tsx; without it the reader sends the files and the loop
  // parses them (T-011108, SH-2: the resolve threw here, synchronously, out of every caller's .catch).
  let loaderUrl: string | undefined;
  let loaderError: string | undefined;
  if (url.endsWith(".ts")) {
    try {
      loaderUrl = pathToFileURL(resolveLoader()).href;
    } catch (error) {
      loaderError = `the tsx loader could not be resolved: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  const worker = new Worker(READER_SOURCE, {
    execArgv: [],
    workerData: { url, parent: import.meta.url, loaderUrl, loaderError },
  });
  worker.unref();
  worker.on("message", (part: ProcPart) => {
    const ask = asked.get(part.id);
    if (!ask) return;
    if ("error" in part) {
      asked.delete(part.id);
      if (asked.size === 0) worker.unref();
      ask.reject(new Error(part.error));
      return;
    }
    if ("rows" in part) {
      partsSeen.rows += 1;
      // The start times are formatted here, in the loop's local time (lstart); the rest of each row came parsed.
      part.rows.forEach((row, index) => { row.startedAt = lstart(part.starts[index]!); ask.census.rows.push(row); });
    } else {
      partsSeen.files += 1;
      parseOnLoop(part.loadError);
      for (const row of procRows(part.files, part.bootTime)) ask.census.rows.push(row);
    }
    ask.census.permissionHiddenPids.push(...part.hidden);
    if (!part.done) return;
    asked.delete(part.id);
    if (asked.size === 0) worker.unref();
    ask.resolve({ rows: frozenTable(ask.census.rows), permissionHiddenPids: ask.census.permissionHiddenPids });
  });
  // A reader lost fails what it owed, and only that: its `error` then its `exit` may come after the next read has
  // started another, whose reads are its own.
  const lost = (error: Error) => {
    if (reader === worker) reader = null;
    for (const [id, ask] of asked) {
      if (ask.worker !== worker) continue;
      asked.delete(id);
      ask.reject(error);
    }
  };
  worker.on("error", lost);
  worker.on("exit", (code) => lost(new Error(`the /proc reader exited with code ${code}`)));
  return worker;
}
