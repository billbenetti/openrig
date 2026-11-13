import { AsyncLocalStorage } from "node:async_hooks";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Worker } from "node:worker_threads";
import { parseLinuxProcessArguments } from "./native-process-arguments.js";

const execFileAsync = promisify(execFile);

/** One OS process, as `ps -Ao pid,ppid,pgid,tpgid,ucomm,lstart,command` prints it in the C locale. */
export interface ProcessTableRow {
  pid: number;
  ppid: number;
  pgid: number;
  tpgid: number;
  executableName: string;
  /** lstart, in local time: the identity half of pid + start time. */
  startedAt: string;
  command: string;
  /** Linux Codex's exact argv from this same /proc cmdline sample; null when unreadable or malformed. */
  argv?: string[] | null;
}

/**
 * How long a sampled table serves later callers, in ms (upstream #3146): the daemon asked for the whole table about
 * once a second from several observers (the attention reconciler's idle proofs, native identity, resume metadata), and
 * each ask walked /proc again, taking seconds under I/O pressure. A table read less than this long ago is reused.
 * `OPENRIG_PROCESS_TABLE_TTL_MS` overrides it; 0 turns the cache off (the test setup does, since its scenarios rewrite
 * their /proc between reads).
 */
export const PROCESS_TABLE_TTL_MS = 750;

function ttlMs(): number {
  const raw = process.env.OPENRIG_PROCESS_TABLE_TTL_MS;
  const value = raw === undefined || raw.trim() === "" ? NaN : Number(raw);
  return Number.isFinite(value) && value >= 0 ? value : PROCESS_TABLE_TTL_MS;
}

/** A read: its rows and when its walk began (performance.now()); a table is as old as that moment. A walk queued behind
 *  another has not begun (null), so it serves any caller that asks before it begins. */
interface Sample { startedAt: number | null; rows: Promise<ProcessTableRow[]> }

let inFlight: Sample | null = null;
/** The newest walk, begun or queued, settled either way: the next walk begins only after it, so walks never overlap. */
let walking: Promise<void> | null = null;
/** The moment the reads inside readingAfter() must start at or after (performance.now()). */
const readFloor = new AsyncLocalStorage<number>();
let last: { startedAt: number; rows: ProcessTableRow[] } | null = null;
let generation = 0;
let reads = 0;
let procRoot = "/proc";

/**
 * The OS process table, every observer's one source: read from /proc on Linux, where a `ps` would cost the daemon a
 * fork of its whole process, and from one `ps` elsewhere. A table that started less than the TTL ago is reused, and
 * callers asking while a read is in flight share it. One walk runs at a time; a walk asked for while another runs
 * waits its turn, and the callers asking meanwhile share it. A caller that needs a read starting after some moment (a
 * proof's samples, a hook) reads inside readingAfter(); a seat change calls invalidateProcessTable(). The rows are
 * shared among callers: never mutate them. Rejects when the table cannot be read.
 */
export function readProcessTable(): Promise<ProcessTableRow[]> {
  const now = performance.now();
  const ttl = ttlMs();
  const after = readFloor.getStore();
  // A clock that went back (fake timers restart near 0) makes the age negative: such a table is never served. Inside
  // readingAfter() the kept table serves only when its walk started at or after that moment, whatever the TTL says.
  if (last && now >= last.startedAt && (after === undefined ? now - last.startedAt < ttl : last.startedAt >= after)) {
    return Promise.resolve(last.rows);
  }
  // The read in flight serves when it began at or after that moment, or has not begun yet (queued): it will begin later.
  if (inFlight && (after === undefined || inFlight.startedAt === null || inFlight.startedAt >= after)) return inFlight.rows;
  const gen = generation;
  const sample: Sample = { startedAt: null, rows: Promise.resolve([]) };
  const walk = (): Promise<ProcessTableRow[]> => {
    sample.startedAt = performance.now();
    return process.platform === "linux" ? readProcTable(procRoot) : readPsTable();
  };
  const rows = (walking ? walking.then(walk) : walk()).then((table) => {
    // Kept only when no seat changed during the walk and no newer table is kept already.
    // With the cache off (TTL 0) nothing is kept, so no later clock can make a table look fresh.
    const began = sample.startedAt!;
    if (ttl > 0 && gen === generation && (!last || began >= last.startedAt)) last = { startedAt: began, rows: table };
    return table;
  }).finally(() => { if (inFlight === sample) inFlight = null; });
  sample.rows = rows;
  const settled: Promise<void> = rows.then(() => undefined, () => undefined)
    .finally(() => { if (walking === settled) walking = null; });
  walking = settled;
  inFlight = sample;
  reads += 1;
  return rows;
}

/** The process table's clock (performance.now()): the moment a later read must start at or after, for readingAfter(). */
export function processTableClock(): number {
  return performance.now();
}

/**
 * Runs `observe` so that every process-table read inside it (through any lister that reads this table) is a table whose
 * walk started at `after` or later: the kept table or the read in flight when it started then or later, else a new
 * walk. A proof reads its samples so (the second one after the first, the first one after the proof began), and a hook
 * after it arrived, so a table read before the process it looks for was born, or before the first sample, never stands
 * in for an observation. Nothing is dropped for the other callers: they keep sharing the newest read.
 */
export function readingAfter<T>(after: number, observe: () => T): T {
  return readFloor.run(after, observe);
}

/** observe() reading only tables whose walk starts from now on (readingAfter(processTableClock(), observe)). */
export function readingFromNow<T>(observe: () => T): T {
  return readFloor.run(performance.now(), observe);
}

/** Drops the cached table and stops sharing the read in flight: a seat was launched, stopped or replaced, so a table
 *  sampled before that no longer says which processes run. The next caller reads again. */
export function invalidateProcessTable(): void {
  generation += 1;
  last = null;
  inFlight = null;
}

/** How many table reads were started (the cache's own count, for tests and measurements). */
export function processTableReadsForTest(): number {
  return reads;
}

/** Points the Linux read at a /proc of a test's own, so a scenario's processes reach the observers through it; no
 *  argument puts the real /proc back. A table of the previous root is never served for the new one. */
export function setProcRootForTest(root = "/proc"): void {
  procRoot = root;
  invalidateProcessTable();
}

const PS_ROW = /^(\d+)\s+(\d+)\s+(-?\d+)\s+(-?\d+)\s+(.+?)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+(.+)$/;

async function readPsTable(): Promise<ProcessTableRow[]> {
  // lstart is locale-formatted; the child-only C locale keeps the English date the parser expects.
  const { stdout } = await execFileAsync("ps", ["-Ao", "pid,ppid,pgid,tpgid,ucomm,lstart,command"], { encoding: "utf-8", maxBuffer: 8 * 1024 * 1024, env: { ...process.env, LC_ALL: "C" } });
  return stdout.split("\n").slice(1).flatMap((line) => {
    // ucomm may contain spaces on every platform: macOS app helpers (`Slack Helper`), and on
    // Linux task names set by prctl(PR_SET_NAME) or process.title (`tmux: server`,
    // `node (vitest 1)`). lstart always begins with a weekday word and runs to the year, and
    // ucomm (16 bytes at most) is too short to contain such a date, so matching ucomm lazily
    // up to the first date is exact.
    const match = line.trim().match(PS_ROW);
    return match ? [{ pid: Number(match[1]), ppid: Number(match[2]), pgid: Number(match[3]), tpgid: Number(match[4]), executableName: match[5]!, startedAt: match[6]!, command: match[7]! }] : [];
  });
}

// USER_HZ (`getconf CLK_TCK`), the unit of /proc's start times. The kernel fixes it at 100 for user space on every
// architecture Node supports (x64, arm, arm64, ppc64, s390x, riscv64, loong64), whatever its own tick rate; only
// Alpha and IA-64 used another. A constant spares a sysconf call Node does not expose.
const TICKS_PER_SECOND = 100;
const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** ps's lstart (ctime's layout, local time): `Fri Oct  6 09:05:01 2026`. */
function lstart(epochSeconds: number): string {
  const d = new Date(epochSeconds * 1000);
  const two = (n: number) => String(n).padStart(2, "0");
  return `${DAYS[d.getDay()]} ${MONTHS[d.getMonth()]} ${String(d.getDate()).padStart(2, " ")} ${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())} ${d.getFullYear()}`;
}

/** What ps prints in the C locale of a name or command line read one character a byte (latin1): a control character
 *  is a `.`, each byte of a non-ASCII character a `?`. Identity reads the command line as ps prints it: a character
 *  such as a no-break space inside an argument is no separator there. */
function printable(bytes: string): string {
  return bytes.replace(/[\u0000-\u001f\u007f]/g, ".").replace(/[\u0080-\u00ff]/g, "?");
}

/** A file read under /proc: its bytes one character a byte (so names are cut and printed on bytes as ps does), or the
 *  code of the error that kept it from being read. */
type ProcFile = string | { code: string };

/** Part of a /proc's table, as the reader sends it: the boot time, then pids with their stat and cmdline (null once
 *  the stat could not be read, its process gone or hidden). The last part is `done`; a read that failed whole sends
 *  its error instead. */
type ProcPart =
  | { id: number; bootTime: number; files: Array<[pid: string, stat: ProcFile, cmdline: ProcFile | null]>; done: boolean }
  | { id: number; error: string };

// The reader's thread. It reads every file synchronously, the cheapest way: as many asynchronous reads cost ten times
// that in the thread pool, as much CPU as the `ps` they replace. A file is read as UTF-8, Node's one native call, and
// read again one character a byte only when it holds a byte that is no UTF-8 (rare). The table is sent in parts of
// PIDS_PER_PART pids, so the event loop parses it a part at a time. A pid whose stat cannot be read is left out only
// when ps leaves it out too: its process is gone (ENOENT, ESRCH), or hidden from this user by /proc's hidepid (EPERM,
// EACCES). Any other failure (out of file descriptors, say) fails the read whole, as a `ps` that cannot run does,
// rather than giving a shorter table that looks complete.
const READER_SOURCE = String.raw`
  const fs = require("node:fs");
  const { parentPort } = require("node:worker_threads");
  const PIDS_PER_PART = 100;
  const STAT_LEFT_OUT = new Set(["ENOENT", "ESRCH", "EPERM", "EACCES"]);

  function read(file) {
    try {
      const text = fs.readFileSync(file, "utf8");
      if (!/[^\x00-\x7f]/.test(text)) return text;
      return text.includes("�") ? fs.readFileSync(file, "latin1") : Buffer.from(text, "utf8").toString("latin1");
    } catch (error) {
      return { code: String(error.code) };
    }
  }

  parentPort.on("message", ({ id, root }) => {
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
        parentPort.postMessage({ id, bootTime, files, done: start + PIDS_PER_PART >= pids.length });
      }
    } catch (error) {
      parentPort.postMessage({ id, error: error instanceof Error ? error.message : String(error) });
    }
  });
`;

let reader: Worker | null = null;
let lastAsk = 0;
const asked = new Map<number, { worker: Worker; rows: ProcessTableRow[]; resolve: (rows: ProcessTableRow[]) => void; reject: (error: Error) => void }>();

/** The process table read from /proc (`root` for tests): the rows ps would print, without a process of its own. The
 *  files are read on the reader's thread: a read that waits (a cmdline waits on its process's memory lock) holds that
 *  thread, never the daemon's event loop, which only parses what it is sent. */
export function readProcTable(root = "/proc"): Promise<ProcessTableRow[]> {
  const worker = reader ??= startReader();
  const id = ++lastAsk;
  // Idle, the reader keeps no process up; while an answer is owed it does.
  worker.ref();
  return new Promise((resolve, reject) => {
    asked.set(id, { worker, rows: [], resolve, reject });
    worker.postMessage({ id, root });
  });
}

function startReader(): Worker {
  const worker = new Worker(READER_SOURCE, { eval: true, execArgv: [] });
  worker.unref();
  worker.on("message", (part: ProcPart) => {
    const ask = asked.get(part.id);
    if (!ask) return;
    if ("files" in part) {
      for (const [pid, stat, cmdline] of part.files) {
        const row = procRow(pid, stat, cmdline, part.bootTime);
        if (row) ask.rows.push(row);
      }
      if (!part.done) return;
    }
    asked.delete(part.id);
    if (asked.size === 0) worker.unref();
    if ("files" in part) ask.resolve(ask.rows);
    else ask.reject(new Error(part.error));
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

/** The errors of a read under /proc/<pid> once the process has exited. */
const PROCESS_GONE = new Set(["ENOENT", "ESRCH"]);

function procRow(pid: string, stat: ProcFile, cmdlineFile: ProcFile | null, bootTime: number): ProcessTableRow | null {
  // Exited between the listing and the read, or hidden; without its stat ps lists nothing of it either.
  if (typeof stat !== "string" || cmdlineFile === null) return null;
  // Only a process that is gone is dropped. One whose command line cannot be read (denied, say) is still running, and
  // its foreground still counts: ps lists it by its name in brackets.
  if (typeof cmdlineFile !== "string" && PROCESS_GONE.has(cmdlineFile.code)) return null;
  const cmdline = typeof cmdlineFile === "string" ? cmdlineFile : "";
  // `pid (comm) state ppid pgrp session tty_nr tpgid …`: comm may hold spaces and parentheses, so fields count from
  // its last `)`; the start time is the 22nd field, in ticks since boot.
  const close = stat.lastIndexOf(")");
  const fields = stat.slice(close + 2).split(" ");
  if (close < 0 || fields.length < 20) return null;
  const comm = stat.slice(stat.indexOf("(") + 1, close);
  const name = printable(comm);
  // A kernel thread or a zombie has no command line, and an unreadable one shows none: ps shows its name in brackets.
  const command = printable(cmdline.replace(/[\0\n]/g, " ")).trim()
    || `[${name}]${fields[0] === "Z" ? " <defunct>" : ""}`;
  return {
    pid: Number(pid),
    ppid: Number(fields[1]),
    pgid: Number(fields[2]),
    tpgid: Number(fields[5]),
    // The kernel may give a worker thread a longer name; ucomm is the task name's 15 bytes.
    executableName: printable(comm.slice(0, 15)),
    startedAt: lstart(bootTime + Math.floor(Number(fields[19]) / TICKS_PER_SECOND)),
    command,
    ...(name === "codex" ? { argv: typeof cmdlineFile === "string"
      ? parseLinuxProcessArguments(Buffer.from(cmdlineFile, "latin1")) : null } : {}),
  };
}

