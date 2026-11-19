import { parseLinuxProcessArguments } from "./native-process-arguments.js";

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

// The /proc rows, parsed where the files are read: on the /proc reader's thread (process-table.ts), so the event loop
// only receives finished rows; and on the loop itself only when that thread could not load this module.

// USER_HZ (`getconf CLK_TCK`), the unit of /proc's start times. The kernel fixes it at 100 for user space on every
// architecture Node supports (x64, arm, arm64, ppc64, s390x, riscv64, loong64), whatever its own tick rate; only
// Alpha and IA-64 used another. A constant spares a sysconf call Node does not expose.
const TICKS_PER_SECOND = 100;
const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** ps's lstart (ctime's layout, local time): `Fri Oct  6 09:05:01 2026`. Formatted on the event loop's thread, never on
 *  the reader's: local time is the main thread's (a TZ the daemon or a test sets after the reader started reaches it). */
export function lstart(epochSeconds: number): string {
  const d = new Date(epochSeconds * 1000);
  const two = (n: number) => String(n).padStart(2, "0");
  return `${DAYS[d.getDay()]} ${MONTHS[d.getMonth()]} ${String(d.getDate()).padStart(2, " ")} ${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())} ${d.getFullYear()}`;
}

/** What ps prints in the C locale of a name or command line read one character a byte (latin1): a control character
 *  is a `.`, each byte of a non-ASCII character a `?`. Identity reads the command line as ps prints it: a character
 *  such as a no-break space inside an argument is no separator there. */
function printable(bytes: string): string {
  return bytes.replace(/[\u0000-\u001f\u007f]/g, ".").replace(/[\u0080-ÿ]/g, "?");
}

/** A file read under /proc: its bytes one character a byte (so names are cut and printed on bytes as ps does), or the
 *  code of the error that kept it from being read. */
export type ProcFile = string | { code: string };

/** The errors of a read under /proc/<pid> once the process has exited. */
const PROCESS_GONE = new Set(["ENOENT", "ESRCH"]);

export function procRow(pid: string, stat: ProcFile, cmdlineFile: ProcFile | null, bootTime: number,
  startedAt: (epochSeconds: number) => string = lstart): ProcessTableRow | null {
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
    startedAt: startedAt(bootTime + Math.floor(Number(fields[19]) / TICKS_PER_SECOND)),
    command,
    ...(name === "codex" ? { argv: typeof cmdlineFile === "string"
      ? parseLinuxProcessArguments(Buffer.from(cmdlineFile, "latin1")) : null } : {}),
  };
}

/** The rows of one part of the table, in the order of its files. */
export function procRows(files: Array<[pid: string, stat: ProcFile, cmdline: ProcFile | null]>, bootTime: number): ProcessTableRow[] {
  const rows: ProcessTableRow[] = [];
  for (const [pid, stat, cmdline] of files) {
    const row = procRow(pid, stat, cmdline, bootTime);
    if (row) rows.push(row);
  }
  return rows;
}

/** The reader's form of procRows: every field but startedAt, which stays empty, with each row's start time (seconds since
 *  the epoch) in `starts`, in the same order, for the loop to format with lstart. */
export function procRowsForLoop(files: Array<[pid: string, stat: ProcFile, cmdline: ProcFile | null]>, bootTime: number): { rows: ProcessTableRow[]; starts: number[] } {
  const rows: ProcessTableRow[] = [], starts: number[] = [];
  for (const [pid, stat, cmdline] of files) {
    let start = 0;
    const row = procRow(pid, stat, cmdline, bootTime, (epochSeconds) => { start = epochSeconds; return ""; });
    if (row) { rows.push(row); starts.push(start); }
  }
  return { rows, starts };
}
