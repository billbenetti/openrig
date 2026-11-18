/** The arrays a process-table read returned (readProcessTable, readProcTable), whose rows are frozen and shared by
 *  everyone who asked during that read (T-010555). Kept apart from process-table.ts so a reader that asks still
 *  works where that module is replaced (a test's mock of readProcessTable): an array this set never saw is just not
 *  a shared read, and the reader derives from it as before. */
const tableReads = new WeakSet<object>();

/** Marks `rows` as an array a table read returned. */
export function markProcessTableRead(rows: readonly unknown[]): void {
  tableReads.add(rows);
}

/** Whether `rows` is an array a table read returned, whose rows are frozen and shared by everyone who asked during
 *  that read: a reader can then share what it derives from them, once per read. */
export function isProcessTableRead(rows: readonly unknown[]): boolean {
  return tableReads.has(rows);
}
