import Database from "better-sqlite3";

/** T-010230: SQLite's own default, now explicit: the connection checkpoints the WAL into the database itself, inside
 *  the commit that crosses this many pages, on the daemon's event loop. The daemon's WAL checkpointer
 *  (db/wal-checkpointer.ts) turns it off while it checkpoints off the loop, and sets it back to this value when it stops
 *  or fails, so the WAL never goes unbounded. */
export const WAL_AUTOCHECKPOINT_PAGES = 1000;

/** T-010230: the WAL file is truncated back to this size when the log is reset (-1, SQLite's default, kept the largest
 *  size it ever reached: 77 MB on 16/11 16:4xZ). */
export const JOURNAL_SIZE_LIMIT_BYTES = 32 * 1024 * 1024;

/**
 * Create a SQLite database connection.
 * Defaults to in-memory for tests. Pass a file path for persistence.
 */
export function createDb(filePath?: string): Database.Database {
  const db = new Database(filePath ?? ":memory:");

  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.pragma(`wal_autocheckpoint = ${WAL_AUTOCHECKPOINT_PAGES}`);
  db.pragma(`journal_size_limit = ${JOURNAL_SIZE_LIMIT_BYTES}`);

  return db;
}
