export type SqliteValue = string | number | bigint | Uint8Array | null;

/**
 * The standard connection interface the Adapter expects.
 */
export interface SqliteDatabase {
  run(sql: string, params: SqliteValue[]): Promise<{ changes: number }>;
  get(sql: string, params: SqliteValue[]): Promise<Record<string, unknown> | null>;
  all(sql: string, params: SqliteValue[]): Promise<Record<string, unknown>[]>;
}

/**
 * Represents a raw native SQLite driver (like Bun or better-sqlite3).
 */
export interface NativeSqliteStatement {
  run(...params: SqliteValue[]): unknown;
  get(...params: SqliteValue[]): unknown;
  all(...params: SqliteValue[]): unknown;
}

export interface NativeSqliteDriver {
  prepare(sql: string): NativeSqliteStatement;
}
