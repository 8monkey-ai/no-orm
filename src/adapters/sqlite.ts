import type { Database as BunDatabase } from "bun:sqlite";
import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type { Adapter, Field, Schema } from "../types";
import { type QueryExecutor, type SqlDialect, SqlAdapter, isQueryExecutor } from "./sql";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

// --- Dialect ---

export const SqliteDialect: SqlDialect = {
  placeholder: () => "?",
  quote: (s) => `"${s.replaceAll('"', '""')}"`,
  escapeLiteral: (s) => s.replaceAll("'", "''"),
  mapFieldType(field: Field): string {
    switch (field.type) {
      case "string":
        return field.max === undefined ? "TEXT" : `VARCHAR(${field.max})`;
      case "number":
        return "REAL";
      case "boolean":
      case "timestamp":
        return "INTEGER";
      case "json":
      case "json[]":
        return "TEXT";
      default:
        return "TEXT";
    }
  },
  buildJsonPath(path: string[]): string {
    let res = "$";
    for (let i = 0; i < path.length; i++) {
      const segment = path[i]!;
      let isIndex = true;
      for (let j = 0; j < segment.length; j++) {
        const c = segment.codePointAt(j);
        if (c === undefined || c < 48 || c > 57) {
          isIndex = false;
          break;
        }
      }
      if (isIndex) {
        res += `[${segment}]`;
      } else {
        res += `.${segment}`;
      }
    }
    return res;
  },
  buildJsonExtract(column: string, path: string[]): string {
    return `json_extract(${column}, '${this.buildJsonPath(path)}')`;
  },
};

// --- Driver detection and executors ---

// better-sqlite3 and bun:sqlite are synchronous — they have `prepare` returning a statement
// with synchronous `.all()`, `.get()`, `.run()` methods.
// The async `sqlite` package wraps sqlite3 and has async `.all()`, `.get()`, `.run()` directly.
function isSyncSqlite(driver: SqliteDriver): driver is BunDatabase | BetterSqlite3Database {
  return "prepare" in driver && !("all" in driver);
}

/**
 * Structural interface for the shared subset of BunDatabase and BetterSqlite3Database
 * `prepare()` APIs. Their full type signatures differ but both satisfy this shape.
 */
interface SyncDriver {
  prepare(sql: string): {
    all(...params: unknown[]): unknown[];
    get(...params: unknown[]): unknown;
    run(...params: unknown[]): { changes: number };
  };
}

function createSyncSqliteExecutor(driver: SyncDriver): QueryExecutor {
  return {
    all: (sql, params) => {
      const result = driver.prepare(sql).all(...(params ?? []));
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SQLite rows are plain objects
      return Promise.resolve(result as Record<string, unknown>[]);
    },
    get: (sql, params) => {
      const result = driver.prepare(sql).get(...(params ?? []));
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SQLite row is a plain object or undefined
      return Promise.resolve(result as Record<string, unknown> | undefined);
    },
    run: (sql, params) => {
      const res = driver.prepare(sql).run(...(params ?? []));
      return Promise.resolve({ changes: res.changes ?? 0 });
    },
    transaction: async (fn) => {
      driver.prepare("BEGIN").run();
      try {
        const res = await fn(createSyncSqliteExecutor(driver));
        driver.prepare("COMMIT").run();
        return res;
      } catch (e) {
        driver.prepare("ROLLBACK").run();
        throw e;
      }
    },
  };
}

function createAsyncSqliteExecutor(driver: SqliteDatabase): QueryExecutor {
  return {
    all: (sql, params) => driver.all(sql, params),
    get: (sql, params) => driver.get(sql, params),
    run: async (sql, params) => {
      const res = await driver.run(sql, params);
      return { changes: res.changes ?? 0 };
    },
    transaction: async (fn) => {
      await driver.run("BEGIN");
      try {
        const res = await fn(createAsyncSqliteExecutor(driver));
        await driver.run("COMMIT");
        return res;
      } catch (e) {
        await driver.run("ROLLBACK");
        throw e;
      }
    },
  };
}

function createSqliteExecutor(driver: SqliteDriver): QueryExecutor {
  if (isSyncSqlite(driver)) {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- BunDatabase and BetterSqlite3Database both satisfy SyncDriver structurally
    return createSyncSqliteExecutor(driver as unknown as SyncDriver);
  }
  return createAsyncSqliteExecutor(driver as SqliteDatabase);
}

// --- Adapter ---

export class SqliteAdapter<S extends Schema = Schema> extends SqlAdapter<S> implements Adapter<S> {
  constructor(schema: S, driver: SqliteDriver | QueryExecutor) {
    super(schema, isQueryExecutor(driver) ? driver : createSqliteExecutor(driver), SqliteDialect);
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction((innerExecutor) => {
      return fn(new SqliteAdapter(this.schema, innerExecutor));
    });
  }
}
