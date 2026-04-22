import type { Database as BunDatabase } from "bun:sqlite";

import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type { Field } from "../types";
import type { QueryExecutor, SqlDialect } from "./types";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

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

function isSyncSqlite(driver: unknown): driver is BunDatabase | BetterSqlite3Database {
  if (typeof driver !== "object" || driver === null) return false;
  const d = driver as unknown;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  const record = d as Record<string, unknown>;
  return "prepare" in record && typeof record["prepare"] === "function";
}

type SqliteStmt = {
  all: (this: void, ...params: unknown[]) => unknown[];
  get: (this: void, ...params: unknown[]) => unknown;
  run: (this: void, ...params: unknown[]) => { changes: number };
};

function createSyncSqliteExecutor(driver: BunDatabase | BetterSqlite3Database): QueryExecutor {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  const driverObj = driver as { prepare: (sql: string) => SqliteStmt };
  return {
    all: (sql, params) => {
      const stmt = driverObj.prepare(sql);
      const result = stmt.all(...(params ?? []));
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      return Promise.resolve(result as Record<string, unknown>[]);
    },
    get: (sql, params) => {
      const stmt = driverObj.prepare(sql);
      const result = stmt.get(...(params ?? []));
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      return Promise.resolve(result as Record<string, unknown> | undefined);
    },
    run: (sql, params) => {
      const stmt = driverObj.prepare(sql);
      const res = stmt.run(...(params ?? []));
      return Promise.resolve({ changes: res.changes ?? 0 });
    },
    transaction: async (fn) => {
      const begin = driverObj.prepare("BEGIN");
      begin.run();
      try {
        const res = await fn(createSyncSqliteExecutor(driver));
        const commit = driverObj.prepare("COMMIT");
        commit.run();
        return res;
      } catch (e) {
        const rollback = driverObj.prepare("ROLLBACK");
        rollback.run();
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
      // Nested transactions stay on the current connection and use savepoints.
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

export function createSqliteExecutor(driver: SqliteDriver): QueryExecutor {
  if (isSyncSqlite(driver)) return createSyncSqliteExecutor(driver);
  return createAsyncSqliteExecutor(driver as SqliteDatabase);
}
