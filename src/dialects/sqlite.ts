import type { Database as BunDatabase } from "bun:sqlite";

import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type { Field } from "../types";
import type { QueryExecutor, SqlDialect } from "./types";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database | any;

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
        if (c < 48 || c > 57) {
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

function isSyncSqlite(driver: any): boolean {
  return typeof driver.prepare === "function" && driver.get?.constructor.name !== "AsyncFunction";
}

function createSyncSqliteExecutor(driver: any): QueryExecutor {
  return {
    all: async (sql, params) => driver.prepare(sql).all(...(params ?? [])),
    get: async (sql, params) => driver.prepare(sql).get(...(params ?? [])),
    run: async (sql, params) => {
      const res = driver.prepare(sql).run(...(params ?? []));
      return { changes: res?.changes ?? 0 };
    },
    transaction: async (fn) => {
      // Nested transactions stay on the current connection and use savepoints.
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

function createAsyncSqliteExecutor(driver: any): QueryExecutor {
  return {
    all: (sql, params) => driver.all(sql, params),
    get: (sql, params) => driver.get(sql, params),
    run: async (sql, params) => {
      const res = await driver.run(sql, params);
      return { changes: res?.changes ?? 0 };
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
  return createAsyncSqliteExecutor(driver);
}
