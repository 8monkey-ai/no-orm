import type { Database as BunDatabase } from "bun:sqlite";

import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type { Adapter, Field, InferModel, Schema, Select, SortBy, Where, Cursor } from "../types";
import {
  type QueryExecutor,
  type SqlFormat,
  isQueryExecutor,
  migrate,
  create,
  find,
  findMany,
  update,
  updateMany,
  upsert,
  remove,
  removeMany,
  count,
} from "./sql";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

// --- Formatting Hooks ---

const sqlite: SqlFormat = {
  placeholder: () => "?",
  quote: (s) => `"${s.replaceAll('"', '""')}"`,
  mapBoolean: (v) => (v ? 1 : 0),
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
  jsonExtract(column: string, path: string[]): string {
    let jsonPath = "$";
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
        jsonPath += `[${segment}]`;
      } else {
        jsonPath += `.${segment}`;
      }
    }
    return `json_extract(${column}, '${jsonPath}')`;
  },
};

const MAX_CACHE_SIZE = 100;

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
 * This allows the adapter to work with both drivers without direct dependencies
 * on their respective type libraries.
 */
type SyncStatement = {
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): { changes: number };
};

interface SyncDriver {
  prepare(sql: string): SyncStatement;
}

// Caches compiled Statement objects per SQL string to avoid re-parsing on every query.
// Uses a simple Map with FIFO eviction at MAX_CACHE_SIZE.
function createSyncSqliteExecutor(driver: SyncDriver, inTransaction = false): QueryExecutor {
  const cache = new Map<string, SyncStatement>();

  function getStmt(sql: string): SyncStatement {
    let stmt = cache.get(sql);
    if (stmt === undefined) {
      if (cache.size >= MAX_CACHE_SIZE) {
        const first = cache.keys().next();
        if (first.done !== true) cache.delete(first.value);
      }
      stmt = driver.prepare(sql);
      cache.set(sql, stmt);
    }
    return stmt;
  }

  return {
    all: (sql, params) => {
      const result = getStmt(sql).all(...(params ?? []));
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SQLite rows are plain objects matching RowData
      return Promise.resolve(result as Record<string, unknown>[]);
    },
    get: (sql, params) => {
      const result = getStmt(sql).get(...(params ?? []));
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SQLite row is a plain object or undefined, matching RowData
      return Promise.resolve(result as Record<string, unknown> | undefined);
    },
    run: (sql, params) => {
      const res = getStmt(sql).run(...(params ?? []));
      return Promise.resolve({ changes: res.changes ?? 0 });
    },
    transaction: async (fn) => {
      getStmt("BEGIN").run();
      try {
        const res = await fn(createSyncSqliteExecutor(driver, true));
        getStmt("COMMIT").run();
        return res;
      } catch (e) {
        getStmt("ROLLBACK").run();
        throw e;
      }
    },
    inTransaction,
  };
}

function createAsyncSqliteExecutor(driver: SqliteDatabase, inTransaction = false): QueryExecutor {
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
        const res = await fn(createAsyncSqliteExecutor(driver, true));
        await driver.run("COMMIT");
        return res;
      } catch (e) {
        await driver.run("ROLLBACK");
        throw e;
      }
    },
    inTransaction,
  };
}

function createSqliteExecutor(driver: SqliteDriver): QueryExecutor {
  if (isSyncSqlite(driver)) {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally compatible with SyncDriver
    return createSyncSqliteExecutor(driver as unknown as SyncDriver);
  }
  return createAsyncSqliteExecutor(driver as SqliteDatabase);
}

// --- Adapter ---

export class SqliteAdapter<S extends Schema = Schema> implements Adapter<S> {
  private executor: QueryExecutor;

  constructor(
    private schema: S,
    driver: SqliteDriver | QueryExecutor,
  ) {
    this.executor = isQueryExecutor(driver) ? driver : createSqliteExecutor(driver);
  }

  migrate = () => migrate(this.executor, this.schema, sqlite);

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    if (this.executor.inTransaction) {
      // Re-use current adapter if already in a transaction.
      return fn(this);
    }
    return this.executor.transaction((exec) => fn(new SqliteAdapter(this.schema, exec)));
  }

  create = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }) => create(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  find = <K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }) => find(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  findMany = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }) => findMany(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  update = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }) => update(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  updateMany = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
    data: Partial<T>;
  }) => updateMany(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  upsert = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }) => upsert(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  delete = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where: Where<T>;
  }) => remove(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  deleteMany = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
  }) => removeMany(this.executor, args.model, this.schema[args.model]!, sqlite, args);

  count = <K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }) => count(this.executor, args.model, this.schema[args.model]!, sqlite, args);
}
