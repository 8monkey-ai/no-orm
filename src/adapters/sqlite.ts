import type { Database as BunDatabase } from "bun:sqlite";

import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type {
  Adapter,
  Field,
  InferModel,
  Schema,
  Select,
  SortBy,
  Where,
  Cursor,
  Model,
} from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildPrimaryKeyFilter,
  getPrimaryKeyFieldNames,
  getPrimaryKeyValues,
  mapNumeric,
} from "./utils/common";
import {
  type QueryExecutor,
  isQueryExecutor,
  Sql,
  sql,
  id,
  where,
  set,
  sort,
  selectSql,
  insertSql,
  updateSql,
  deleteSql,
  upsertSql,
  countSql,
  migrateSqls,
} from "./utils/sql";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

/**
 * Limits the number of prepared statement objects kept in memory to prevent leaks
 * while allowing statement reuse for performance.
 */
const MAX_CACHED_STATEMENTS = 100;

// --- Internal SQLite Syntax Helpers ---

const mapSqliteValue = (val: unknown, field?: Field) => {
  if (field?.type === "boolean" || (field === undefined && typeof val === "boolean")) {
    return val === true ? 1 : 0;
  }
  if (val !== null && typeof val === "object") {
    return JSON.stringify(val);
  }
  return val;
};

function mapFromRecord<T extends Record<string, unknown>>(
  model: Model,
  record: Record<string, unknown>,
): T {
  const fields = model.fields;
  const keys = Object.keys(record);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const field = fields[k];
    if (field === undefined || record[k] === null || record[k] === undefined) continue;

    if (field.type === "json" || field.type === "json[]") {
      record[k] = typeof record[k] === "string" ? JSON.parse(record[k]) : record[k];
    } else if (field.type === "boolean") {
      record[k] = record[k] === 1 || record[k] === true;
    } else if (field.type === "number" || field.type === "timestamp") {
      record[k] = mapNumeric(record[k]);
    }
  }
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
  return record as T;
}

function sqlType(field: Field): string {
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
      return "TEXT"; // SQLite stores JSON as plain text
    default:
      return "TEXT";
  }
}

function toJsonPath(path: string[]): string {
  let jsonPath = "$";
  for (let i = 0; i < path.length; i++) {
    const segment = path[i]!;
    let isIndex = true;
    if (segment.length === 0) isIndex = false;
    else {
      for (let j = 0; j < segment.length; j++) {
        const c = segment.codePointAt(j);
        if (c === undefined || c < 48 || c > 57) {
          isIndex = false;
          break;
        }
      }
    }
    if (isIndex) jsonPath += `[${segment}]`;
    else jsonPath += `.${segment}`;
  }
  return jsonPath;
}

function toColumnExpr(model: Model, fieldName: string, path?: string[]): Sql {
  if (!path || path.length === 0) return id(fieldName);
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }
  return sql`json_extract(${id(fieldName)}, ${toJsonPath(path)})`;
}

// --- Driver detection and executors ---

function isBunSqlite(driver: SqliteDriver): driver is BunDatabase {
  return "query" in driver && !("all" in driver);
}

function isBetterSqlite3(driver: SqliteDriver): driver is BetterSqlite3Database {
  return "prepare" in driver && !("all" in driver) && !("query" in driver);
}

type SyncStatement = {
  all(...params: unknown[]): Record<string, unknown>[];
  get(...params: unknown[]): Record<string, unknown> | undefined;
  run(...params: unknown[]): { changes: number };
};

function createBunSqliteExecutor(driver: BunDatabase, inTransaction = false): QueryExecutor {
  function getPrepared(sqlStr: string): SyncStatement {
    // driver.query() caches at the BunDatabase level — no manual Map needed
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- bun:sqlite Statement structurally matches SyncStatement; run() returns {changes} at runtime despite void TypeScript type
    return driver.query(sqlStr) as unknown as SyncStatement;
  }

  return {
    all: (query: Sql) => Promise.resolve(getPrepared(query.compile("?")).all(...query.params)),
    get: (query: Sql) => Promise.resolve(getPrepared(query.compile("?")).get(...query.params)),
    run: (query: Sql) => {
      const res = getPrepared(query.compile("?")).run(...query.params);
      return Promise.resolve({ changes: res.changes });
    },
    transaction: async (fn) => {
      getPrepared("BEGIN").run();
      try {
        const result = await fn(createBunSqliteExecutor(driver, true));
        getPrepared("COMMIT").run();
        return result;
      } catch (e) {
        getPrepared("ROLLBACK").run();
        throw e;
      }
    },
    inTransaction,
  };
}

function createBetterSqlite3Executor(
  driver: BetterSqlite3Database,
  inTransaction = false,
  cache = new Map<string, SyncStatement>(),
): QueryExecutor {
  function getPrepared(sqlStr: string): SyncStatement {
    let stmt = cache.get(sqlStr);
    if (stmt === undefined) {
      if (cache.size >= MAX_CACHED_STATEMENTS) {
        const first = cache.keys().next();
        if (first.done !== true) cache.delete(first.value);
      }
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- better-sqlite3 Statement structurally matches SyncStatement
      stmt = driver.prepare(sqlStr) as unknown as SyncStatement;
      cache.set(sqlStr, stmt);
    }
    return stmt;
  }

  return {
    all: (query: Sql) => Promise.resolve(getPrepared(query.compile("?")).all(...query.params)),
    get: (query: Sql) => Promise.resolve(getPrepared(query.compile("?")).get(...query.params)),
    run: (query: Sql) => {
      const res = getPrepared(query.compile("?")).run(...query.params);
      return Promise.resolve({ changes: res.changes });
    },
    transaction: async (fn) => {
      getPrepared("BEGIN").run();
      try {
        const result = await fn(createBetterSqlite3Executor(driver, true, cache));
        getPrepared("COMMIT").run();
        return result;
      } catch (e) {
        getPrepared("ROLLBACK").run();
        throw e;
      }
    },
    inTransaction,
  };
}

function createSqliteExecutor(driver: SqliteDatabase, inTransaction = false): QueryExecutor {
  return {
    all: (query: Sql) => driver.all(query.compile("?"), query.params),
    get: (query: Sql) => driver.get(query.compile("?"), query.params),
    run: async (query: Sql) => {
      if (query.params.length === 0) {
        await driver.exec(query.compile("?"));
        return { changes: 0 };
      }
      const res = await driver.run(query.compile("?"), query.params);
      return { changes: res.changes ?? 0 };
    },
    transaction: async (fn) => {
      await driver.run("BEGIN");
      try {
        const res = await fn(createSqliteExecutor(driver, true));
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

function createExecutor(driver: SqliteDriver): QueryExecutor {
  if (isBunSqlite(driver)) return createBunSqliteExecutor(driver);
  if (isBetterSqlite3(driver)) return createBetterSqlite3Executor(driver);
  return createSqliteExecutor(driver);
}

// --- Adapter ---

/**
 * SQLite Adapter for no-orm.
 */
export class SqliteAdapter<S extends Schema> implements Adapter<S> {
  private executor: QueryExecutor;

  constructor(
    private schema: S,
    driver: SqliteDriver | QueryExecutor,
  ) {
    this.executor = isQueryExecutor(driver) ? driver : createExecutor(driver);
  }

  async migrate(): Promise<void> {
    const stmts = migrateSqls(this.schema, { sqlType });
    await this.executor.transaction(async (exec) => {
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      for (let i = 0; i < stmts.length; i++) await exec.run(stmts[i]!);
    });
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    if (this.executor.inTransaction) return fn(this);
    return this.executor.transaction((exec) => fn(new SqliteAdapter(this.schema, exec)));
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    const { model: modelName, data, select } = args;
    const model = this.schema[modelName]!;
    const rawData = data as Record<string, unknown>;
    const fields: string[] = [];
    const values: unknown[] = [];
    for (const k of Object.keys(rawData)) {
      const val = rawData[k];
      if (val === undefined) continue;
      fields.push(k);
      values.push(mapSqliteValue(val));
    }
    const query = insertSql({ table: modelName, fields, values, returning: select });

    const row = await this.executor.get(query);
    if (row === undefined || row === null) {
      const res = await this.find({
        model: modelName,
        where: buildPrimaryKeyFilter<T>(model, getPrimaryKeyValues(model, data)),
        select,
      });
      if (!res) throw new Error("Failed to insert record");
      return res;
    }
    return mapFromRecord<T>(model, row);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model: modelName, select } = args;
    const model = this.schema[modelName]!;
    const query = selectSql({
      table: modelName,
      select,
      where: where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue }),
      limit: 1,
    });

    const row = await this.executor.get(query);
    if (row === undefined || row === null) return null;
    return mapFromRecord<T>(model, row);
  }

  async findMany<
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
  }): Promise<T[]> {
    const { model: modelName, select, sortBy, limit, offset, cursor } = args;
    const model = this.schema[modelName]!;
    const query = selectSql({
      table: modelName,
      select,
      where: where(args.where, {
        model,
        columnExpr: toColumnExpr,
        mapValue: mapSqliteValue,
        cursor,
        sortBy,
      }),
      orderBy: sortBy && sortBy.length > 0 ? sort(model, sortBy, toColumnExpr) : undefined,
      limit,
      offset,
    });
    const rows = await this.executor.all(query);

    const result: T[] = [];
    for (let i = 0; i < rows.length; i++) {
      result.push(mapFromRecord<T>(model, rows[i]!));
    }
    return result;
  }

  /**
   * Updates the first record matching the criteria. Primary key updates are rejected.
   */
  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: Partial<T>; where: Where<T> }): Promise<T | null> {
    const { model: modelName, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const dataRecord = data as Record<string, unknown>;

    if (!Object.keys(dataRecord).some((k) => dataRecord[k] !== undefined))
      return this.find({ model: modelName, where: args.where, select: undefined });

    const query = updateSql({
      table: modelName,
      set: set(dataRecord, (v) => mapSqliteValue(v)),
      where: sql`rowid = (SELECT rowid FROM ${id(modelName)} WHERE ${where(args.where, {
        model,
        columnExpr: toColumnExpr,
        mapValue: mapSqliteValue,
      })} LIMIT 1)`,
      returning: true,
    });

    const row = await this.executor.get(query);
    if (row === undefined || row === null)
      return this.find({ model: modelName, where: args.where });
    return mapFromRecord<T>(model, row);
  }

  /**
   * Updates all records matching the criteria. Primary key updates are rejected.
   */
  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model: modelName, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const dataRecord = data as Record<string, unknown>;
    if (!Object.keys(dataRecord).some((k) => dataRecord[k] !== undefined)) return 0;

    const query = updateSql({
      table: modelName,
      set: set(dataRecord, (v) => mapSqliteValue(v)),
      where: where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue }),
    });

    const res = await this.executor.run(query);
    return res.changes;
  }

  /**
   * Performs an atomic insert-or-update.
   *
   * Conflicts are always handled on the Primary Key. If `where` is provided, the record
   * is only updated if the condition is met (acting as a predicate). Primary key
   * updates are rejected.
   */
  async upsert<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }): Promise<T> {
    const { model: modelName, create: createData, update: updateData, select } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, updateData);

    const rawCreate = createData as Record<string, unknown>;
    const createFields: string[] = [];
    const createValues: unknown[] = [];
    for (const k of Object.keys(rawCreate)) {
      const val = rawCreate[k];
      if (val === undefined) continue;
      createFields.push(k);
      createValues.push(mapSqliteValue(val));
    }

    const rawUpdate = updateData as Record<string, unknown>;
    const hasUpdateFields = Object.keys(rawUpdate).some((k) => rawUpdate[k] !== undefined);
    const primaryKeyFieldNames = getPrimaryKeyFieldNames(model);

    const onConflict = hasUpdateFields
      ? args.where
        ? sql`DO UPDATE SET ${set(rawUpdate, (v) => mapSqliteValue(v))} WHERE ${where(args.where, {
            model,
            columnExpr: toColumnExpr,
            mapValue: mapSqliteValue,
          })}`
        : sql`DO UPDATE SET ${set(rawUpdate, (v) => mapSqliteValue(v))}`
      : sql`DO NOTHING`;

    const query = upsertSql({
      table: modelName,
      fields: createFields,
      values: createValues,
      conflictColumns: primaryKeyFieldNames,
      onConflict,
      returning: select,
    });

    const row = await this.executor.get(query);
    if (row !== undefined && row !== null) {
      return mapFromRecord<T>(model, row);
    }

    const existing = await this.find({
      model: modelName,
      where: buildPrimaryKeyFilter<T>(model, getPrimaryKeyValues(model, createData)),
      select,
    });
    if (existing === null) throw new Error("Failed to refetch record after upsert");
    return existing;
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = deleteSql({
      table: modelName,
      where: where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue }),
    });
    await this.executor.run(query);
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = deleteSql({
      table: modelName,
      where: where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue }),
    });
    const res = await this.executor.run(query);
    return res.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = countSql({
      table: modelName,
      where: where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue }),
    });
    const row = await this.executor.get(query);
    return Number(row?.["count"] ?? 0);
  }
}
