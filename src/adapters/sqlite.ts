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
  getPrimaryKeyFields,
  getPrimaryKeyValues,
  mapNumeric,
} from "./utils/common";
import {
  type QueryExecutor,
  isQueryExecutor,
  Sql,
  sql,
  raw,
  idList,
  paramList,
  where,
  set,
  sort,
} from "./utils/sql";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

/**
 * Limits the number of prepared statement objects kept in memory to prevent leaks
 * while allowing statement reuse for performance.
 */
const MAX_CACHED_STATEMENTS = 100;

// --- Internal SQLite Syntax Helpers ---

const ident = (s: string) => raw(`"${s}"`);
const selectCols = (select?: readonly string[]) => (select ? idList(select) : raw("*"));

const mapSqliteValue = (val: unknown, field?: Field) => {
  if (field?.type === "boolean" || (field === undefined && typeof val === "boolean")) {
    return val === true ? 1 : 0;
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

function mapToRecord(model: Model, data: Record<string, unknown>): Record<string, unknown> {
  const fields = model.fields;
  const res: Record<string, unknown> = {};
  const keys = Object.keys(data);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = data[k];
    if (val === undefined) continue;
    if (val === null) {
      res[k] = null;
      continue;
    }
    const field = fields[k];
    if (field === undefined) {
      res[k] = val;
      continue;
    }

    let processed = val;
    if (field.type === "json" || field.type === "json[]") {
      processed = JSON.stringify(val);
    } else if (field.type === "boolean") {
      processed = val === true ? 1 : 0;
    }
    res[k] = processed;
  }
  return res;
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
  if (!path || path.length === 0) return ident(fieldName);
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }
  return sql`json_extract(${ident(fieldName)}, ${toJsonPath(path)})`;
}

// --- Driver detection and executors ---

function isSyncSqlite(driver: SqliteDriver): driver is BunDatabase | BetterSqlite3Database {
  return "prepare" in driver && !("all" in driver);
}

type SyncStatement = {
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): { changes: number };
};

interface SyncDriver {
  prepare(sql: string): SyncStatement;
}

function createSyncSqliteExecutor(driver: SyncDriver, inTransaction = false): QueryExecutor {
  const cache = new Map<string, SyncStatement>();

  function getPrepared(sqlStr: string): SyncStatement {
    let stmt = cache.get(sqlStr);
    if (stmt === undefined) {
      if (cache.size >= MAX_CACHED_STATEMENTS) {
        const first = cache.keys().next();
        if (first.done !== true) cache.delete(first.value);
      }
      stmt = driver.prepare(sqlStr);
      cache.set(sqlStr, stmt);
    }
    return stmt;
  }

  return {
    all: (query: Sql) => {
      const { strings, params } = query;
      const sqlStr = strings.join("?");
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver result row matches Record shape
      return Promise.resolve(getPrepared(sqlStr).all(...params) as Record<string, unknown>[]);
    },
    get: (query: Sql) => {
      const { strings, params } = query;
      const sqlStr = strings.join("?");
      return Promise.resolve(
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver returns either a row object or undefined
        getPrepared(sqlStr).get(...params) as Record<string, unknown> | undefined,
      );
    },
    run: (query: Sql) => {
      const { strings, params } = query;
      const sqlStr = strings.join("?");
      const res = getPrepared(sqlStr).run(...params);
      return Promise.resolve({ changes: res.changes });
    },
    transaction: async (fn) => {
      getPrepared("BEGIN").run();
      try {
        const res = await fn(createSyncSqliteExecutor(driver, true));
        getPrepared("COMMIT").run();
        return res;
      } catch (e) {
        getPrepared("ROLLBACK").run();
        throw e;
      }
    },
    inTransaction,
  };
}

function createAsyncSqliteExecutor(driver: SqliteDatabase, inTransaction = false): QueryExecutor {
  return {
    // eslint-disable-next-line typescript-eslint/no-unsafe-return -- async driver returns rows
    all: (query: Sql) => driver.all(query.strings.join("?"), query.params),
    // eslint-disable-next-line typescript-eslint/no-unsafe-return -- async driver returns row
    get: (query: Sql) => driver.get(query.strings.join("?"), query.params),
    run: async (query: Sql) => {
      const res = await driver.run(query.strings.join("?"), query.params);
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
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- bridges narrowed driver type to internal structural SyncDriver shape
  if (isSyncSqlite(driver)) return createSyncSqliteExecutor(driver as SyncDriver);
  return createAsyncSqliteExecutor(driver);
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
    this.executor = isQueryExecutor(driver) ? driver : createSqliteExecutor(driver);
  }

  async migrate(): Promise<void> {
    const models = Object.entries(this.schema);

    // Create tables first, then indexes — indexes depend on tables existing.
    // DDL must be sequential: some drivers don't support concurrent DDL on one connection.
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      const fields = Object.entries(model.fields);
      const columns = fields.map(
        ([fname, f]) => `"${fname}" ${sqlType(f)}${f.nullable === true ? "" : " NOT NULL"}`,
      );
      const primaryKeyFields = getPrimaryKeyFields(model);
      const pk = `PRIMARY KEY (${primaryKeyFields.map((f) => `"${f}"`).join(", ")})`;
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      await this.executor.run(sql`
        CREATE TABLE IF NOT EXISTS ${ident(name)} (
          ${raw(columns.join(", "))},
          ${raw(pk)}
        )
      `);
    }

    // Now create indexes
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      if (!model.indexes) continue;
      for (let j = 0; j < model.indexes.length; j++) {
        const idx = model.indexes[j]!;
        const fields = Array.isArray(idx.field) ? idx.field : [idx.field];
        const formatted = fields.map(
          (f) => `"${f}"${idx.order ? ` ${idx.order.toUpperCase()}` : ""}`,
        );
        // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
        await this.executor.run(sql`
          CREATE INDEX IF NOT EXISTS ${ident(`idx_${name}_${j}`)}
          ON ${ident(name)} (${raw(formatted.join(", "))})
        `);
      }
    }
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
    const input = mapToRecord(model, data);
    const fields = Object.keys(input);
    const query = sql`
      INSERT INTO ${ident(modelName)} (${idList(fields)})
      VALUES (${paramList(fields.map((f) => input[f]))})
      RETURNING ${selectCols(select)}
    `;

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
    const query = sql`
      SELECT ${selectCols(select)}
      FROM ${ident(modelName)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue })}
      LIMIT 1
    `;

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
    let query = sql`
      SELECT ${selectCols(select)}
      FROM ${ident(modelName)}
      WHERE ${where(args.where, {
        model,
        columnExpr: toColumnExpr,
        mapValue: mapSqliteValue,
        cursor,
        sortBy,
      })}
    `;

    if (sortBy && sortBy.length > 0) {
      query = sql`${query} ORDER BY ${sort(model, sortBy, toColumnExpr)}`;
    }
    if (limit !== undefined) {
      query = sql`${query} LIMIT ${limit}`;
    }
    if (offset !== undefined) {
      query = sql`${query} OFFSET ${offset}`;
    }
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
    const input = mapToRecord(model, data);
    const fields = Object.keys(input);

    if (fields.length === 0)
      return this.find({ model: modelName, where: args.where, select: undefined });

    const query = sql`
      UPDATE ${ident(modelName)}
      SET ${set(input)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue })}
      RETURNING *
    `;

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
    const input = mapToRecord(model, data);
    const fields = Object.keys(input);
    if (fields.length === 0) return 0;

    const query = sql`
      UPDATE ${ident(modelName)}
      SET ${set(input)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue })}
    `;

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

    const insertRow = mapToRecord(model, createData);
    const createFields = Object.keys(insertRow);
    const updateRow = mapToRecord(model, updateData);
    const updateFields = Object.keys(updateRow);
    const primaryKeyFields = getPrimaryKeyFields(model);

    const action =
      updateFields.length === 0
        ? sql`DO NOTHING`
        : args.where
          ? sql`DO UPDATE SET ${set(updateRow)} WHERE ${where(args.where, {
              model,
              columnExpr: toColumnExpr,
              mapValue: mapSqliteValue,
            })}`
          : sql`DO UPDATE SET ${set(updateRow)}`;

    const query = sql`
      INSERT INTO ${ident(modelName)} (${idList(createFields)})
      VALUES (${paramList(createFields.map((f) => insertRow[f]))})
      ON CONFLICT (${idList(primaryKeyFields)}) ${action}
      RETURNING ${selectCols(select)}
    `;

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
    const query = sql`
      DELETE FROM ${ident(modelName)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue })}
    `;
    await this.executor.run(query);
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = sql`
      DELETE FROM ${ident(modelName)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue })}
    `;
    const res = await this.executor.run(query);
    return res.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = sql`
      SELECT COUNT(*) as count
      FROM ${ident(modelName)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, mapValue: mapSqliteValue })}
    `;
    const row = await this.executor.get(query);
    return Number(row?.["count"] ?? 0);
  }
}
