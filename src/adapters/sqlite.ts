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
  buildIdentityFilter,
  getIdentityValues,
  getPaginationFilter,
  getPrimaryKeyFields,
} from "./common";
import { type QueryExecutor, isQueryExecutor, toRow } from "./sql";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

const MAX_CACHE_SIZE = 100;

// --- Internal SQLite Syntax Helpers ---

const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const placeholder = () => "?";

function mapFieldType(field: Field): string {
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
}

function jsonExtract(column: string, path: string[]): string {
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
    if (isIndex) jsonPath += `[${segment}]`;
    else jsonPath += `.${segment}`;
  }
  return `json_extract(${column}, '${jsonPath}')`;
}

function toColumnExpr(model: Model, fieldName: string, path?: string[]): string {
  if (!path || path.length === 0) return quote(fieldName);
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }
  return jsonExtract(quote(fieldName), path);
}

function toWhereRecursive(model: Model, where: Where): { sql: string; params: unknown[] } {
  if ("and" in where) {
    const parts = [];
    const params = [];
    for (let i = 0; i < where.and.length; i++) {
      const built = toWhereRecursive(model, where.and[i]!);
      parts.push(`(${built.sql})`);
      params.push(...built.params);
    }
    return { sql: parts.join(" AND "), params };
  }

  if ("or" in where) {
    const parts = [];
    const params = [];
    for (let i = 0; i < where.or.length; i++) {
      const built = toWhereRecursive(model, where.or[i]!);
      parts.push(`(${built.sql})`);
      params.push(...built.params);
    }
    return { sql: parts.join(" OR "), params };
  }

  const expr = toColumnExpr(model, where.field, where.path);
  const val = where.value;
  const mappedVal = typeof val === "boolean" ? (val ? 1 : 0) : val;

  switch (where.op) {
    case "eq":
      if (val === null) return { sql: `${expr} IS NULL`, params: [] };
      return { sql: `${expr} = ${placeholder()}`, params: [mappedVal] };
    case "ne":
      if (val === null) return { sql: `${expr} IS NOT NULL`, params: [] };
      return { sql: `${expr} != ${placeholder()}`, params: [mappedVal] };
    case "gt":
      return { sql: `${expr} > ${placeholder()}`, params: [mappedVal] };
    case "gte":
      return { sql: `${expr} >= ${placeholder()}`, params: [mappedVal] };
    case "lt":
      return { sql: `${expr} < ${placeholder()}`, params: [mappedVal] };
    case "lte":
      return { sql: `${expr} <= ${placeholder()}`, params: [mappedVal] };
    case "in": {
      if (!Array.isArray(val) || val.length === 0) return { sql: "1=0", params: [] };
      const inParams = val.map((v): unknown => (typeof v === "boolean" ? (v ? 1 : 0) : v));
      return { sql: `${expr} IN (${val.map(() => placeholder()).join(", ")})`, params: inParams };
    }
    case "not_in": {
      if (!Array.isArray(val) || val.length === 0) return { sql: "1=1", params: [] };
      const inParams = val.map((v): unknown => (typeof v === "boolean" ? (v ? 1 : 0) : v));
      return {
        sql: `${expr} NOT IN (${val.map(() => placeholder()).join(", ")})`,
        params: inParams,
      };
    }
    default:
      throw new Error(`Unsupported operator: ${String((where as Record<string, unknown>)["op"])}`);
  }
}

function toWhere(
  model: Model,
  where?: Where,
  cursor?: Cursor,
  sortBy?: SortBy[],
): { sql: string; params: unknown[] } {
  const parts: string[] = [];
  const params: unknown[] = [];

  if (where) {
    const built = toWhereRecursive(model, where);
    parts.push(`(${built.sql})`);
    params.push(...built.params);
  }

  if (cursor) {
    const paginationWhere = getPaginationFilter(cursor, sortBy);
    if (paginationWhere) {
      const built = toWhereRecursive(model, paginationWhere);
      parts.push(`(${built.sql})`);
      params.push(...built.params);
    }
  }

  return { sql: parts.length > 0 ? parts.join(" AND ") : "1=1", params };
}

function toInput(
  fields: Record<string, Field>,
  data: Record<string, unknown>,
): Record<string, unknown> {
  const res: Record<string, unknown> = {};
  const keys = Object.keys(data);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = data[k];
    const spec = fields[k];
    if (val === undefined) continue;
    if (val === null) {
      res[k] = null;
      continue;
    }
    if (spec?.type === "json" || spec?.type === "json[]") {
      res[k] = JSON.stringify(val);
    } else if (spec?.type === "boolean") {
      res[k] = val === true ? 1 : 0;
    } else {
      res[k] = val;
    }
  }
  return res;
}

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
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SQLite rows match Record shape
      return Promise.resolve(result as Record<string, unknown>[]);
    },
    get: (sql, params) => {
      const result = getStmt(sql).get(...(params ?? []));
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SQLite row matches Record shape
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
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally checked
  if (isSyncSqlite(driver)) return createSyncSqliteExecutor(driver as unknown as SyncDriver);
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

  async migrate(): Promise<void> {
    const models = Object.entries(this.schema);

    // Create tables first, then indexes — indexes depend on tables existing.
    // DDL must be sequential: some drivers don't support concurrent DDL on one connection.
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      const fields = Object.entries(model.fields);
      const columns = fields.map(
        ([fname, f]) =>
          `${quote(fname)} ${mapFieldType(f)}${f.nullable === true ? "" : " NOT NULL"}`,
      );
      const pkFields = getPrimaryKeyFields(model);
      const pk = `PRIMARY KEY (${pkFields.map((f) => quote(f)).join(", ")})`;
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      await this.executor.run(
        `CREATE TABLE IF NOT EXISTS ${quote(name)} (${columns.join(", ")}, ${pk})`,
      );
    }

    // Now create indexes
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      if (!model.indexes) continue;
      for (let j = 0; j < model.indexes.length; j++) {
        const idx = model.indexes[j]!;
        const fields = Array.isArray(idx.field) ? idx.field : [idx.field];
        const formatted = fields.map(
          (f) => `${quote(f)}${idx.order ? ` ${idx.order.toUpperCase()}` : ""}`,
        );
        // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
        await this.executor.run(
          `CREATE INDEX IF NOT EXISTS ${quote(`idx_${name}_${j}`)} ON ${quote(name)} (${formatted.join(", ")})`,
        );
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
    const input = toInput(model.fields, data);
    const fields = Object.keys(input);
    const sqlFields = fields.map((f) => quote(f)).join(", ");
    const sqlPlaceholders = fields.map(() => placeholder()).join(", ");
    const sql = `INSERT INTO ${quote(modelName)} (${sqlFields}) VALUES (${sqlPlaceholders}) RETURNING *`;
    const row = await this.executor.get(
      sql,
      fields.map((f) => input[f]),
    );
    if (row === undefined || row === null) {
      // Fallback for drivers that don't support RETURNING (though Bun/Better-Sqlite3 do)
      const res = await this.find({
        model: modelName,
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- PK filter matches T
        where: buildIdentityFilter(model, getIdentityValues(model, data)) as Where<T>,

        select,
      });
      if (!res) throw new Error("Failed to insert record");
      return res;
    }
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row, select as Select<Record<string, unknown>>);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model: modelName, where, select } = args;
    const model = this.schema[modelName]!;
    const built = toWhere(model, where as Where);
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";
    const sql = `SELECT ${sqlSelect} FROM ${quote(modelName)} WHERE ${built.sql} LIMIT 1`;
    const row = await this.executor.get(sql, built.params);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    if (row === undefined || row === null) return null;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
    return toRow<T>(model, row, select as Select<Record<string, unknown>>);
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
    const { model: modelName, where, select, sortBy, limit, offset, cursor } = args;
    const model = this.schema[modelName]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where/Cursor/SortBy types match at runtime
    const built = toWhere(model, where as Where, cursor as Cursor, sortBy as SortBy[]);
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";
    let sql = `SELECT ${sqlSelect} FROM ${quote(modelName)} WHERE ${built.sql}`;

    if (sortBy && sortBy.length > 0) {
      const parts = sortBy.map(
        (s) => `${toColumnExpr(model, s.field, s.path)} ${(s.direction ?? "asc").toUpperCase()}`,
      );
      sql += ` ORDER BY ${parts.join(", ")}`;
    }
    if (limit !== undefined) {
      sql += ` LIMIT ${placeholder()}`;
      built.params.push(limit);
    }
    if (offset !== undefined) {
      sql += ` OFFSET ${placeholder()}`;
      built.params.push(offset);
    }
    const rows = await this.executor.all(sql, built.params);

    const result: T[] = [];
    for (let i = 0; i < rows.length; i++) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
      result.push(toRow<T>(model, rows[i]!, select as Select<Record<string, unknown>>));
    }
    return result;
  }

  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; data: Partial<T> }): Promise<T | null> {
    const { model: modelName, where, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const input = toInput(model.fields, data);
    const fields = Object.keys(input);
    if (fields.length === 0) return this.find({ model: modelName, where, select: undefined });

    const assignments = fields.map((f) => `${quote(f)} = ${placeholder()}`);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where);
    const sql = `UPDATE ${quote(modelName)} SET ${assignments.join(", ")} WHERE ${built.sql} RETURNING *`;
    const row = await this.executor.get(sql, [...fields.map((f) => input[f]), ...built.params]);
    if (row === undefined || row === null) return this.find({ model: modelName, where });
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row);
  }

  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model: modelName, where, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const input = toInput(model.fields, data);
    const fields = Object.keys(input);
    if (fields.length === 0) return 0;

    const assignments = fields.map((f) => `${quote(f)} = ${placeholder()}`);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where);
    const sql = `UPDATE ${quote(modelName)} SET ${assignments.join(", ")} WHERE ${built.sql}`;
    const res = await this.executor.run(sql, [...fields.map((f) => input[f]), ...built.params]);
    return res.changes;
  }

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
    const { model: modelName, create: cData, update: uData, where, select } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, uData);

    const cInput = toInput(model.fields, cData);
    const cFields = Object.keys(cInput);
    const uInput = toInput(model.fields, uData);
    const uFields = Object.keys(uInput);
    const pkFields = getPrimaryKeyFields(model);

    const sqlColumns = cFields.map((f) => quote(f)).join(", ");
    const sqlPlaceholders = cFields.map(() => placeholder()).join(", ");
    const sqlConflict = pkFields.map((f) => quote(f)).join(", ");

    let sql = `INSERT INTO ${quote(modelName)} (${sqlColumns}) VALUES (${sqlPlaceholders}) ON CONFLICT (${sqlConflict}) `;
    const params = cFields.map((f) => cInput[f]);

    if (uFields.length > 0) {
      const assignments: string[] = [];
      for (let i = 0; i < uFields.length; i++) {
        const f = uFields[i]!;
        assignments.push(`${quote(f)} = ${placeholder()}`);
        params.push(uInput[f]);
      }
      sql += `DO UPDATE SET ${assignments.join(", ")}`;
      if (where) {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
        const built = toWhere(model, where as Where);
        sql += ` WHERE ${built.sql}`;
        params.push(...built.params);
      }
    } else {
      sql += "DO NOTHING";
    }

    sql += " RETURNING *";
    const row = await this.executor.get(sql, params);
    if (row !== undefined && row !== null) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
      return toRow<T>(model, row, select as Select<Record<string, unknown>>);
    }

    const existing = await this.find({
      model: modelName,
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- PK filter matches T
      where: buildIdentityFilter(model, getIdentityValues(model, cData)) as Where<T>,
      select,
    });
    if (existing === null) throw new Error("Failed to refetch record after upsert");
    return existing;
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    const { model: modelName, where } = args;
    const model = this.schema[modelName]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where);
    await this.executor.run(`DELETE FROM ${quote(modelName)} WHERE ${built.sql}`, built.params);
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName, where } = args;
    const model = this.schema[modelName]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where);
    const res = await this.executor.run(
      `DELETE FROM ${quote(modelName)} WHERE ${built.sql}`,
      built.params,
    );
    return res.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName, where } = args;
    const model = this.schema[modelName]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where);
    const row = await this.executor.get(
      `SELECT COUNT(*) as count FROM ${quote(modelName)} WHERE ${built.sql}`,
      built.params,
    );
    return row === undefined || row === null ? 0 : Number(row["count"] ?? 0);
  }
}
