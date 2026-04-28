import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";
import type postgres from "postgres";

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

type PostgresJsSql = postgres.Sql;
type TransactionSql = postgres.TransactionSql;

export type PostgresDriver = PgClient | PgPool | PgPoolClient | PostgresJsSql | TransactionSql;

const MAX_CACHE_SIZE = 100;

// --- Internal PG Syntax Helpers ---

const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const placeholder = (i: number) => `$${i + 1}`;

function mapFieldType(field: Field): string {
  switch (field.type) {
    case "string":
      return field.max === undefined ? "TEXT" : `VARCHAR(${field.max})`;
    case "number":
      return "DOUBLE PRECISION";
    case "boolean":
      return "BOOLEAN";
    case "timestamp":
      return "BIGINT";
    case "json":
    case "json[]":
      return "JSONB";
    default:
      return "TEXT";
  }
}

function jsonExtract(
  column: string,
  path: string[],
  isNumeric?: boolean,
  isBoolean?: boolean,
): string {
  let segments = "";
  for (let i = 0; i < path.length; i++) {
    if (i > 0) segments += ", ";
    segments += `'${path[i]!.replaceAll("'", "''")}'`;
  }
  const base = `jsonb_extract_path_text(${column}, ${segments})`;
  if (isNumeric === true) return `(${base})::double precision`;
  if (isBoolean === true) return `(${base})::boolean`;
  return base;
}

function toColumnExpr(model: Model, fieldName: string, path?: string[], value?: unknown): string {
  if (!path || path.length === 0) return quote(fieldName);
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }
  return jsonExtract(quote(fieldName), path, typeof value === "number", typeof value === "boolean");
}

function toWhereRecursive(
  model: Model,
  where: Where,
  startIndex: number,
): { sql: string; params: unknown[] } {
  if ("and" in where) {
    const parts = [];
    const params = [];
    let currentIdx = startIndex;
    for (let i = 0; i < where.and.length; i++) {
      const built = toWhereRecursive(model, where.and[i]!, currentIdx);
      parts.push(`(${built.sql})`);
      for (let j = 0; j < built.params.length; j++) params.push(built.params[j]);
      currentIdx += built.params.length;
    }
    return { sql: parts.join(" AND "), params };
  }

  if ("or" in where) {
    const parts = [];
    const params = [];
    let currentIdx = startIndex;
    for (let i = 0; i < where.or.length; i++) {
      const built = toWhereRecursive(model, where.or[i]!, currentIdx);
      parts.push(`(${built.sql})`);
      for (let j = 0; j < built.params.length; j++) params.push(built.params[j]);
      currentIdx += built.params.length;
    }
    return { sql: parts.join(" OR "), params };
  }

  const expr = toColumnExpr(model, where.field, where.path, where.value);
  const val = where.value;

  switch (where.op) {
    case "eq":
      if (val === null) return { sql: `${expr} IS NULL`, params: [] };
      return { sql: `${expr} = ${placeholder(startIndex)}`, params: [val] };
    case "ne":
      if (val === null) return { sql: `${expr} IS NOT NULL`, params: [] };
      return { sql: `${expr} != ${placeholder(startIndex)}`, params: [val] };
    case "gt":
      return { sql: `${expr} > ${placeholder(startIndex)}`, params: [val] };
    case "gte":
      return { sql: `${expr} >= ${placeholder(startIndex)}`, params: [val] };
    case "lt":
      return { sql: `${expr} < ${placeholder(startIndex)}`, params: [val] };
    case "lte":
      return { sql: `${expr} <= ${placeholder(startIndex)}`, params: [val] };
    case "in": {
      if (!Array.isArray(val) || val.length === 0) return { sql: "1=0", params: [] };
      const phs = [];
      for (let i = 0; i < val.length; i++) phs.push(placeholder(startIndex + i));
      return { sql: `${expr} IN (${phs.join(", ")})`, params: val };
    }
    case "not_in": {
      if (!Array.isArray(val) || val.length === 0) return { sql: "1=1", params: [] };
      const phs = [];
      for (let i = 0; i < val.length; i++) phs.push(placeholder(startIndex + i));
      return { sql: `${expr} NOT IN (${phs.join(", ")})`, params: val };
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
  startIndex = 0,
): { sql: string; params: unknown[] } {
  const parts: string[] = [];
  const params: unknown[] = [];
  let nextIndex = startIndex;

  if (where) {
    const built = toWhereRecursive(model, where, nextIndex);
    parts.push(`(${built.sql})`);
    for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
    nextIndex += built.params.length;
  }

  if (cursor) {
    const paginationWhere = getPaginationFilter(cursor, sortBy);
    if (paginationWhere) {
      const built = toWhereRecursive(model, paginationWhere, nextIndex);
      parts.push(`(${built.sql})`);
      for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
      nextIndex += built.params.length;
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
    } else {
      res[k] = val;
    }
  }
  return res;
}

// --- Driver detection ---
// Bun SQL: has `unsafe` + `transaction` (and `begin`).
// postgres.js: has `unsafe` + `begin`, but NOT `transaction`.
// pg: has `query`.
// Order matters: check Bun SQL first (most specific), then postgres.js, then pg.

function isBunSql(driver: PostgresDriver): boolean {
  return "unsafe" in driver && "transaction" in driver;
}

function isPostgresJs(driver: PostgresDriver): driver is PostgresJsSql {
  return "unsafe" in driver && "begin" in driver;
}

function isPg(driver: PostgresDriver): driver is PgClient | PgPool | PgPoolClient {
  return "query" in driver;
}

// --- Executor factories ---

// postgres.js: uses `sql.unsafe(query, params, { prepare: true })` for server-side
// prepared statements. The driver manages statement name caching internally.
// Works for both Sql (top-level) and TransactionSql (inside begin/savepoint) since
// both extend ISql which provides `unsafe()`.
function createPostgresJsExecutor(
  sql: postgres.Sql | postgres.TransactionSql,
  inTransaction = false,
): QueryExecutor {
  const run = (query: string, params?: unknown[]) =>
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- postgres.js params type is a stricter version of unknown[]
    sql.unsafe(query, params as postgres.ParameterOrJSON<never>[], { prepare: true });

  return {
    all: async (query, params) => {
      const rows = await run(query, params);
      return rows as Record<string, unknown>[];
    },
    get: async (query, params) => {
      const rows = await run(query, params);
      return rows[0] as Record<string, unknown> | undefined;
    },
    run: async (query, params) => {
      const rows = await run(query, params);
      return { changes: rows.count ?? 0 };
    },
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) => {
      // Top-level Sql uses begin(); TransactionSql uses savepoint() for nesting
      if ("begin" in sql) {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- T matches return type of fn
        return sql.begin((tx) => fn(createPostgresJsExecutor(tx, true))) as Promise<T>;
      }
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- T matches return type of fn
      return sql.savepoint((tx) => fn(createPostgresJsExecutor(tx, true))) as Promise<T>;
    },
    inTransaction,
  };
}

// Bun SQL: uses `sql.unsafe(query, params)`. No prepare option — the driver
// manages prepared statements internally.
function createBunSqlExecutor(
  driver: Record<string, unknown>,
  inTransaction = false,
): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally checked in isBunSql
  const unsafeFn = driver["unsafe"] as (
    query: string,
    params?: unknown[],
  ) => Promise<
    Record<string, unknown>[] & { count?: number; affectedRows?: number; command?: string }
  >;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally checked in isBunSql
  const transactionFn = driver["transaction"] as (
    cb: (tx: unknown) => Promise<unknown>,
  ) => Promise<unknown>;

  return {
    all: (query, params) => unsafeFn(query, params),
    get: async (query, params) => {
      const rows = await unsafeFn(query, params);
      return rows[0];
    },
    run: async (query, params) => {
      const rows = await unsafeFn(query, params);
      let changes = rows.affectedRows ?? rows.count ?? 0;

      // Special treat for GreptimeDB over Postgres wire protocol:
      // command string might be "OK 1" while count/affectedRows is 0.
      if (changes === 0 && rows.command !== undefined && rows.command.startsWith("OK ")) {
        const parsed = parseInt(rows.command.slice(3), 10);
        if (!isNaN(parsed)) changes = parsed;
      }
      return { changes };
    },
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) =>
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- T matches return type of fn
      transactionFn((tx) =>
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Bun SQL transaction provides a driver-like object
        fn(createBunSqlExecutor(tx as Record<string, unknown>, true)),
      ) as Promise<T>,
    inTransaction,
  };
}

// pg: uses named queries with a bounded LRU cache for server-side prepared
// statement reuse. Each unique SQL string gets a stable name (e.g. `q_0`).
function createPgExecutor(
  driver: PgClient | PgPool | PgPoolClient,
  inTransaction = false,
): QueryExecutor {
  const cache = new Map<string, string>();
  let statementCount = 0;

  function getQuery(sql: string, values?: unknown[]) {
    let name = cache.get(sql);
    if (name === undefined) {
      if (cache.size >= MAX_CACHE_SIZE) {
        const first = cache.keys().next();
        if (first.done !== true) cache.delete(first.value);
      }
      name = `q_${statementCount++}`;
      cache.set(sql, name);
    }
    return { name, text: sql, values };
  }

  return {
    all: async (sql, params) => {
      const res = await driver.query<Record<string, unknown>>(getQuery(sql, params));
      return res.rows;
    },
    get: async (sql, params) => {
      const res = await driver.query<Record<string, unknown>>(getQuery(sql, params));
      return res.rows[0];
    },
    run: async (sql, params) => {
      const res = await driver.query(getQuery(sql, params));
      return { changes: res.rowCount ?? 0 };
    },
    transaction: async (fn) => {
      const isPool = "connect" in driver && !("release" in driver);
      if (isPool) {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is guaranteed to be PgPool by isPool check
        const client = await (driver as PgPool).connect();
        try {
          await client.query("BEGIN");
          const res = await fn(createPgExecutor(client, true));
          await client.query("COMMIT");
          return res;
        } catch (e) {
          await client.query("ROLLBACK");
          throw e;
        } finally {
          client.release();
        }
      }
      await driver.query("BEGIN");
      try {
        const res = await fn(createPgExecutor(driver, true));
        await driver.query("COMMIT");
        return res;
      } catch (e) {
        await driver.query("ROLLBACK");
        throw e;
      }
    },
    inTransaction,
  };
}

function createPostgresExecutor(driver: PostgresDriver): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally checked in isBunSql
  if (isBunSql(driver)) return createBunSqlExecutor(driver as unknown as Record<string, unknown>);
  if (isPostgresJs(driver)) return createPostgresJsExecutor(driver);
  if (isPg(driver)) return createPgExecutor(driver);
  throw new Error("Unsupported Postgres driver.");
}

// --- Adapter ---

export class PostgresAdapter<S extends Schema = Schema> implements Adapter<S> {
  private executor: QueryExecutor;

  constructor(
    private schema: S,
    driver: PostgresDriver | QueryExecutor,
  ) {
    this.executor = isQueryExecutor(driver) ? driver : createPostgresExecutor(driver);
  }

  async migrate(): Promise<void> {
    const models = Object.entries(this.schema);

    // Create tables first, then indexes — indexes depend on tables existing.
    // DDL must be sequential: some drivers don't support concurrent DDL on one connection.
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      const fields = Object.entries(model.fields);
      const columns: string[] = [];
      for (let j = 0; j < fields.length; j++) {
        const [fieldName, field] = fields[j]!;
        const nullable = field.nullable === true ? "" : " NOT NULL";
        columns.push(`${quote(fieldName)} ${mapFieldType(field)}${nullable}`);
      }
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
    return this.executor.transaction((exec) => fn(new PostgresAdapter(this.schema, exec)));
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
    const sqlValues = fields.map((_, i) => placeholder(i)).join(", ");
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";
    const sql = `INSERT INTO ${quote(modelName)} (${sqlFields}) VALUES (${sqlValues}) RETURNING ${sqlSelect}`;
    const row = await this.executor.get(
      sql,
      fields.map((f) => input[f]),
    );
    if (row === undefined || row === null) throw new Error("Failed to insert record");
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
      sql += ` LIMIT ${placeholder(built.params.length)}`;
      built.params.push(limit);
    }
    if (offset !== undefined) {
      sql += ` OFFSET ${placeholder(built.params.length)}`;
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

    const assignments = fields.map((f, i) => `${quote(f)} = ${placeholder(i)}`);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where, undefined, undefined, fields.length);
    const sql = `UPDATE ${quote(modelName)} SET ${assignments.join(", ")} WHERE ${built.sql} RETURNING *`;
    const row = await this.executor.get(sql, [...fields.map((f) => input[f]), ...built.params]);
    if (row === undefined || row === null) return null;
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

    const assignments = fields.map((f, i) => `${quote(f)} = ${placeholder(i)}`);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
    const built = toWhere(model, where as Where, undefined, undefined, fields.length);
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
    const sqlPlaceholders = cFields.map((_, i) => placeholder(i)).join(", ");
    const sqlConflict = pkFields.map((f) => quote(f)).join(", ");
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";

    let sql = `INSERT INTO ${quote(modelName)} (${sqlColumns}) VALUES (${sqlPlaceholders}) ON CONFLICT (${sqlConflict}) `;
    const params = cFields.map((f) => cInput[f]);

    if (uFields.length > 0) {
      const assignments = uFields.map((f, i) => `${quote(f)} = ${placeholder(cFields.length + i)}`);
      params.push(...uFields.map((f) => uInput[f]));
      sql += `DO UPDATE SET ${assignments.join(", ")}`;
      if (where) {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches at runtime
        const built = toWhere(model, where as Where, undefined, undefined, params.length);
        sql += ` WHERE ${built.sql}`;
        params.push(...built.params);
      }
    } else {
      sql += "DO NOTHING";
    }

    sql += ` RETURNING ${sqlSelect}`;
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
