import { createHash } from "node:crypto";

import type { SQL as BunSQL } from "bun";
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

type PostgresJsSql = postgres.Sql;
type TransactionSql = postgres.TransactionSql;

export type PostgresDriver =
  | PgClient
  | PgPool
  | PgPoolClient
  | PostgresJsSql
  | TransactionSql
  | BunSQL;

// --- Internal PG Syntax Helpers ---

const ident = (s: string) => raw(`"${s}"`);
const selectCols = (select?: readonly string[]) => (select ? idList(select) : raw("*"));

function mapFromRecord<T extends Record<string, unknown>>(
  model: Model,
  record: Record<string, unknown>,
): T {
  const fields = model.fields;
  const keys = Object.keys(record);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const field = fields[k];
    if (field?.type === "timestamp" && typeof record[k] === "string") {
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
    const field = fields[k];
    // Postgres drivers handle JS objects for jsonb, but json[] often requires stringification
    if (field?.type === "json[]" && val !== null) {
      res[k] = JSON.stringify(val);
    } else {
      res[k] = val;
    }
  }
  return res;
}

function sqlType(field: Field): string {
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
      return "JSONB"; // Postgres stores JSON as binary jsonb for efficiency
    default:
      return "TEXT";
  }
}

function toColumnExpr(model: Model, fieldName: string, path?: string[], value?: unknown): Sql {
  if (!path || path.length === 0) return ident(fieldName);
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }

  const isNumeric = typeof value === "number";
  const isBoolean = typeof value === "boolean";

  let res = sql`jsonb_extract_path_text(${ident(fieldName)}, ${paramList(path)})`;
  if (isNumeric) {
    res = sql`(${res})::double precision`;
  } else if (isBoolean) {
    res = sql`(${res})::boolean`;
  }
  return res;
}

// --- Driver detection ---

function isBunSql(driver: PostgresDriver): driver is BunSQL {
  return typeof driver === "function" && "unsafe" in driver && "transaction" in driver;
}

function isPostgresJs(driver: PostgresDriver): driver is PostgresJsSql {
  return "unsafe" in driver && "begin" in driver;
}

function isPg(driver: PostgresDriver): driver is PgClient | PgPool | PgPoolClient {
  return "query" in driver;
}

const isPgPool = (d: PgClient | PgPool | PgPoolClient): d is PgPool =>
  "connect" in d && !("release" in d);

// --- Executor factories ---

function createPostgresJsExecutor(
  driver: postgres.Sql | postgres.TransactionSql,
  inTransaction = false,
): QueryExecutor {
  const runQuery = (query: Sql) => {
    const [strings, ...params] = query.toTaggedArgs();
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- calling driver as tagged template function to avoid .unsafe()
    const run = driver as (
      s: TemplateStringsArray,
      ...p: unknown[]
    ) => Promise<Record<string, unknown>[]>;
    return run(strings, ...params);
  };

  return {
    all: (query) => {
      return runQuery(query);
    },
    get: async (query) => {
      const rows = await runQuery(query);
      return rows[0];
    },
    run: async (query) => {
      const rows = await runQuery(query);
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- postgres.js returns result with .count
      const r = rows as unknown as { count?: number };
      return { changes: r.count ?? 0 };
    },
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) => {
      // PostgresAdapter.transaction() short-circuits nested calls with `if (this.executor.inTransaction) return fn(this)`.
      // This means we only ever enter here when NOT in a transaction, so we always use `begin` and never `savepoint`.
      if ("begin" in driver) {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- T matches return type of fn
        return driver.begin((tx) => fn(createPostgresJsExecutor(tx, true))) as Promise<T>;
      }
      throw new Error("Transaction not supported by driver (begin missing)");
    },
    inTransaction,
  };
}

function createBunSqlExecutor(bunSql: BunSQL, inTransaction = false): QueryExecutor {
  const runQuery = (query: Sql) => {
    const [strings, ...params] = query.toTaggedArgs();
    return bunSql(strings, ...params) as Promise<Record<string, unknown>[]>;
  };

  return {
    all: (query) => runQuery(query),
    get: async (query) => {
      const rows = await runQuery(query);
      return rows[0];
    },
    run: async (query) => {
      const rows = await runQuery(query);
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver result has count/affectedRows/command
      const r = rows as unknown as { affectedRows?: number; count?: number; command?: string };
      let changes = r.affectedRows ?? r.count ?? 0;
      if (changes === 0 && r.command !== undefined && r.command.startsWith("OK ")) {
        const parsed = parseInt(r.command.slice(3), 10);
        if (!isNaN(parsed)) changes = parsed;
      }
      return { changes };
    },
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) =>
      bunSql.transaction((tx) => fn(createBunSqlExecutor(tx as BunSQL, true))),
    inTransaction,
  };
}

function createPgExecutor(
  driver: PgClient | PgPool | PgPoolClient,
  inTransaction = false,
): QueryExecutor {
  function getPrepared(query: Sql) {
    // pg needs a single string with $1, $2 placeholders
    const text = query.strings.reduce(
      (acc, s, i) => acc + s + (i < query.params.length ? "$" + (i + 1) : ""),
      "",
    );
    const values = query.params;

    const name = `q_${createHash("sha1").update(text).digest("hex").slice(0, 16)}`;
    return { name, text, values };
  }

  return {
    all: async (q) => {
      const res = await driver.query<Record<string, unknown>>(getPrepared(q));
      return res.rows;
    },
    get: async (q) => {
      const res = await driver.query<Record<string, unknown>>(getPrepared(q));
      return res.rows[0];
    },
    run: async (q) => {
      const res = await driver.query(getPrepared(q));
      return { changes: res.rowCount ?? 0 };
    },
    transaction: async (fn) => {
      if (isPgPool(driver)) {
        const client = await driver.connect();
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
  if (isBunSql(driver)) return createBunSqlExecutor(driver);
  if (isPostgresJs(driver)) return createPostgresJsExecutor(driver);
  if (isPg(driver)) return createPgExecutor(driver);
  throw new Error("Unsupported Postgres driver.");
}

// --- Adapter ---

/**
 * Postgres Adapter for no-orm.
 */
export class PostgresAdapter<S extends Schema> implements Adapter<S> {
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
      const columnParts: string[] = [];
      for (let j = 0; j < fields.length; j++) {
        const [fieldName, field] = fields[j]!;
        const type = sqlType(field);
        const nullable = field.nullable === true ? "" : " NOT NULL";
        columnParts.push(`"${fieldName}" ${type}${nullable}`);
      }
      const primaryKeyFields = getPrimaryKeyFields(model);
      const pk = `PRIMARY KEY (${primaryKeyFields.map((f) => `"${f}"`).join(", ")})`;
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      await this.executor.run(sql`
        CREATE TABLE IF NOT EXISTS ${ident(name)} (
          ${raw(columnParts.join(", "))},
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
    return this.executor.transaction((exec) => fn(new PostgresAdapter(this.schema, exec)));
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
    if (row === undefined || row === null) throw new Error("Failed to insert record");
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr, cursor, sortBy })}
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
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
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
    `;
    const row = await this.executor.get(query);
    return Number(row?.["count"] ?? 0);
  }
}
