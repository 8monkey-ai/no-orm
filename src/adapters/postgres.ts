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
} from "./utils/common";
import {
  type QueryExecutor,
  isQueryExecutor,
  toRow,
  toDbRow,
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

/**
 * Limits the number of prepared statement objects kept in memory to prevent leaks
 * while allowing statement reuse for performance.
 */
const MAX_CACHED_STATEMENTS = 100;

// --- Internal PG Syntax Helpers ---

const quote = (s: string) => `"${s}"`;
const ident = (s: string) => raw(quote(s));
const selectCols = (select?: readonly unknown[]) =>
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Select<T> keys are strings at runtime
  select ? idList(select as readonly string[], quote) : raw("*");

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

// --- Executor factories ---

function createPostgresJsExecutor(
  driver: postgres.Sql | postgres.TransactionSql,
  inTransaction = false,
): QueryExecutor {
  const runQuery = (query: Sql) => {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- constructing TemplateStringsArray for driver call
    const strings = query.strings as string[] & { raw: string[] };
    strings.raw = query.strings;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion, typescript-eslint/no-unsafe-return -- calling driver as tagged template function to avoid .unsafe()
    const run = driver as (
      strings: TemplateStringsArray,
      ...params: unknown[]
    ) => Promise<Record<string, unknown>[]>;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- TemplateStringsArray is required by the driver's tagged template signature
    return run(strings as unknown as TemplateStringsArray, ...query.params);
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
      // eslint-disable-next-line typescript-eslint/no-unsafe-assignment -- driver returns result with count/affectedRows
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
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- constructing TemplateStringsArray for driver call
    const strings = query.strings as string[] & { raw: string[] };
    strings.raw = query.strings;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver call expects TemplateStringsArray
    return bunSql(strings as unknown as TemplateStringsArray, ...query.params) as Promise<
      Record<string, unknown>[]
    >;
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
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Bun TransactionSQL extends SQL
      bunSql.transaction((tx) => fn(createBunSqlExecutor(tx as unknown as BunSQL, true))),
    inTransaction,
  };
}

function createPgExecutor(
  driver: PgClient | PgPool | PgPoolClient,
  inTransaction = false,
): QueryExecutor {
  const cache = new Map<string, string>();
  let statementCount = 0;

  function getPrepared(query: Sql) {
    // pg needs a single string with $1, $2 placeholders
    const text = query.strings.reduce(
      (acc, s, i) => acc + s + (i < query.params.length ? "$" + (i + 1) : ""),
      "",
    );
    const values = query.params;

    let name = cache.get(text);
    if (name === undefined) {
      if (cache.size >= MAX_CACHED_STATEMENTS) {
        const first = cache.keys().next();
        if (first.done !== true) cache.delete(first.value);
      }
      name = `q_${statementCount++}`;
      cache.set(text, name);
    }
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
        columnParts.push(`${quote(fieldName)} ${type}${nullable}`);
      }
      const primaryKeyFields = getPrimaryKeyFields(model);
      const pk = `PRIMARY KEY (${primaryKeyFields.map((f) => quote(f)).join(", ")})`;
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
          (f) => `${quote(f)}${idx.order ? ` ${idx.order.toUpperCase()}` : ""}`,
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
    const input = toDbRow(model, data);
    const fields = Object.keys(input);
    const query = sql`
      INSERT INTO ${ident(modelName)} (${idList(fields, quote)})
      VALUES (${paramList(fields.map((f) => input[f]))})
      RETURNING ${selectCols(select)}
    `;

    const row = await this.executor.get(query);
    if (row === undefined || row === null) throw new Error("Failed to insert record");
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row, select);
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
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
    return toRow<T>(model, row, select);
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
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
      result.push(toRow<T>(model, rows[i]!, select));
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
    const input = toDbRow(model, data);
    const fields = Object.keys(input);

    if (fields.length === 0)
      return this.find({ model: modelName, where: args.where, select: undefined });

    const query = sql`
      UPDATE ${ident(modelName)}
      SET ${set(input, quote)}
      WHERE ${where(args.where, { model, columnExpr: toColumnExpr })}
      RETURNING *
    `;

    const row = await this.executor.get(query);
    if (row === undefined || row === null)
      return this.find({ model: modelName, where: args.where });
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row);
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
    const input = toDbRow(model, data);
    const fields = Object.keys(input);
    if (fields.length === 0) return 0;

    const query = sql`
      UPDATE ${ident(modelName)}
      SET ${set(input, quote)}
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

    const insertRow = toDbRow(model, createData);
    const createFields = Object.keys(insertRow);
    const updateRow = toDbRow(model, updateData);
    const updateFields = Object.keys(updateRow);
    const primaryKeyFields = getPrimaryKeyFields(model);

    const action =
      updateFields.length === 0
        ? sql`DO NOTHING`
        : args.where
          ? sql`DO UPDATE SET ${set(updateRow, quote)} WHERE ${where(args.where, {
              model,
              columnExpr: toColumnExpr,
            })}`
          : sql`DO UPDATE SET ${set(updateRow, quote)}`;

    const query = sql`
      INSERT INTO ${ident(modelName)} (${idList(createFields, quote)})
      VALUES (${paramList(createFields.map((f) => insertRow[f]))})
      ON CONFLICT (${idList(primaryKeyFields, quote)}) ${action}
      RETURNING ${selectCols(select)}
    `;

    const row = await this.executor.get(query);
    if (row !== undefined && row !== null) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
      return toRow<T>(model, row, select);
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
