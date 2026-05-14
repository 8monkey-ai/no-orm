import type { SQL as BunSQL } from "bun";
import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";
import type postgres from "postgres";

import type {
  Adapter,
  Field,
  FieldName,
  InferModel,
  Schema,
  SortBy,
  Where,
  Cursor,
  Model,
} from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildPrimaryKeyFilter,
  fnv1aHash,
  getPrimaryKeyFieldNames,
  getPrimaryKeyValues,
  mapNumeric,
  type Project,
  type RowData,
} from "./utils/common";
import {
  type Fragment,
  type QueryExecutor,
  isQueryExecutor,
  id,
  extractFields,
  where,
  set,
  sort,
  stringifyJsonParam,
  toNumberedParams,
  selectSql,
  insertSql,
  updateSql,
  deleteSql,
  upsertSql,
  countSql,
  migrateSqls,
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

function mapFromRecord<T extends RowData, F extends FieldName<T> = never>(
  model: Model,
  record: RowData,
): Project<T, F> {
  const fields = model.fields;
  const keys = Object.keys(record);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const field = fields[k];
    if (field?.type === "timestamp" && typeof record[k] === "string") {
      record[k] = mapNumeric(record[k]);
    }
  }
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- DB returned only the selected columns; body only coerces field types
  return record as Project<T, F>;
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
      return "JSONB";
    default:
      return "TEXT";
  }
}

function toColumnExpr(model: Model, fieldName: string, path?: string[], value?: unknown): Fragment {
  if (!path || path.length === 0) return id(fieldName);
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }

  const hint: unknown = Array.isArray(value) ? value[0] : value;
  const isNumeric = typeof hint === "number";
  const isBoolean = typeof hint === "boolean";

  // Path elements are schema-defined identifiers; safe to inline as SQL string literals.
  let pathStr = id(fieldName).text;
  for (let i = 0; i < path.length; i++) pathStr += `, '${path[i]!}'`;

  let text = `jsonb_extract_path_text(${pathStr})`;
  if (isNumeric) text = `(${text})::double precision`;
  else if (isBoolean) text = `(${text})::boolean`;
  return { text, params: [] };
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

// --- Driver result types ---
// postgres.js and Bun SQL attach metadata (affected row count, command) to the result array object itself.
type PostgresJsResult = Record<string, unknown>[] & { count?: number };
type BunSqlResult = Record<string, unknown>[] & {
  affectedRows?: number;
  count?: number;
  command?: string;
};

// --- Executor factories ---

function toTaggedArgs(query: Fragment): [TemplateStringsArray, ...unknown[]] {
  const parts = query.text.split("?") as string[] & { raw: readonly string[] };
  parts.raw = parts.slice();
  return [parts as TemplateStringsArray, ...query.params];
}

function createPostgresJsExecutor(
  driver: postgres.Sql | postgres.TransactionSql,
  inTransaction = false,
): QueryExecutor {
  const runQuery = (query: Fragment): Promise<PostgresJsResult> => {
    const [strings, ...params] = toTaggedArgs(query);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- calling driver as tagged template to use native parameterization; result has .count on the array object
    const run = driver as (s: TemplateStringsArray, ...p: unknown[]) => Promise<PostgresJsResult>;
    return run(strings, ...params);
  };

  return {
    all: (query) => runQuery(query),
    get: async (query) => {
      const rows = await runQuery(query);
      return rows[0];
    },
    run: async (query) => {
      const rows = await runQuery(query);
      return { changes: rows.count ?? 0 };
    },
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) => {
      // PostgresAdapter.transaction() short-circuits nested calls so we only enter here outside a transaction.
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
  const runQuery = (query: Fragment): Promise<BunSqlResult> => {
    const [strings, ...params] = toTaggedArgs(query);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- bun:sql result has affectedRows/count/command on the array object
    return bunSql(strings, ...params) as Promise<BunSqlResult>;
  };

  return {
    all: (query) => runQuery(query),
    get: async (query) => {
      const rows = await runQuery(query);
      return rows[0];
    },
    run: async (query) => {
      const rows = await runQuery(query);
      let changes = rows.affectedRows ?? rows.count ?? 0;
      if (changes === 0 && rows.command !== undefined && rows.command.startsWith("OK ")) {
        const parsed = parseInt(rows.command.slice(3), 10);
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
  function getPrepared(query: Fragment) {
    const { text, values: rawValues } = toNumberedParams(query);
    const values = rawValues.map(stringifyJsonParam);
    const name = `q_${fnv1aHash(text)}`;
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
    const stmts = migrateSqls(this.schema, { sqlType });
    await this.executor.transaction(async (exec) => {
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      for (let i = 0; i < stmts.length; i++) await exec.run(stmts[i]!);
    });
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    if (this.executor.inTransaction) return fn(this);
    return this.executor.transaction((exec) => fn(new PostgresAdapter(this.schema, exec)));
  }

  async create<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    data: InferModel<S[K]>;
    select?: readonly F[];
  }): Promise<[F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>> {
    type Row = InferModel<S[K]>;
    const { model: modelName, data, select } = args;
    const model = this.schema[modelName]!;
    const { fields, values } = extractFields(data as Record<string, unknown>);
    const query = insertSql({ table: modelName, fields, values, returning: select });

    const row = await this.executor.get(query);
    if (row === undefined || row === null) throw new Error("Failed to insert record");
    return mapFromRecord<Row, F>(model, row);
  }

  async find<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    where: Where<InferModel<S[K]>>;
    select?: readonly F[];
  }): Promise<([F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>) | null> {
    type Row = InferModel<S[K]>;
    const { model: modelName, select } = args;
    const model = this.schema[modelName]!;
    const query = selectSql({
      table: modelName,
      select,
      where: where(args.where, { model, columnExpr: toColumnExpr }),
      limit: 1,
    });

    const row = await this.executor.get(query);
    if (row === undefined || row === null) return null;
    return mapFromRecord<Row, F>(model, row);
  }

  async findMany<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
    select?: readonly F[];
    sortBy?: SortBy<InferModel<S[K]>>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<InferModel<S[K]>>;
  }): Promise<([F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>)[]> {
    type Row = InferModel<S[K]>;
    const { model: modelName, select, sortBy, limit, offset, cursor } = args;
    const model = this.schema[modelName]!;
    const query = selectSql({
      table: modelName,
      select,
      where: where(args.where, { model, columnExpr: toColumnExpr, cursor, sortBy }),
      orderBy: sortBy && sortBy.length > 0 ? sort(model, sortBy, toColumnExpr) : undefined,
      limit,
      offset,
    });
    const rows = await this.executor.all(query);

    const result: Project<Row, F>[] = [];
    for (let i = 0; i < rows.length; i++) {
      result.push(mapFromRecord<Row, F>(model, rows[i]!));
    }
    return result;
  }

  /**
   * Updates the first record matching the criteria. Primary key updates are rejected.
   */
  async update<K extends keyof S & string>(args: {
    model: K;
    where: Where<InferModel<S[K]>>;
    data: Partial<InferModel<S[K]>>;
  }): Promise<InferModel<S[K]> | null> {
    type Row = InferModel<S[K]>;
    const { model: modelName, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const dataRecord = data as Record<string, unknown>;

    if (!Object.keys(dataRecord).some((k) => dataRecord[k] !== undefined))
      return this.find({ model: modelName, where: args.where, select: undefined });

    const innerWhere = where(args.where, { model, columnExpr: toColumnExpr });
    const query = updateSql({
      table: modelName,
      set: set(dataRecord),
      where: {
        text: `ctid = (SELECT ctid FROM ${id(modelName).text} WHERE ${innerWhere.text} LIMIT 1)`,
        params: innerWhere.params,
      },
      returning: true,
    });

    const row = await this.executor.get(query);
    if (row === undefined || row === null)
      return this.find({ model: modelName, where: args.where });
    return mapFromRecord<Row>(model, row);
  }

  /**
   * Updates all records matching the criteria. Primary key updates are rejected.
   */
  async updateMany<K extends keyof S & string>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
    data: Partial<InferModel<S[K]>>;
  }): Promise<number> {
    const { model: modelName, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const dataRecord = data as Record<string, unknown>;
    if (!Object.keys(dataRecord).some((k) => dataRecord[k] !== undefined)) return 0;

    const query = updateSql({
      table: modelName,
      set: set(dataRecord),
      where: where(args.where, { model, columnExpr: toColumnExpr }),
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
  async upsert<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    create: InferModel<S[K]>;
    update: Partial<InferModel<S[K]>>;
    where?: Where<InferModel<S[K]>>;
    select?: readonly F[];
  }): Promise<[F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>> {
    type Row = InferModel<S[K]>;
    const { model: modelName, create: createData, update: updateData, select } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, updateData);

    const { fields: createFields, values: createValues } = extractFields(
      createData as Record<string, unknown>,
    );

    const rawUpdate = updateData as Record<string, unknown>;
    const hasUpdateFields = Object.keys(rawUpdate).some((k) => rawUpdate[k] !== undefined);
    const primaryKeyFieldNames = getPrimaryKeyFieldNames(model);

    const qualifiedColumnExpr = (m: Model, fieldName: string, path?: string[], value?: unknown): Fragment => {
      if (!path || path.length === 0) return { text: `${id(modelName).text}.${id(fieldName).text}`, params: [] };
      return toColumnExpr(m, fieldName, path, value);
    };

    let onConflict: Fragment;
    if (!hasUpdateFields) {
      onConflict = { text: "DO NOTHING", params: [] };
    } else if (args.where) {
      const updateSet = set(rawUpdate);
      const updateWhere = where(args.where, { model, columnExpr: qualifiedColumnExpr });
      const params = updateSet.params.slice();
      for (let i = 0; i < updateWhere.params.length; i++) params.push(updateWhere.params[i]);
      onConflict = { text: `DO UPDATE SET ${updateSet.text} WHERE ${updateWhere.text}`, params };
    } else {
      const updateSet = set(rawUpdate);
      onConflict = { text: `DO UPDATE SET ${updateSet.text}`, params: updateSet.params };
    }

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
      return mapFromRecord<Row, F>(model, row);
    }

    const existing = await this.find({
      model: modelName,
      where: buildPrimaryKeyFilter<Row>(model, getPrimaryKeyValues(model, createData)),
      select,
    });
    if (existing === null) throw new Error("Failed to refetch record after upsert");
    return existing;
  }

  async delete<K extends keyof S & string>(args: {
    model: K;
    where: Where<InferModel<S[K]>>;
  }): Promise<void> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = deleteSql({
      table: modelName,
      where: where(args.where, { model, columnExpr: toColumnExpr }),
    });
    await this.executor.run(query);
  }

  async deleteMany<K extends keyof S & string>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
  }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = deleteSql({
      table: modelName,
      where: where(args.where, { model, columnExpr: toColumnExpr }),
    });
    const res = await this.executor.run(query);
    return res.changes;
  }

  async count<K extends keyof S & string>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
  }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const query = countSql({
      table: modelName,
      where: where(args.where, { model, columnExpr: toColumnExpr }),
    });
    const row = await this.executor.get(query);
    return Number(row?.["count"] ?? 0);
  }
}
