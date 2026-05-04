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
  getPaginationFilter,
  getPrimaryKeyFields,
  getPrimaryKeyValues,
} from "./utils/common";
import {
  type QueryExecutor,
  isQueryExecutor,
  toRow,
  toDbRow,
  type Fragment,
  join,
  wrap,
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

function toColumnExpr(
  model: Model,
  fieldName: string,
  path?: string[],
  value?: unknown,
  quoteFn: (s: string) => string = quote,
): Fragment {
  const quoted = quoteFn(fieldName);
  if (!path || path.length === 0) return { strings: [quoted], params: [] };
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }

  const isNumeric = typeof value === "number";
  const isBoolean = typeof value === "boolean";

  const strings = [
    `jsonb_extract_path_text(${quoted}, `,
    // eslint-disable-next-line unicorn/no-new-array -- creating array of specific length for placeholders
    ...new Array<string>(path.length - 1).fill(", "),
    ")",
  ];
  if (isNumeric) {
    strings[0] = "(" + strings[0]!;
    strings[strings.length - 1] += ")::double precision";
  } else if (isBoolean) {
    strings[0] = "(" + strings[0]!;
    strings[strings.length - 1] += ")::boolean";
  }
  return { strings, params: path };
}

function toWhereRecursive<T>(
  model: Model,
  where: Where<T>,
  quoteFn: (s: string) => string = quote,
): Fragment {
  if ("and" in where) {
    const parts: Fragment[] = [];
    for (let i = 0; i < where.and.length; i++) {
      parts.push(wrap(toWhereRecursive(model, where.and[i]!, quoteFn), "(", ")"));
    }
    return join(parts, " AND ");
  }

  if ("or" in where) {
    const parts: Fragment[] = [];
    for (let i = 0; i < where.or.length; i++) {
      parts.push(wrap(toWhereRecursive(model, where.or[i]!, quoteFn), "(", ")"));
    }
    return join(parts, " OR ");
  }

  const expr = toColumnExpr(model, where.field as string, where.path, where.value, quoteFn);
  const val = where.value;

  switch (where.op) {
    case "eq":
      if (val === null) return wrap(expr, "", " IS NULL");
      return join([expr, { strings: [" = ", ""], params: [val] }], "");
    case "ne":
      if (val === null) return wrap(expr, "", " IS NOT NULL");
      return join([expr, { strings: [" != ", ""], params: [val] }], "");
    case "gt":
      return join([expr, { strings: [" > ", ""], params: [val] }], "");
    case "gte":
      return join([expr, { strings: [" >= ", ""], params: [val] }], "");
    case "lt":
      return join([expr, { strings: [" < ", ""], params: [val] }], "");
    case "lte":
      return join([expr, { strings: [" <= ", ""], params: [val] }], "");
    case "in": {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val cast to unknown array for in operator
      const vArr = val as unknown[];
      if (!Array.isArray(vArr) || vArr.length === 0) return { strings: ["1=0"], params: [] };
      const inFrag: Fragment = {
        // eslint-disable-next-line unicorn/no-new-array -- creating array of specific length for placeholders
        strings: [" IN (", ...new Array<string>(vArr.length - 1).fill(", "), ")"],
        params: vArr,
      };
      return join([expr, inFrag], "");
    }
    case "not_in": {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val cast to unknown array for not_in operator
      const vArr = val as unknown[];
      if (!Array.isArray(vArr) || vArr.length === 0) return { strings: ["1=1"], params: [] };
      const inFrag: Fragment = {
        // eslint-disable-next-line unicorn/no-new-array -- creating array of specific length for placeholders
        strings: [" NOT IN (", ...new Array<string>(vArr.length - 1).fill(", "), ")"],
        params: vArr,
      };
      return join([expr, inFrag], "");
    }
    default:
      throw new Error(`Unsupported operator: ${String((where as Record<string, unknown>)["op"])}`);
  }
}

function toWhere<T>(
  model: Model,
  where?: Where<T>,
  cursor?: Cursor<T>,
  sortBy?: SortBy<T>[],
  quoteFn: (s: string) => string = quote,
): Fragment {
  const parts: Fragment[] = [];

  if (where) {
    parts.push(wrap(toWhereRecursive(model, where, quoteFn), "(", ")"));
  }

  if (cursor) {
    const paginationWhere = getPaginationFilter(cursor, sortBy);
    if (paginationWhere) {
      parts.push(wrap(toWhereRecursive(model, paginationWhere, quoteFn), "(", ")"));
    }
  }

  return parts.length > 0 ? join(parts, " AND ") : { strings: ["1=1"], params: [] };
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
  sql: postgres.Sql | postgres.TransactionSql,
  inTransaction = false,
): QueryExecutor {
  const runQuery = (query: Fragment) => {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- constructing TemplateStringsArray for driver call
    const strings = query.strings as string[] & { raw: string[] };
    strings.raw = query.strings;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion, typescript-eslint/no-unsafe-return -- calling driver as tagged template function to avoid .unsafe()
    const run = sql as (
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
      if ("begin" in sql) {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- T matches return type of fn
        return sql.begin((tx) => fn(createPostgresJsExecutor(tx, true))) as Promise<T>;
      }
      throw new Error("Transaction not supported by driver (begin missing)");
    },
    inTransaction,
  };
}

function createBunSqlExecutor(bunSql: BunSQL, inTransaction = false): QueryExecutor {
  const runQuery = (query: Fragment) => {
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

  function getQuery(query: Fragment) {
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
      const res = await driver.query<Record<string, unknown>>(getQuery(q));
      return res.rows;
    },
    get: async (q) => {
      const res = await driver.query<Record<string, unknown>>(getQuery(q));
      return res.rows[0];
    },
    run: async (q) => {
      const res = await driver.query(getQuery(q));
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
 *
 * Notes:
 * - Sequential DDL: Create tables first, then indexes.
 * - SQLite stores JSON as text; Postgres stores JSON as jsonb.
 * - Driver support: node-postgres (pg), postgres.js, and Bun.SQL.
 * - upsert always conflicts on the Primary Key.
 * - Optional where in upsert acts as a predicate -- record is only updated if condition is met.
 * - Primary-key updates are rejected to keep adapter behavior consistent.
 * - number and timestamp use standard JavaScript Number. bigint is not supported in v1.
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
        const type = mapFieldType(field);
        const nullable = field.nullable === true ? "" : " NOT NULL";
        columnParts.push(`${quote(fieldName)} ${type}${nullable}`);
      }
      const primaryKeyFields = getPrimaryKeyFields(model);
      const pk = `PRIMARY KEY (${primaryKeyFields.map((f) => quote(f)).join(", ")})`;
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      await this.executor.run({
        strings: [
          `CREATE TABLE IF NOT EXISTS ${quote(name)} (${columnParts.join(", ")}, ${pk})`,
        ],
        params: [],
      });
    }

    // Now create indexes
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      if (!model.indexes) continue;
      for (let j = 0; j < model.indexes.length; j++) {
        const idx = model.indexes[j]!;
        const fields = Array.isArray(idx.field) ? idx.field : [idx.field];
        const formatted = fields.map(
          (f) =>
            `${quote(f)}${idx.order ? ` ${idx.order.toUpperCase()}` : ""}`,
        );
        // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
        await this.executor.run({
          strings: [
            `CREATE INDEX IF NOT EXISTS ${quote(`idx_${name}_${j}`)} ON ${quote(name)} (${formatted.join(", ")})`,
          ],
          params: [],
        });
      }
    }
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    if (this.executor.inTransaction) return fn(this);
    return this.executor.transaction((exec) =>
      fn(new PostgresAdapter(this.schema, exec)),
    );
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    const { model: modelName, data, select } = args;
    const model = this.schema[modelName]!;
    const input = toDbRow(model, data);
    const fields = Object.keys(input);
    const sqlFields = fields.map((f) => quote(f)).join(", ");
    const sqlSelect = select
      ? select.map((s) => quote(s)).join(", ")
      : "*";

    const strings = [`INSERT INTO ${quote(modelName)} (${sqlFields}) VALUES (`];
    for (let i = 1; i < fields.length; i++) strings.push(", ");
    strings.push(`) RETURNING ${sqlSelect}`);
    const params = fields.map((f) => input[f]);

    const row = await this.executor.get({ strings, params });
    if (row === undefined || row === null) throw new Error("Failed to insert record");
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row, select);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model: modelName, where, select } = args;
    const model = this.schema[modelName]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where matches model fields
    const built = toWhere(model, where);
    const sqlSelect = select
      ? select.map((s) => quote(s)).join(", ")
      : "*";

    const query = wrap(
      built,
      `SELECT ${sqlSelect} FROM ${quote(modelName)} WHERE `,
      " LIMIT 1",
    );

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
    const { model: modelName, where, select, sortBy, limit, offset, cursor } = args;
    const model = this.schema[modelName]!;
    const built = toWhere(model, where, cursor, sortBy);
    const sqlSelect = select
      ? select.map((s) => quote(s)).join(", ")
      : "*";

    const query = wrap(
      built,
      `SELECT ${sqlSelect} FROM ${quote(modelName)} WHERE `,
      "",
    );

    if (sortBy && sortBy.length > 0) {
      query.strings[query.strings.length - 1] += " ORDER BY ";
      for (let i = 0; i < sortBy.length; i++) {
        const s = sortBy[i]!;
        const expr = toColumnExpr(model, s.field, s.path);
        const dir = (s.direction ?? "asc").toUpperCase();
        if (i > 0) query.strings[query.strings.length - 1] += ", ";
        query.strings[query.strings.length - 1] += expr.strings[0]!;
        for (let j = 1; j < expr.strings.length; j++) {
          query.strings.push(expr.strings[j]!);
        }
        for (let j = 0; j < expr.params.length; j++) {
          query.params.push(expr.params[j]);
        }
        query.strings[query.strings.length - 1] += ` ${dir}`;
      }
    }
    if (limit !== undefined) {
      query.strings[query.strings.length - 1] += " LIMIT ";
      query.strings.push("");
      query.params.push(limit);
    }
    if (offset !== undefined) {
      query.strings[query.strings.length - 1] += " OFFSET ";
      query.strings.push("");
      query.params.push(offset);
    }
    const rows = await this.executor.all(query);

    const result: T[] = [];
    for (let i = 0; i < rows.length; i++) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
      result.push(toRow<T>(model, rows[i]!, select));
    }
    return result;
  }

  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: Partial<T>; where: Where<T> }): Promise<T | null> {
    const { model: modelName, data, where } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const input = toDbRow(model, data);
    const fields = Object.keys(input);

    if (fields.length === 0) return this.find({ model: modelName, where, select: undefined });

    const setParts: Fragment[] = [];
    for (let i = 0; i < fields.length; i++) {
      const f = fields[i]!;
      setParts.push({
        strings: [`${quote(f)} = `, ""],
        params: [input[f]],
      });
    }
    const setFrag = join(setParts, ", ");

    const whereFrag = toWhere(model, where);
    const query = join(
      [wrap(setFrag, `UPDATE ${quote(modelName)} SET `, ""), whereFrag],
      " WHERE ",
    );
    query.strings[query.strings.length - 1] += " RETURNING *";

    const row = await this.executor.get(query);
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
    const input = toDbRow(model, data);
    const fields = Object.keys(input);
    if (fields.length === 0) return 0;

    const setParts: Fragment[] = [];
    for (let i = 0; i < fields.length; i++) {
      const f = fields[i]!;
      setParts.push({
        strings: [`${quote(f)} = `, ""],
        params: [input[f]],
      });
    }
    const setFrag = join(setParts, ", ");

    const whereFrag = toWhere(model, where);
    const query = join(
      [wrap(setFrag, `UPDATE ${quote(modelName)} SET `, ""), whereFrag],
      " WHERE ",
    );

    const res = await this.executor.run(query);
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

    const insertRow = toDbRow(model, cData);
    const cFields = Object.keys(insertRow);
    const updateRow = toDbRow(model, uData);
    const uFields = Object.keys(updateRow);
    const primaryKeyFields = getPrimaryKeyFields(model);

    const sqlFields = cFields.map((f) => quote(f)).join(", ");
    const sqlConflict = primaryKeyFields.map((f) => quote(f)).join(", ");

    let query: Fragment = {
      strings: [`INSERT INTO ${quote(modelName)} (${sqlFields}) VALUES (`],
      params: [],
    };
    for (let i = 0; i < cFields.length; i++) {
      if (i > 0) query.strings[query.strings.length - 1] += ", ";
      query.params.push(insertRow[cFields[i]!]);
      query.strings.push("");
    }
    query.strings[query.strings.length - 1] += `) ON CONFLICT (${sqlConflict}) `;

    if (uFields.length > 0) {
      const setParts: Fragment[] = [];
      for (let i = 0; i < uFields.length; i++) {
        const f = uFields[i]!;
        setParts.push({
          strings: [`${quote(f)} = `, ""],
          params: [updateRow[f]],
        });
      }
      const setFrag = join(setParts, ", ");
      query.strings[query.strings.length - 1] += "DO UPDATE SET ";
      query = join([query, setFrag], "");

      if (where) {
        const built = toWhere(model, where);
        query.strings[query.strings.length - 1] += " WHERE ";
        query = join([query, built], "");
      }
    } else {
      query.strings[query.strings.length - 1] += "DO NOTHING";
    }

    const sqlSelect = select
      ? select.map((s) => quote(s)).join(", ")
      : "*";
    query.strings[query.strings.length - 1] += ` RETURNING ${sqlSelect}`;

    const row = await this.executor.get(query);
    if (row !== undefined && row !== null) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
      return toRow<T>(model, row, select);
    }

    const existing = await this.find({
      model: modelName,
      where: buildPrimaryKeyFilter<T>(model, getPrimaryKeyValues(model, cData)),
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
    const built = toWhere(model, where);
    const query = wrap(built, `DELETE FROM ${quote(modelName)} WHERE `, "");
    await this.executor.run(query);
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName, where } = args;
    const model = this.schema[modelName]!;
    const built = toWhere(model, where);
    const query = wrap(built, `DELETE FROM ${quote(modelName)} WHERE `, "");
    const res = await this.executor.run(query);
    return res.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName, where } = args;
    const model = this.schema[modelName]!;
    const built = toWhere(model, where);
    const query = wrap(
      built,
      `SELECT COUNT(*) as count FROM ${quote(modelName)} WHERE `,
      "",
    );
    const row = await this.executor.get(query);
    const count = row?.["count"];
    return count === undefined || count === null ? 0 : Number(count);
  }
}
