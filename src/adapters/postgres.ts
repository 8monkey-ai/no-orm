import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";
import type postgres from "postgres";

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

type PostgresJsSql = postgres.Sql;
type TransactionSql = postgres.TransactionSql;

export type PostgresDriver = PgClient | PgPool | PgPoolClient | PostgresJsSql | TransactionSql;

const MAX_CACHE_SIZE = 100;

// --- Formatting Hooks ---

const pg: SqlFormat = {
  placeholder: (i) => `$${i + 1}`,
  quote: (s) => `"${s.replaceAll('"', '""')}"`,
  mapFieldType(field: Field): string {
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
  },
  jsonExtract(column: string, path: string[], isNumeric?: boolean, isBoolean?: boolean): string {
    let segments = "";
    for (let i = 0; i < path.length; i++) {
      if (i > 0) segments += ", ";
      segments += `'${path[i]!.replaceAll("'", "''")}'`;
    }
    const base = `jsonb_extract_path_text(${column}, ${segments})`;
    if (isNumeric === true) return `(${base})::double precision`;
    if (isBoolean === true) return `(${base})::boolean`;
    return base;
  },
  upsert(args) {
    const { table, insertColumns, insertPlaceholders, updateColumns, conflictColumns, whereSql } =
      args;
    const pk: string[] = [];
    for (let i = 0; i < conflictColumns.length; i++) {
      pk.push(this.quote(conflictColumns[i]!));
    }

    let updateSet = "";
    if (updateColumns.length > 0) {
      const sets = [];
      for (let i = 0; i < updateColumns.length; i++) {
        const col = updateColumns[i]!;
        sets.push(`${this.quote(col)} = EXCLUDED.${this.quote(col)}`);
      }
      updateSet = `DO UPDATE SET ${sets.join(", ")}`;
      if (whereSql !== undefined && whereSql !== "") updateSet += ` WHERE ${whereSql}`;
    } else {
      updateSet = "DO NOTHING";
    }

    return {
      sql: `INSERT INTO ${this.quote(table)} (${insertColumns.join(", ")}) VALUES (${insertPlaceholders.join(", ")}) ON CONFLICT (${pk.join(", ")}) ${updateSet} RETURNING *`,
    };
  },
};

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
function createPostgresJsExecutor(sql: postgres.Sql | postgres.TransactionSql): QueryExecutor {
  const run = (query: string, params?: unknown[]) =>
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- postgres.js params type is narrower than unknown[]
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
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- UnwrapPromiseArray<T> = T for single promises
        return sql.begin((tx) => fn(createPostgresJsExecutor(tx))) as Promise<T>;
      }
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- same as above
      return sql.savepoint((tx) => fn(createPostgresJsExecutor(tx))) as Promise<T>;
    },
  };
}

// Bun SQL: uses `sql.unsafe(query, params)`. No prepare option — the driver
// manages prepared statements internally.
function createBunSqlExecutor(driver: Record<string, unknown>): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- extracting unsafe() from Bun SQL driver
  const unsafeFn = driver["unsafe"] as (
    query: string,
    params?: unknown[],
  ) => Promise<Record<string, unknown>[] & { count?: number }>;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- extracting transaction() from Bun SQL driver
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
      return { changes: rows.count ?? 0 };
    },
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) =>
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Promise<unknown> -> Promise<T> at executor boundary
      transactionFn((tx) => fn(createBunSqlExecutor(tx as Record<string, unknown>))) as Promise<T>,
  };
}

// pg: uses named queries with a bounded LRU cache for server-side prepared
// statement reuse. Each unique SQL string gets a stable name (e.g. `q_0`).
function createPgExecutor(driver: PgClient | PgPool | PgPoolClient): QueryExecutor {
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
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- narrowed by isPool check above
        const client = await (driver as PgPool).connect();
        try {
          await client.query("BEGIN");
          const res = await fn(createPgExecutor(client));
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
        const res = await fn(createPgExecutor(driver));
        await driver.query("COMMIT");
        return res;
      } catch (e) {
        await driver.query("ROLLBACK");
        throw e;
      }
    },
  };
}

function createPostgresExecutor(driver: PostgresDriver): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- structural duck-typing for Bun SQL
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

  migrate = () => migrate(this.executor, this.schema, pg);

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction((exec) => fn(new PostgresAdapter(this.schema, exec)));
  }

  create = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }) => create(this.executor, args.model, this.schema[args.model]!, pg, args);

  find = <K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }) => find(this.executor, args.model, this.schema[args.model]!, pg, args);

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
  }) => findMany(this.executor, args.model, this.schema[args.model]!, pg, args);

  update = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }) => update(this.executor, args.model, this.schema[args.model]!, pg, args);

  updateMany = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
    data: Partial<T>;
  }) => updateMany(this.executor, args.model, this.schema[args.model]!, pg, args);

  upsert = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }) => upsert(this.executor, args.model, this.schema[args.model]!, pg, args);

  delete = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where: Where<T>;
  }) => remove(this.executor, args.model, this.schema[args.model]!, pg, args);

  deleteMany = <
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
  }) => removeMany(this.executor, args.model, this.schema[args.model]!, pg, args);

  count = <K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }) => count(this.executor, args.model, this.schema[args.model]!, pg, args);
}
