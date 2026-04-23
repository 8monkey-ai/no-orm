import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";
import type postgres from "postgres";

import type { Adapter, Field, Schema } from "../types";
import { type QueryExecutor, type SqlDialect, SqlAdapter, isQueryExecutor } from "./sql";

type PostgresJsSql = postgres.Sql;
type TransactionSql = postgres.TransactionSql;

export type PostgresDriver = PgClient | PgPool | PgPoolClient | PostgresJsSql | TransactionSql;

// --- Prepared statement name cache for pg driver ---
// Reuses statement names per SQL string to benefit from server-side prepared statements.
const pgStatementCache = new Map<string, string>();
let queryCount = 0;

function getNamedQuery(sql: string): { name: string; text: string } {
  let name = pgStatementCache.get(sql);
  if (name === undefined) {
    name = `no_orm_${queryCount++}`;
    pgStatementCache.set(sql, name);
  }
  return { name, text: sql };
}

// --- Dialect ---

export const PostgresDialect: SqlDialect = {
  placeholder: (i) => `$${i + 1}`,
  quote: (s) => `"${s.replaceAll('"', '""')}"`,
  escapeLiteral: (s) => s.replaceAll("'", "''"),
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
  buildJsonPath(path: string[]): string {
    let res = "";
    for (let i = 0; i < path.length; i++) {
      if (i > 0) res += ", ";
      res += `'${this.escapeLiteral(path[i]!)}'`;
    }
    return res;
  },
  buildJsonExtract(
    column: string,
    path: string[],
    isNumeric?: boolean,
    isBoolean?: boolean,
  ): string {
    const segments = this.buildJsonPath(path);
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

/**
 * Bun SQL and postgres.js both use `unsafe()` for raw queries.
 * Both drivers accept arrays of primitives at runtime. We use a structural
 * approach via Record<string, unknown> to avoid driver-specific type gymnastics.
 */
// eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- structural duck-typing for multi-driver support
function createUnsafeExecutor(
  sql: Record<string, unknown>,
  beginFn: (cb: (tx: Record<string, unknown>) => Promise<unknown>) => Promise<unknown>,
): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- extracting unsafe() from structurally-typed driver
  const unsafeFn = sql["unsafe"] as (
    query: string,
    params?: unknown[],
  ) => Promise<Record<string, unknown>[] & { count?: number }>;

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
      beginFn((tx) => fn(createUnsafeExecutor(tx, beginFn))) as Promise<T>,
  };
}

function createPostgresJsExecutor(sql: PostgresJsSql): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- postgres.js Sql -> Record for createUnsafeExecutor
  const driver = sql as unknown as Record<string, unknown>;
  return createUnsafeExecutor(driver, (cb) =>
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- TransactionSql -> Record for createUnsafeExecutor
    sql.begin((tx) => cb(tx as unknown as Record<string, unknown>)),
  );
}

function createBunSqlExecutor(driver: Record<string, unknown>): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Bun SQL transaction function extraction
  const transactionFn = driver["transaction"] as (
    cb: (tx: unknown) => Promise<unknown>,
  ) => Promise<unknown>;
  return createUnsafeExecutor(driver, (cb) =>
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Bun SQL tx -> Record
    transactionFn((tx) => cb(tx as Record<string, unknown>)),
  );
}

function createPgExecutor(driver: PgClient | PgPool | PgPoolClient): QueryExecutor {
  return {
    all: async (sql, params) => {
      const res = await driver.query<Record<string, unknown>>({
        ...getNamedQuery(sql),
        values: params,
      });
      return res.rows;
    },
    get: async (sql, params) => {
      const res = await driver.query<Record<string, unknown>>({
        ...getNamedQuery(sql),
        values: params,
      });
      return res.rows[0];
    },
    run: async (sql, params) => {
      const res = await driver.query({ ...getNamedQuery(sql), values: params });
      return { changes: res.rowCount ?? 0 };
    },
    transaction: async (fn) => {
      // Pool has `connect()` but no `release()`; PoolClient has `release()`.
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
      // Already a Client or PoolClient — use directly
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

export class PostgresAdapter<S extends Schema = Schema>
  extends SqlAdapter<S>
  implements Adapter<S>
{
  constructor(schema: S, driver: PostgresDriver | QueryExecutor) {
    super(
      schema,
      isQueryExecutor(driver) ? driver : createPostgresExecutor(driver),
      PostgresDialect,
    );
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction((innerExecutor) => {
      return fn(new PostgresAdapter(this.schema, innerExecutor));
    });
  }
}
