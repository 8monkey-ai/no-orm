import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";
import type postgres from "postgres";

import type { Field } from "../types";
import type { QueryExecutor, SqlDialect } from "./types";

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
// postgres.js has both `unsafe` and `begin`; Bun SQL has `unsafe` and `transaction` but no `begin`.
// pg has `query`.

function isPostgresJs(driver: PostgresDriver): driver is PostgresJsSql {
  return "unsafe" in driver && "begin" in driver;
}

function isPg(driver: PostgresDriver): driver is PgClient | PgPool | PgPoolClient {
  return "query" in driver;
}

// --- Executor factories ---

function createPostgresJsExecutor(sql: PostgresJsSql): QueryExecutor {
  // postgres.js `unsafe()` accepts `ParameterOrJSON[]` but our executor uses `unknown[]`.
  // The cast is safe: postgres.js serializes primitive values internally.
  const run = (query: string, params?: unknown[]) =>
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- postgres.js handles unknown params at runtime
    sql.unsafe<Record<string, unknown>[]>(query, params as postgres.ParameterOrJSON<never>[]);

  return {
    all: (query, params) => run(query, params),
    get: async (query, params) => {
      const rows = await run(query, params);
      return rows[0];
    },
    run: async (query, params) => {
      const rows = await run(query, params);
      return { changes: rows.count ?? 0 };
    },
    // postgres.js `begin` returns `Promise<UnwrapPromiseArray<T>>` which equals `T`
    // when `fn` returns `Promise<T>` (single promise, not tuple). Cast is safe.
    transaction: <T>(fn: (executor: QueryExecutor) => Promise<T>) =>
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- UnwrapPromiseArray<T> = T for single promises
      sql.begin((tx) => fn(createPostgresJsExecutor(tx))) as Promise<T>,
  };
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

export function createPostgresExecutor(driver: PostgresDriver): QueryExecutor {
  if (isPostgresJs(driver)) return createPostgresJsExecutor(driver);
  if (isPg(driver)) return createPgExecutor(driver);
  throw new Error("Unsupported Postgres driver.");
}
