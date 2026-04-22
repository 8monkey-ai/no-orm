import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";
import type { Sql as PostgresJsSql, TransactionSql } from "postgres";

import type { Field } from "../types";
import type { QueryExecutor, SqlDialect } from "./types";

export type PostgresDriver =
  | PgClient
  | PgPool
  | PgPoolClient
  | PostgresJsSql
  | TransactionSql
  | any;

let LRU: any;
// @ts-expect-error
import("lru-cache")
  .then((m) => {
    LRU = m.LRUCache;
  })
  .catch(() => {});

const pgCacheMap = new WeakMap<any>();

function getPgCache(client: any) {
  let cache = pgCacheMap.get(client);
  if (!cache && LRU) {
    cache = new LRU({ max: 100 });
    pgCacheMap.set(client, cache);
  }
  return cache;
}

let queryCount = 0;
function getNamedQuery(sql: string, cache?: any) {
  if (!cache) return { text: sql };
  let name = cache.get(sql);
  if (!name) {
    name = `no_orm_${queryCount++}`;
    cache.set(sql, name);
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
    if (isNumeric) return `(${base})::double precision`;
    if (isBoolean) return `(${base})::boolean`;
    return base;
  },
  upsert(args) {
    const { table, insertColumns, insertPlaceholders, updateColumns, conflictColumns, whereSql } =
      args;
    const pk = [];
    for (let i = 0; i < conflictColumns.length; i++) pk.push(this.quote(conflictColumns[i]));

    let updateSet = "";
    if (updateColumns.length > 0) {
      const sets = [];
      for (let i = 0; i < updateColumns.length; i++) {
        const col = updateColumns[i]!;
        sets.push(`${this.quote(col)} = EXCLUDED.${this.quote(col)}`);
      }
      updateSet = `DO UPDATE SET ${sets.join(", ")}`;
      if (whereSql) updateSet += ` WHERE ${whereSql}`;
    } else {
      updateSet = "DO NOTHING";
    }

    return {
      sql: `INSERT INTO ${this.quote(table)} (${insertColumns.join(", ")}) VALUES (${insertPlaceholders.join(", ")}) ON CONFLICT (${pk.join(", ")}) ${updateSet} RETURNING *`,
    };
  },
};

function isPostgresJs(driver: any): driver is PostgresJsSql {
  return typeof driver.unsafe === "function" && typeof driver.begin === "function";
}

function isBunSql(driver: any): boolean {
  return typeof driver.unsafe === "function" && typeof driver.transaction === "function";
}

function isPg(driver: any): boolean {
  return typeof driver.query === "function";
}

function createPostgresJsExecutor(sql: PostgresJsSql): QueryExecutor {
  return {
    all: (query, params) => sql.unsafe(query, params, { prepare: true }),
    get: async (query, params) => {
      const rows = await sql.unsafe(query, params, { prepare: true });
      return rows[0];
    },
    run: async (query, params) => {
      const res = await sql.unsafe(query, params, { prepare: true });
      return { changes: (res as any).count ?? 0 };
    },
    transaction: (fn) => sql.begin((tx) => fn(createPostgresJsExecutor(tx))),
  };
}

function createBunExecutor(sql: any): QueryExecutor {
  return {
    all: (query, params) => sql.unsafe(query, params),
    get: async (query, params) => {
      const rows = await sql.unsafe(query, params);
      return rows[0];
    },
    run: async (query, params) => {
      const res = await sql.unsafe(query, params);
      return { changes: res.count ?? res.affectedRows ?? 0 };
    },
    transaction: (fn) => sql.transaction((tx: any) => fn(createBunExecutor(tx))),
  };
}

function createPgExecutor(driver: any): QueryExecutor {
  const cache = getPgCache(driver);
  const executor: QueryExecutor = {
    all: async (sql, params) => {
      const query = getNamedQuery(sql, cache);
      const res = await driver.query({ ...query, values: params });
      return res.rows;
    },
    get: async (sql, params) => {
      const query = getNamedQuery(sql, cache);
      const res = await driver.query({ ...query, values: params });
      return res.rows[0];
    },
    run: async (sql, params) => {
      const query = getNamedQuery(sql, cache);
      const res = await driver.query({ ...query, values: params });
      return { changes: res.rowCount ?? 0 };
    },
    transaction: async (fn) => {
      const isPool = typeof driver.connect === "function";
      const client = isPool ? await driver.connect() : driver;
      try {
        await client.query("BEGIN");
        const res = await fn(createPgExecutor(client));
        await client.query("COMMIT");
        return res;
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        if (isPool) client.release();
      }
    },
  };
  return executor;
}

export function createPostgresExecutor(driver: PostgresDriver): QueryExecutor {
  if (isPostgresJs(driver)) return createPostgresJsExecutor(driver);
  if (isBunSql(driver)) return createBunExecutor(driver);
  if (isPg(driver)) return createPgExecutor(driver);
  throw new Error("Unsupported Postgres driver.");
}
