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
  | BunSqlDriver;

export interface BunSqlDriver {
  unsafe<R = Record<string, unknown>[]>(query: string, params?: unknown[]): Promise<R>;
  transaction<T>(fn: (tx: BunSqlDriver) => Promise<T>): Promise<T>;
}

let queryCount = 0;
function getNamedQuery(sql: string) {
  const name = `no_orm_${queryCount++}`;
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
      const col = conflictColumns[i];
      if (col !== undefined) pk.push(this.quote(col));
    }

    let updateSet = "";
    if (updateColumns.length > 0) {
      const sets = [];
      for (let i = 0; i < updateColumns.length; i++) {
        const col = updateColumns[i]!;
        sets.push(`${this.quote(col)} = EXCLUDED.${this.quote(col)}`);
      }
      updateSet = `DO UPDATE SET ${sets.join(", ")}`;
      if (whereSql !== undefined) updateSet += ` WHERE ${whereSql}`;
    } else {
      updateSet = "DO NOTHING";
    }

    return {
      sql: `INSERT INTO ${this.quote(table)} (${insertColumns.join(", ")}) VALUES (${insertPlaceholders.join(", ")}) ON CONFLICT (${pk.join(", ")}) ${updateSet} RETURNING *`,
    };
  },
};

function isPostgresJs(driver: unknown): driver is PostgresJsSql {
  if (typeof driver !== "object" || driver === null) return false;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  const record = driver as Record<string, unknown>;
  return (
    "unsafe" in record &&
    "begin" in record &&
    typeof record["unsafe"] === "function" &&
    typeof record["begin"] === "function"
  );
}

function isBunSql(driver: unknown): driver is BunSqlDriver {
  if (typeof driver !== "object" || driver === null) return false;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  const record = driver as Record<string, unknown>;
  return (
    "unsafe" in record &&
    "transaction" in record &&
    typeof record["unsafe"] === "function" &&
    typeof record["transaction"] === "function"
  );
}

function isPg(driver: unknown): driver is PgClient | PgPool | PgPoolClient {
  if (typeof driver !== "object" || driver === null) return false;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  const record = driver as Record<string, unknown>;
  return "query" in record && typeof record["query"] === "function";
}

function createPostgresJsExecutor(sql: PostgresJsSql): QueryExecutor {
  return {
    all: (query, params) => sql.unsafe(query, params),
    get: async (query, params) => {
      const rows = await sql.unsafe(query, params);
      return rows[0];
    },
    run: async (query, params) => {
      const res = await sql.unsafe(query, params);
      const count = getPgQueryResultCount(res);
      return { changes: count };
    },
    transaction: (fn) => sql.begin((tx) => fn(createPostgresJsExecutor(tx))),
  };
}

function getPgQueryResultRows<T = unknown[]>(result: unknown): T {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  if (Array.isArray(result)) return result as T;
  if (typeof result === "object" && result !== null && "rows" in result) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
    return (result as Record<string, unknown>)["rows"] as T;
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  return [] as T;
}

function getPgQueryResultCount(result: unknown): number {
  if (typeof result === "object" && result !== null) {
    const possibleProps = ["count", "rowCount", "rowsAffected"];
    for (const prop of possibleProps) {
      if (prop in result) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        const val = (result as Record<string, unknown>)[prop];
        if (typeof val === "number") return val;
      }
    }
  }
  return 0;
}

function getBunQueryResultCount(result: unknown): number {
  if (typeof result === "object" && result !== null) {
    const possibleProps = ["changes", "rowCount", "rowsAffected", "affectedRows"];
    for (const prop of possibleProps) {
      if (prop in result) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        const val = (result as Record<string, unknown>)[prop];
        if (typeof val === "number") return val;
      }
    }
  }
  return 0;
}
function createBunExecutor(sql: BunSqlDriver): QueryExecutor {
  return {
    all: (query, params) => sql.unsafe(query, params),
    get: async (query, params) => {
      const rows = await sql.unsafe(query, params);
      return rows[0];
    },
    run: async (query, params) => {
      const res = await sql.unsafe(query, params);
      const count = getBunQueryResultCount(res);
      return { changes: count };
    },
    transaction: (fn) => sql.transaction((tx) => fn(createBunExecutor(tx))),
  };
}

function createPgExecutor(driver: PgClient | PgPool | PgPoolClient): QueryExecutor {
  return {
    all: async (sql, params) => {
      const query = getNamedQuery(sql);
      const res = await driver.query({ ...query, values: params });
      return getPgQueryResultRows(res);
    },
    get: async (sql, params) => {
      const query = getNamedQuery(sql);
      const res = await driver.query({ ...query, values: params });
      const rows = getPgQueryResultRows<Array<Record<string, unknown>>>(res);
      return rows[0];
    },
    run: async (sql, params) => {
      const query = getNamedQuery(sql);
      const res = await driver.query({ ...query, values: params });
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      const typed = res as unknown as Record<string, unknown>;
      const changes = typed["rowsAffected"] ?? typed["rowCount"] ?? 0;
      return { changes: typeof changes === "number" ? changes : 0 };
    },
    transaction: async (fn) => {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      const poolDriver = driver as unknown as Record<string, unknown>;
      const isPool = "release" in poolDriver && typeof poolDriver["release"] === "function";
      const client = isPool ? await driver.connect() : driver;
      try {
        await client.query("BEGIN");
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        const pgClient = isPool ? (client as PgPoolClient) : (client as PgClient);
        const res = await fn(createPgExecutor(pgClient));
        await client.query("COMMIT");
        return res;
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        if (isPool) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
          const poolClient = client as PgPoolClient;
          poolClient["release"]();
        }
      }
    },
  };
}

export function createPostgresExecutor(driver: PostgresDriver): QueryExecutor {
  if (isPostgresJs(driver)) return createPostgresJsExecutor(driver);
  if (isBunSql(driver)) return createBunExecutor(driver);
  if (isPg(driver)) return createPgExecutor(driver);
  throw new Error("Unsupported Postgres driver.");
}
