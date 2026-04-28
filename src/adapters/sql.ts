import type { Model, Select } from "../types";
import { mapNumeric } from "./common";

/** Shared contracts for SQL executors */

export interface QueryExecutor {
  all(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
  get(sql: string, params?: unknown[]): Promise<Record<string, unknown> | undefined | null>;
  run(sql: string, params?: unknown[]): Promise<{ changes: number }>;
  transaction<T>(fn: (executor: QueryExecutor) => Promise<T>): Promise<T>;
  readonly inTransaction: boolean;
}

export function isQueryExecutor(obj: unknown): obj is QueryExecutor {
  if (typeof obj !== "object" || obj === null) return false;
  return (
    "all" in obj &&
    "run" in obj &&
    typeof (obj as Record<string, unknown>)["all"] === "function" &&
    typeof (obj as Record<string, unknown>)["run"] === "function"
  );
}

/**
 * Maps a raw database row to the inferred model type T.
 * Handles JSON parsing, boolean conversion, and numeric mapping.
 */
export function toRow<T extends Record<string, unknown>>(
  model: Model,
  row: Record<string, unknown>,
  select?: Select<Record<string, unknown>>,
): T {
  const fields = model.fields;
  const res: Record<string, unknown> = {};
  const keys = select ?? Object.keys(row);

  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = row[k];
    const spec = fields[k];
    if (spec === undefined || val === undefined || val === null) {
      res[k] = val;
      continue;
    }
    if (spec.type === "json" || spec.type === "json[]") {
      res[k] = typeof val === "string" ? JSON.parse(val) : val;
    } else if (spec.type === "boolean") {
      res[k] = val === true || val === 1;
    } else if (spec.type === "number" || spec.type === "timestamp") {
      res[k] = mapNumeric(val);
    } else {
      res[k] = val;
    }
  }
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
  return res as T;
}

/**
 * FUTURE EXTENSION: GreptimeDB & MySQL
 *
 * Lessons learned from hebo-gateway and typical SQL quirks:
 *
 * 1. GreptimeDB:
 *    - Supports multiple protocols (Postgres, MySQL). When using Postgres wire protocol
 *      (via `PostgresAdapter`), use double quotes (") for identifiers. When using MySQL
 *      protocol, use backticks (`).
 *    - Mutation responses over Bun.SQL might not populate `count` but provide a `command` string
 *      like "OK 1". Parsed in `postgres.ts`.
 *    - JSON strings can contain Rust-style Unicode escapes (\u{xxxx}) which are invalid JSON.
 *      Empty JSON strings ("") or "{}" should be normalized to {}. To avoid driver crashes,
 *      JSON columns might need to be cast to STRING on the wire (e.g. `col::STRING`).
 *    - DDL: `TIME INDEX` is mandatory. May also need `PARTITION BY`, `WITH` (e.g. merge_mode),
 *      or `SKIPPING INDEX` for performance optimizations.
 *    - JSON: Specialized functions like `json_get_string` might be required instead of
 *      standard Postgres operators like `->>`.
 *
 * 2. MySQL / MariaDB:
 *    - Quoting uses backticks (`) instead of double quotes ("). Note that backticks
 *      within identifiers might need to be escaped by doubling them (``).
 *    - Upsert uses `ON DUPLICATE KEY UPDATE` instead of `ON CONFLICT`.
 *    - Does not support `CREATE INDEX IF NOT EXISTS`. Must be handled via try/catch in `migrate`.
 *    - Does not support `RETURNING`. `create` and `upsert` must fall back to a second `find` call.
 *
 * 3. General SQL:
 *    - Some databases don't support parameters in `LIMIT` clauses. May need a `limitAsLiteral` flag.
 *    - Indexing: Different types like `BRIN` might not support `ASC`/`DESC` or require `USING` syntax.
 */
