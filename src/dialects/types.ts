import type { Field } from "../types";

export interface QueryExecutor {
  all(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
  get(sql: string, params?: unknown[]): Promise<Record<string, unknown> | undefined>;
  run(sql: string, params?: unknown[]): Promise<{ changes: number }>;
  transaction<T>(fn: (executor: QueryExecutor) => Promise<T>): Promise<T>;
}

export interface SqlDialect {
  placeholder(index: number): string;
  quote(identifier: string): string;
  escapeLiteral(value: string): string;
  mapFieldType(field: Field): string;
  buildJsonPath(path: string[]): string;
  buildJsonExtract(
    fieldName: string,
    path: (string | number)[],
    isNumeric?: boolean,
    isBoolean?: boolean,
  ): string;
  upsert?(options: {
    table: string;
    insertColumns: string[];
    insertPlaceholders: string[];
    updateColumns: string[];
    conflictColumns: string[];
    select?: readonly string[];
    whereSql?: string;
  }): { sql: string; params?: unknown[] };
}

export function isQueryExecutor(obj: unknown): obj is QueryExecutor {
  if (obj === null || typeof obj !== "object") return false;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
  const o = obj as Record<string, unknown>;
  return typeof o["all"] === "function" && typeof o["run"] === "function";
}
