import type { Cursor, Field, Model, Select, SortBy, Where } from "../../types";
import { getPaginationFilter, mapNumeric } from "./common";

/**
 * A Sql instance keeps SQL logic and dynamic data separate to prevent injection.
 * It is structured to be compatible with TemplateStringsArray for safe driver calls.
 */
export class Sql {
  constructor(
    readonly strings: string[],
    readonly params: unknown[],
  ) {}
}

/**
 * Raw text to be included directly in SQL without parameterization.
 * Returns a Sql instance with no parameters.
 */
export const raw = (s: string) => new Sql([s], []);

/**
 * Tagged template literal for building SQL fragments safely.
 * Nesting Sql instances is supported.
 */
export function sql(strings: TemplateStringsArray, ...values: unknown[]): Sql {
  const outStrings = [strings[0]!];
  const outParams: unknown[] = [];

  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    const tail = strings[i + 1]!;

    if (v instanceof Sql) {
      outStrings[outStrings.length - 1] += v.strings[0]!;
      for (let j = 1; j < v.strings.length; j++) {
        outStrings.push(v.strings[j]!);
      }
      for (let j = 0; j < v.params.length; j++) {
        outParams.push(v.params[j]);
      }
      outStrings[outStrings.length - 1] += tail;
    } else {
      outParams.push(v);
      outStrings.push(tail);
    }
  }
  return new Sql(outStrings, outParams);
}

/**
 * Joins multiple identifiers with a separator.
 */
export function idList(names: readonly string[], quote: (s: string) => string): Sql {
  return new Sql([names.map((n) => quote(n)).join(", ")], []);
}

/**
 * Generates a comma-separated list of placeholders for values.
 */
export function paramList(values: unknown[]): Sql {
  if (values.length === 0) return new Sql([""], []);
  const strings: string[] = [""];
  for (let i = 1; i < values.length; i++) {
    strings.push(", ");
  }
  strings.push("");
  return new Sql(strings, values);
}

/** Shared contracts for SQL executors */
export interface QueryExecutor {
  all(query: Sql): Promise<Record<string, unknown>[]>;
  get(query: Sql): Promise<Record<string, unknown> | undefined | null>;
  run(query: Sql): Promise<{ changes: number }>;
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
  select?: Select<T>,
): T {
  const fields = model.fields;
  const res: Record<string, unknown> = {};
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select fields are strings
  const keys = (select as readonly string[]) ?? Object.keys(row);

  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = row[k];
    const field = fields[k];
    if (field === undefined || val === undefined || val === null) {
      res[k] = val;
      continue;
    }
    if (field.type === "json" || field.type === "json[]") {
      res[k] = typeof val === "string" ? JSON.parse(val) : val;
    } else if (field.type === "boolean") {
      // Postgres returns boolean, SQLite returns 1/0
      res[k] = val === true || val === 1;
    } else if (field.type === "number" || field.type === "timestamp") {
      res[k] = mapNumeric(val);
    } else {
      res[k] = val;
    }
  }
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
  return res as T;
}

/**
 * Prepares a data object for database insertion/update.
 * Handles JSON stringification and optional adapter-specific mapping.
 */
export function toDbRow(
  model: Model,
  data: Record<string, unknown>,
  mapValue?: (val: unknown, field: Field) => unknown,
): Record<string, unknown> {
  const fields = model.fields;
  const res: Record<string, unknown> = {};
  const keys = Object.keys(data);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = data[k];
    const field = fields[k];
    if (val === undefined) continue;

    if (val === null) {
      res[k] = null;
      continue;
    }

    if (field === undefined) {
      res[k] = val;
      continue;
    }

    let processed = val;
    if (field.type === "json" || field.type === "json[]") {
      processed = JSON.stringify(val);
    }

    res[k] = mapValue ? mapValue(processed, field) : processed;
  }
  return res;
}

/**
 * Concatenates multiple Sql fragments with a separator.
 */
export function join(fragments: Sql[], separator: string): Sql {
  if (fragments.length === 0) return new Sql([""], []);

  const strings = [...fragments[0]!.strings];
  const params = [...fragments[0]!.params];

  for (let i = 1; i < fragments.length; i++) {
    const f = fragments[i]!;
    strings[strings.length - 1] += separator + f.strings[0];
    for (let j = 1; j < f.strings.length; j++) {
      strings.push(f.strings[j]!);
    }
    for (let j = 0; j < f.params.length; j++) {
      params.push(f.params[j]);
    }
  }

  return new Sql(strings, params);
}

export type ColumnExprFn = (model: Model, fieldName: string, path?: string[], value?: unknown) => Sql;
export type MapValueFn = (val: unknown, field?: Field) => unknown;

export interface WhereOptions {
  model: Model;
  columnExpr: ColumnExprFn;
  mapValue?: MapValueFn;
}

function buildWhere<T>(clause: Where<T>, options: WhereOptions): Sql {
  const stack: { clause: Where<T>; processed: boolean }[] = [{ clause, processed: false }];
  const results: Sql[] = [];

  while (stack.length > 0) {
    // eslint-disable-next-line typescript-eslint/no-non-null-assertion -- stack length checked
    const item = stack.pop()!;
    const c = item.clause;

    // Handle logical composition (AND/OR)
    if ("and" in c || "or" in c) {
      const children = "and" in c ? c.and : c.or;

      if (item.processed) {
        // Second pass: All children have been processed and their results are in 'results' stack.
        // We pop them, wrap in parentheses, and join with the operator.
        const op = "and" in c ? " AND " : " OR ";
        const parts: Sql[] = [];
        for (let i = 0; i < children.length; i++) {
          // eslint-disable-next-line typescript-eslint/no-non-null-assertion -- results match children count
          parts.push(sql`(${results.pop()!})`);
        }
        // Parts were popped in reverse order, restore original order for deterministic SQL
        parts.reverse();
        results.push(join(parts, op));
      } else {
        // First pass: Push self back as 'processed', then push children to be processed.
        stack.push({ clause: c, processed: true });
        for (let i = children.length - 1; i >= 0; i--) {
          // eslint-disable-next-line typescript-eslint/no-non-null-assertion -- children is a valid array
          stack.push({ clause: children[i]!, processed: false });
        }
      }
      continue;
    }

    // Handle leaf nodes (individual field operations)
    const expr = options.columnExpr(options.model, c.field as string, c.path, c.value);
    const val = c.value;
    const field = options.model.fields[c.field as string];
    const mapped = options.mapValue ? options.mapValue(val, field) : val;

    let res: Sql;
    switch (c.op) {
      case "eq":
        res = val === null ? sql`${expr} IS NULL` : sql`${expr} = ${mapped}`;
        break;
      case "ne":
        res = val === null ? sql`${expr} IS NOT NULL` : sql`${expr} != ${mapped}`;
        break;
      case "gt":
        res = sql`${expr} > ${mapped}`;
        break;
      case "gte":
        res = sql`${expr} >= ${mapped}`;
        break;
      case "lt":
        res = sql`${expr} < ${mapped}`;
        break;
      case "lte":
        res = sql`${expr} <= ${mapped}`;
        break;
      case "in": {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val cast to unknown array for in operator
        const values = val as unknown[];
        if (!Array.isArray(values) || values.length === 0) {
          res = sql`1=0`;
        } else {
          const params = options.mapValue ? values.map((v) => options.mapValue!(v, field)) : values;
          res = sql`${expr} IN (${paramList(params)})`;
        }
        break;
      }
      case "not_in": {
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val cast to unknown array for not_in operator
        const values = val as unknown[];
        if (!Array.isArray(values) || values.length === 0) {
          res = sql`1=1`;
        } else {
          const params = options.mapValue ? values.map((v) => options.mapValue!(v, field)) : values;
          res = sql`${expr} NOT IN (${paramList(params)})`;
        }
        break;
      }
      default:
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- accessing op for error message
        throw new Error(`Unsupported operator: ${String((c as Record<string, unknown>)["op"])}`);
    }
    results.push(res);
  }

  // eslint-disable-next-line typescript-eslint/no-non-null-assertion -- final result is always present
  return results[0]!;
}

export function where<T>(
  clause: Where<T> | undefined,
  options: WhereOptions & { cursor?: Cursor<T>; sortBy?: SortBy<T>[] },
): Sql {
  const parts: Sql[] = [];

  if (clause) {
    parts.push(sql`(${buildWhere(clause, options)})`);
  }

  if (options.cursor) {
    const paginationWhere = getPaginationFilter(options.cursor, options.sortBy);
    if (paginationWhere) {
      parts.push(sql`(${buildWhere(paginationWhere, options)})`);
    }
  }

  return parts.length > 0 ? join(parts, " AND ") : sql`1=1`;
}

/**
 * Prepares a SET clause for UPDATE or UPSERT.
 */
export function set(data: Record<string, unknown>, quote: (s: string) => string): Sql {
  const fields = Object.keys(data);
  if (fields.length === 0) throw new Error("set() called with empty data");
  const parts: Sql[] = [];
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    parts.push(sql`${raw(quote(f))} = ${data[f]}`);
  }
  return join(parts, ", ");
}

/**
 * Prepares an ORDER BY clause.
 */
export function sort<T>(model: Model, sortBy: SortBy<T>[], columnExpr: ColumnExprFn): Sql {
  if (sortBy.length === 0) throw new Error("sort() called with empty sortBy");
  const parts: Sql[] = [];
  for (let i = 0; i < sortBy.length; i++) {
    const s = sortBy[i]!;
    const expr = columnExpr(model, s.field as string, s.path);
    const dir = (s.direction ?? "asc").toUpperCase();
    parts.push(sql`${expr} ${raw(dir)}`);
  }
  return join(parts, ", ");
}
