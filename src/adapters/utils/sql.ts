import type { Cursor, Field, Model, Select, SortBy, Where } from "../../types";
import { getPaginationFilter, mapNumeric } from "./common";

/**
 * A Fragment keeps SQL logic and dynamic data separate to prevent injection.
 * It is structured to be compatible with TemplateStringsArray for safe driver calls.
 */
export interface Fragment {
  strings: string[];
  params: unknown[];
}

/** Shared contracts for SQL executors */
export interface QueryExecutor {
  all(query: Fragment): Promise<Record<string, unknown>[]>;
  get(query: Fragment): Promise<Record<string, unknown> | undefined | null>;
  run(query: Fragment): Promise<{ changes: number }>;
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
    const spec = fields[k];
    if (spec === undefined || val === undefined || val === null) {
      res[k] = val;
      continue;
    }
    if (spec.type === "json" || spec.type === "json[]") {
      res[k] = typeof val === "string" ? JSON.parse(val) : val;
    } else if (spec.type === "boolean") {
      // Postgres returns boolean, SQLite returns 1/0
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
    const spec = fields[k];
    if (val === undefined) continue;

    if (val === null) {
      res[k] = null;
      continue;
    }

    if (spec === undefined) {
      res[k] = val;
      continue;
    }

    let processed = val;
    if (spec.type === "json" || spec.type === "json[]") {
      processed = JSON.stringify(val);
    }

    res[k] = mapValue ? mapValue(processed, spec) : processed;
  }
  return res;
}

/**
 * Concatenates multiple fragments with a separator.
 */
export function join(fragments: Fragment[], separator: string): Fragment {
  if (fragments.length === 0) return { strings: [""], params: [] };

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

  return { strings, params };
}

/**
 * Wraps a fragment with a prefix and suffix.
 */
export function wrap(fragment: Fragment, prefix: string, suffix: string): Fragment {
  const strings = [...fragment.strings];
  strings[0] = prefix + strings[0]!;
  strings[strings.length - 1] += suffix;
  return { strings, params: [...fragment.params] };
}

export type ColumnExprFn = (fieldName: string, path?: string[], value?: unknown) => Fragment;
export type MapValueFn = (val: unknown, field?: Field) => unknown;

export interface BuildOptions {
  model: Model;
  columnExpr: ColumnExprFn;
  mapValue?: MapValueFn;
}

function whereRecursive<T>(clause: Where<T>, options: BuildOptions): Fragment {
  if ("and" in clause) {
    const parts: Fragment[] = [];
    for (let i = 0; i < clause.and.length; i++) {
      parts.push(wrap(whereRecursive(clause.and[i]!, options), "(", ")"));
    }
    return join(parts, " AND ");
  }

  if ("or" in clause) {
    const parts: Fragment[] = [];
    for (let i = 0; i < clause.or.length; i++) {
      parts.push(wrap(whereRecursive(clause.or[i]!, options), "(", ")"));
    }
    return join(parts, " OR ");
  }

  const expr = options.columnExpr(clause.field as string, clause.path, clause.value);
  const val = clause.value;
  const field = options.model.fields[clause.field as string];
  const mapped = options.mapValue ? options.mapValue(val, field) : val;

  switch (clause.op) {
    case "eq":
      if (val === null) return wrap(expr, "", " IS NULL");
      return join([expr, { strings: [" = ", ""], params: [mapped] }], "");
    case "ne":
      if (val === null) return wrap(expr, "", " IS NOT NULL");
      return join([expr, { strings: [" != ", ""], params: [mapped] }], "");
    case "gt":
      return join([expr, { strings: [" > ", ""], params: [mapped] }], "");
    case "gte":
      return join([expr, { strings: [" >= ", ""], params: [mapped] }], "");
    case "lt":
      return join([expr, { strings: [" < ", ""], params: [mapped] }], "");
    case "lte":
      return join([expr, { strings: [" <= ", ""], params: [mapped] }], "");
    case "in": {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val cast to unknown array for in operator
      const vArr = val as unknown[];
      if (!Array.isArray(vArr) || vArr.length === 0) return { strings: ["1=0"], params: [] };
      const params = options.mapValue ? vArr.map((v) => options.mapValue!(v, field)) : vArr;
      const inFrag: Fragment = {
        // eslint-disable-next-line unicorn/no-new-array -- creating array of specific length for placeholders
        strings: [" IN (", ...new Array<string>(vArr.length - 1).fill(", "), ")"],
        params,
      };
      return join([expr, inFrag], "");
    }
    case "not_in": {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val cast to unknown array for not_in operator
      const vArr = val as unknown[];
      if (!Array.isArray(vArr) || vArr.length === 0) return { strings: ["1=1"], params: [] };
      const params = options.mapValue ? vArr.map((v) => options.mapValue!(v, field)) : vArr;
      const inFrag: Fragment = {
        // eslint-disable-next-line unicorn/no-new-array -- creating array of specific length for placeholders
        strings: [" NOT IN (", ...new Array<string>(vArr.length - 1).fill(", "), ")"],
        params,
      };
      return join([expr, inFrag], "");
    }
    default:
      throw new Error(`Unsupported operator: ${String((clause as Record<string, unknown>)["op"])}`);
  }
}

export function where<T>(
  clause: Where<T> | undefined,
  options: BuildOptions & { cursor?: Cursor<T>; sortBy?: SortBy<T>[] },
): Fragment {
  const parts: Fragment[] = [];

  if (clause) {
    parts.push(wrap(whereRecursive(clause, options), "(", ")"));
  }

  if (options.cursor) {
    const paginationWhere = getPaginationFilter(options.cursor, options.sortBy);
    if (paginationWhere) {
      parts.push(wrap(whereRecursive(paginationWhere, options), "(", ")"));
    }
  }

  return parts.length > 0 ? join(parts, " AND ") : { strings: ["1=1"], params: [] };
}

/**
 * Prepares a SET clause for UPDATE or UPSERT.
 */
export function set(data: Record<string, unknown>, quote: (s: string) => string): Fragment {
  const fields = Object.keys(data);
  if (fields.length === 0) throw new Error("set() called with empty data");
  const parts: Fragment[] = [];
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    parts.push({
      strings: [`${quote(f)} = `, ""],
      params: [data[f]],
    });
  }
  return join(parts, ", ");
}

/**
 * Prepares an ORDER BY clause.
 */
export function sort<T>(sortBy: SortBy<T>[], columnExpr: ColumnExprFn): Fragment {
  if (sortBy.length === 0) throw new Error("sort() called with empty sortBy");
  const parts: Fragment[] = [];
  for (let i = 0; i < sortBy.length; i++) {
    const s = sortBy[i]!;
    const expr = columnExpr(s.field as string, s.path);
    const dir = (s.direction ?? "asc").toUpperCase();
    parts.push(wrap(expr, "", ` ${dir}`));
  }
  return join(parts, ", ");
}
