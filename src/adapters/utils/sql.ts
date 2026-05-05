import type { Cursor, Field, Model, SortBy, Where } from "../../types";
import { getPaginationFilter } from "./common";

/**
 * A Sql instance keeps SQL logic and dynamic data separate to prevent injection.
 * It is structured to be compatible with TemplateStringsArray for safe driver calls.
 */
export class Sql {
  constructor(
    readonly strings: string[],
    readonly params: unknown[],
  ) {}

  /**
   * Augments the strings array with a .raw property for drivers that expect TemplateStringsArray.
   * This is required for safe driver calls that use tagged templates (e.g. postgres.js, Bun SQL).
   */
  toTaggedArgs(): [TemplateStringsArray, ...unknown[]] {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- augmenting array to satisfy TemplateStringsArray contract
    const strings = this.strings as string[] & { raw: readonly string[] };
    // eslint-disable-next-line typescript-eslint/no-unnecessary-condition -- raw may be missing if not already augmented
    strings.raw ??= this.strings;
    return [strings as TemplateStringsArray, ...this.params];
  }
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
export function idList(names: readonly string[], quoteChar: string = '"'): Sql {
  return new Sql([names.map((n) => `${quoteChar}${n}${quoteChar}`).join(", ")], []);
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
 * Concatenates multiple Sql fragments with a separator.
 */
export function join(fragments: Sql[], separator: string): Sql {
  if (fragments.length === 0) return new Sql([""], []);

  const strings = fragments[0]!.strings.slice();
  const params = fragments[0]!.params.slice();

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

export type ColumnExprFn = (
  model: Model,
  fieldName: string,
  path?: string[],
  value?: unknown,
) => Sql;
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
          parts.push(sql`(${results.pop()!})`);
        }
        // Parts were popped in reverse order, restore original order for deterministic SQL
        parts.reverse();
        results.push(join(parts, op));
      } else {
        // First pass: Push self back as 'processed', then push children to be processed.
        stack.push({ clause: c, processed: true });
        for (let i = children.length - 1; i >= 0; i--) {
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
        const values = c.value;
        if (values.length === 0) {
          res = sql`1=0`;
        } else {
          const params = options.mapValue ? values.map((v) => options.mapValue!(v, field)) : values;
          res = sql`${expr} IN (${paramList(params)})`;
        }
        break;
      }
      case "not_in": {
        const values = c.value;
        if (values.length === 0) {
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
export function set(data: Record<string, unknown>, quoteChar: string = '"'): Sql {
  const fields = Object.keys(data);
  if (fields.length === 0) throw new Error("set() called with empty data");
  const parts: Sql[] = [];
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    parts.push(sql`${raw(`${quoteChar}${f}${quoteChar}`)} = ${data[f]}`);
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
