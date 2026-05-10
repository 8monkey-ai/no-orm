import type { Cursor, Field, Model, SortBy, Where } from "../../types";
import { getPaginationFilter, walkWhere } from "./common";

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
    strings.raw = this.strings;
    return [strings as TemplateStringsArray, ...this.params];
  }

  /**
   * Compiles the Sql instance into a single string with placeholders.
   * Useful for drivers that expect a SQL string and a separate array of parameters.
   */
  compile(placeholder: string | ((index: number) => string)): string {
    const paramsLength = this.params.length;
    if (paramsLength === 0) return this.strings[0]!;

    if (typeof placeholder === "string") {
      return this.strings.join(placeholder);
    }

    let result = "";
    for (let i = 0; i < paramsLength; i++) {
      result += this.strings[i]! + placeholder(i);
    }
    result += this.strings[paramsLength]!;
    return result;
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
 * Safely quotes a SQL identifier or list of identifiers.
 * - id("users") -> "users"
 * - id(["id", "name"]) -> "id", "name"
 */
export function id(val: string | readonly string[], quoteChar = '"'): Sql {
  if (val === "" || (Array.isArray(val) && val.length === 0)) return raw("");

  if (typeof val === "string") {
    return raw(quoteChar + val + quoteChar);
  }

  let res = "";
  for (let i = 0; i < val.length; i++) {
    if (i > 0) res += ", ";
    res += quoteChar + val[i]! + quoteChar;
  }
  return raw(res);
}

/**
 * Generates a comma-separated list of placeholders for values.
 */
export function placeholders(values: unknown[]): Sql {
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
    "transaction" in obj &&
    typeof (obj as Record<string, unknown>)["all"] === "function" &&
    typeof (obj as Record<string, unknown>)["run"] === "function" &&
    typeof (obj as Record<string, unknown>)["transaction"] === "function"
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
  return walkWhere<Sql, T>(clause, {
    and: (children) => {
      const parts: Sql[] = [];
      for (let i = 0; i < children.length; i++) {
        parts.push(sql`(${children[i]!})`);
      }
      return join(parts, " AND ");
    },
    or: (children) => {
      const parts: Sql[] = [];
      for (let i = 0; i < children.length; i++) {
        parts.push(sql`(${children[i]!})`);
      }
      return join(parts, " OR ");
    },
    leaf: (c) => {
      const expr = options.columnExpr(options.model, c.field as string, c.path, c.value);
      const field = options.model.fields[c.field as string];
      const mapped = options.mapValue ? options.mapValue(c.value, field) : c.value;
      const op: string = c.op;

      switch (c.op) {
        case "eq":
          return c.value === null ? sql`${expr} IS NULL` : sql`${expr} = ${mapped}`;
        case "ne":
          return c.value === null ? sql`${expr} IS NOT NULL` : sql`${expr} != ${mapped}`;
        case "gt":
          return sql`${expr} > ${mapped}`;
        case "gte":
          return sql`${expr} >= ${mapped}`;
        case "lt":
          return sql`${expr} < ${mapped}`;
        case "lte":
          return sql`${expr} <= ${mapped}`;
        case "in": {
          if (c.value.length === 0) return sql`1=0`;
          let params: unknown[] = c.value;
          if (options.mapValue) {
            params = [];
            for (let i = 0; i < c.value.length; i++) {
              params.push(options.mapValue(c.value[i], field));
            }
          }
          return sql`${expr} IN (${placeholders(params)})`;
        }
        case "not_in": {
          if (c.value.length === 0) return sql`1=1`;
          let params: unknown[] = c.value;
          if (options.mapValue) {
            params = [];
            for (let i = 0; i < c.value.length; i++) {
              params.push(options.mapValue(c.value[i], field));
            }
          }
          return sql`${expr} NOT IN (${placeholders(params)})`;
        }
        default:
          throw new Error(`Unsupported operator: ${op}`);
      }
    },
  });
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
export function set(data: Record<string, unknown>, quoteChar = '"'): Sql {
  const fields = Object.keys(data);
  if (fields.length === 0) throw new Error("set() called with empty data");
  const parts: Sql[] = [];
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    parts.push(sql`${id(f, quoteChar)} = ${data[f]}`);
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
    // Pass a type-representative sentinel so columnExpr can apply the right SQL cast
    // (e.g. ::double precision for number/timestamp, ::boolean for boolean).
    const typeValue: unknown =
      s.type === "number" || s.type === "timestamp" ? 0 : s.type === "boolean" ? true : undefined;
    const expr = columnExpr(model, s.field as string, s.path, typeValue);
    const dir = (s.direction ?? "asc").toUpperCase();
    parts.push(sql`${expr} ${raw(dir)}`);
  }
  return join(parts, ", ");
}
