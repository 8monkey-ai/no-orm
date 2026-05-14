import type { Cursor, Field, Model, Schema, SortBy, Where } from "../../types";
import { getPaginationFilter, getPrimaryKeyFieldNames, walkWhere } from "./common";

export type Fragment = { text: string; params: unknown[] };

/**
 * Safely quotes a SQL identifier or comma-separated list of identifiers.
 */
export function id(val: string | readonly string[], quoteChar = '"'): Fragment {
  if (val === "" || (Array.isArray(val) && val.length === 0)) return { text: "", params: [] };

  if (typeof val === "string") {
    return { text: quoteChar + val + quoteChar, params: [] };
  }

  let text = "";
  for (let i = 0; i < val.length; i++) {
    if (i > 0) text += ", ";
    text += quoteChar + val[i]! + quoteChar;
  }
  return { text, params: [] };
}

/**
 * Generates a comma-separated list of ? placeholders for values.
 */
export function placeholders(values: unknown[]): Fragment {
  if (values.length === 0) return { text: "", params: [] };
  let text = "?";
  for (let i = 1; i < values.length; i++) text += ", ?";
  return { text, params: values };
}

/**
 * Concatenates multiple fragments with a separator, merging params in order.
 */
export function join(fragments: Fragment[], separator: string): Fragment {
  if (fragments.length === 0) return { text: "", params: [] };

  let text = fragments[0]!.text;
  const params: unknown[] = fragments[0]!.params.slice();

  for (let i = 1; i < fragments.length; i++) {
    const f = fragments[i]!;
    text += separator + f.text;
    for (let j = 0; j < f.params.length; j++) params.push(f.params[j]);
  }

  return { text, params };
}

/**
 * Converts ? placeholders to $1, $2, ... for pg-style drivers.
 * Uses split instead of regex for performance.
 */
export function toNumberedParams(f: Fragment): { text: string; values: unknown[] } {
  const parts = f.text.split("?");
  if (parts.length === 1) return { text: f.text, values: f.params };
  let text = parts[0]!;
  for (let i = 1; i < parts.length; i++) text += "$" + i + parts[i]!;
  return { text, values: f.params };
}

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
    typeof obj.all === "function" &&
    "get" in obj &&
    typeof obj.get === "function" &&
    "run" in obj &&
    typeof obj.run === "function" &&
    "transaction" in obj &&
    typeof obj.transaction === "function"
  );
}

export type ColumnExprFn = (
  model: Model,
  fieldName: string,
  path?: string[],
  value?: unknown,
) => Fragment;
export type MapValueFn = (val: unknown, field?: Field) => unknown;

export interface WhereOptions {
  model: Model;
  columnExpr: ColumnExprFn;
  mapValue?: MapValueFn;
}

function buildWhere<T>(clause: Where<T>, options: WhereOptions): Fragment {
  return walkWhere<Fragment, T>(clause, {
    and: (children) => {
      const parts: Fragment[] = [];
      for (let i = 0; i < children.length; i++) {
        parts.push({ text: `(${children[i]!.text})`, params: children[i]!.params });
      }
      return join(parts, " AND ");
    },
    or: (children) => {
      const parts: Fragment[] = [];
      for (let i = 0; i < children.length; i++) {
        parts.push({ text: `(${children[i]!.text})`, params: children[i]!.params });
      }
      return join(parts, " OR ");
    },
    leaf: (c) => {
      const expr = options.columnExpr(options.model, c.field as string, c.path, c.value);
      const field = options.model.fields[c.field as string];
      const mapped = options.mapValue ? options.mapValue(c.value, field) : c.value;

      switch (c.op) {
        case "eq": {
          if (c.value === null) return { text: `${expr.text} IS NULL`, params: expr.params };
          const params = expr.params.slice();
          params.push(mapped);
          return { text: `${expr.text} = ?`, params };
        }
        case "ne": {
          if (c.value === null) return { text: `${expr.text} IS NOT NULL`, params: expr.params };
          const params = expr.params.slice();
          params.push(mapped);
          return { text: `${expr.text} != ?`, params };
        }
        case "gt": {
          const params = expr.params.slice();
          params.push(mapped);
          return { text: `${expr.text} > ?`, params };
        }
        case "gte": {
          const params = expr.params.slice();
          params.push(mapped);
          return { text: `${expr.text} >= ?`, params };
        }
        case "lt": {
          const params = expr.params.slice();
          params.push(mapped);
          return { text: `${expr.text} < ?`, params };
        }
        case "lte": {
          const params = expr.params.slice();
          params.push(mapped);
          return { text: `${expr.text} <= ?`, params };
        }
        case "in": {
          if (c.value.length === 0) return { text: "1=0", params: [] };
          let vals: unknown[] = c.value;
          if (options.mapValue) {
            vals = [];
            for (let i = 0; i < c.value.length; i++) vals.push(options.mapValue(c.value[i], field));
          }
          const ph = placeholders(vals);
          const params = expr.params.slice();
          for (let i = 0; i < vals.length; i++) params.push(vals[i]);
          return { text: `${expr.text} IN (${ph.text})`, params };
        }
        case "not_in": {
          if (c.value.length === 0) return { text: "1=1", params: [] };
          let vals: unknown[] = c.value;
          if (options.mapValue) {
            vals = [];
            for (let i = 0; i < c.value.length; i++) vals.push(options.mapValue(c.value[i], field));
          }
          const ph = placeholders(vals);
          const params = expr.params.slice();
          for (let i = 0; i < vals.length; i++) params.push(vals[i]);
          return { text: `${expr.text} NOT IN (${ph.text})`, params };
        }
        default:
          throw new Error("Unsupported where operator");
      }
    },
  });
}

export function where<T>(
  clause: Where<T> | undefined,
  options: WhereOptions & { cursor?: Cursor<T>; sortBy?: SortBy<T>[] },
): Fragment {
  const parts: Fragment[] = [];

  if (clause) {
    const bw = buildWhere(clause, options);
    parts.push({ text: `(${bw.text})`, params: bw.params });
  }

  if (options.cursor) {
    const paginationWhere = getPaginationFilter(options.cursor, options.sortBy);
    if (paginationWhere) {
      const bw = buildWhere(paginationWhere, options);
      parts.push({ text: `(${bw.text})`, params: bw.params });
    }
  }

  return parts.length > 0 ? join(parts, " AND ") : { text: "1=1", params: [] };
}

export function set(
  data: Record<string, unknown>,
  mapValue?: (val: unknown) => unknown,
  quoteChar = '"',
): Fragment {
  const fields = Object.keys(data);
  const parts: Fragment[] = [];
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    const val = data[f];
    if (val === undefined) continue;
    const mapped = mapValue ? mapValue(val) : val;
    parts.push({ text: `${id(f, quoteChar).text} = ?`, params: [mapped] });
  }
  if (parts.length === 0) throw new Error("set() called with empty data");
  return join(parts, ", ");
}

export function sort<T>(model: Model, sortBy: SortBy<T>[], columnExpr: ColumnExprFn): Fragment {
  if (sortBy.length === 0) throw new Error("sort() called with empty sortBy");
  const parts: Fragment[] = [];
  for (let i = 0; i < sortBy.length; i++) {
    const s = sortBy[i]!;
    const typeValue: unknown =
      s.type === "number" || s.type === "timestamp" ? 0 : s.type === "boolean" ? true : undefined;
    const expr = columnExpr(model, s.field as string, s.path, typeValue);
    const dir = (s.direction ?? "asc").toUpperCase();
    parts.push({ text: `${expr.text} ${dir}`, params: expr.params });
  }
  return join(parts, ", ");
}

export function stringifyJsonParam(v: unknown): unknown {
  return v !== null && typeof v === "object" && !(v instanceof Date) && !(v instanceof Uint8Array)
    ? JSON.stringify(v)
    : v;
}

export function extractFields(
  data: Record<string, unknown>,
  mapValue?: (val: unknown) => unknown,
): { fields: string[]; values: unknown[] } {
  const keys = Object.keys(data);
  const fields: string[] = [];
  const values: unknown[] = [];
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = data[k];
    if (val === undefined) continue;
    fields.push(k);
    values.push(mapValue === undefined ? val : mapValue(val));
  }
  return { fields, values };
}

export function selectSql(opts: {
  table: string;
  select: readonly string[] | undefined;
  where: Fragment;
  orderBy?: Fragment;
  limit?: number;
  offset?: number;
}): Fragment {
  const colsText = opts.select && opts.select.length > 0 ? id(opts.select).text : "*";
  let text = `SELECT ${colsText} FROM ${id(opts.table).text} WHERE ${opts.where.text}`;
  const params = opts.where.params.slice();
  if (opts.orderBy !== undefined) {
    text += ` ORDER BY ${opts.orderBy.text}`;
    for (let i = 0; i < opts.orderBy.params.length; i++) params.push(opts.orderBy.params[i]);
  }
  if (opts.limit !== undefined) text += ` LIMIT ${opts.limit}`;
  if (opts.offset !== undefined) text += ` OFFSET ${opts.offset}`;
  return { text, params };
}

export function insertSql(opts: {
  table: string;
  fields: readonly string[];
  values: unknown[];
  returning: readonly string[] | undefined;
}): Fragment {
  const returningCols = opts.returning && opts.returning.length > 0 ? id(opts.returning).text : "*";
  const ph = placeholders(opts.values);
  return {
    text: `INSERT INTO ${id(opts.table).text} (${id(opts.fields).text}) VALUES (${ph.text}) RETURNING ${returningCols}`,
    params: opts.values,
  };
}

export function updateSql(opts: {
  table: string;
  set: Fragment;
  where: Fragment;
  returning?: boolean;
}): Fragment {
  const text = `UPDATE ${id(opts.table).text} SET ${opts.set.text} WHERE ${opts.where.text}${opts.returning ? " RETURNING *" : ""}`;
  const params = opts.set.params.slice();
  for (let i = 0; i < opts.where.params.length; i++) params.push(opts.where.params[i]);
  return { text, params };
}

export function deleteSql(opts: { table: string; where: Fragment }): Fragment {
  return {
    text: `DELETE FROM ${id(opts.table).text} WHERE ${opts.where.text}`,
    params: opts.where.params,
  };
}

export function upsertSql(opts: {
  table: string;
  fields: readonly string[];
  values: unknown[];
  conflictColumns: readonly string[];
  onConflict: Fragment;
  returning: readonly string[] | undefined;
}): Fragment {
  const returningCols = opts.returning && opts.returning.length > 0 ? id(opts.returning).text : "*";
  const ph = placeholders(opts.values);
  const params = ph.params.slice();
  for (let i = 0; i < opts.onConflict.params.length; i++) params.push(opts.onConflict.params[i]);
  return {
    text: `INSERT INTO ${id(opts.table).text} (${id(opts.fields).text}) VALUES (${ph.text}) ON CONFLICT (${id(opts.conflictColumns).text}) ${opts.onConflict.text} RETURNING ${returningCols}`,
    params,
  };
}

export function countSql(opts: { table: string; where: Fragment }): Fragment {
  return {
    text: `SELECT COUNT(*) as count FROM ${id(opts.table).text} WHERE ${opts.where.text}`,
    params: opts.where.params,
  };
}

export function migrateSqls(
  schema: Schema,
  opts: {
    sqlType: (field: Field) => string;
    quoteChar?: string;
    indexIfNotExists?: boolean;
  },
): Fragment[] {
  const q = opts.quoteChar ?? '"';
  const ifNotExists = opts.indexIfNotExists ?? true;
  const models = Object.entries(schema);
  const stmts: Fragment[] = [];

  for (let i = 0; i < models.length; i++) {
    const [name, model] = models[i]!;
    const fields = Object.entries(model.fields);
    const columnsList: Fragment[] = [];
    for (let j = 0; j < fields.length; j++) {
      const [fname, f] = fields[j]!;
      columnsList.push({
        text: `${id(fname, q).text} ${opts.sqlType(f)}${f.nullable === true ? "" : " NOT NULL"}`,
        params: [],
      });
    }
    const pkFields = getPrimaryKeyFieldNames(model);
    const pkText = `PRIMARY KEY (${id(pkFields, q).text})`;
    const colsText = join(columnsList, ", ").text;
    stmts.push({
      text: `CREATE TABLE IF NOT EXISTS ${id(name, q).text} (${colsText}, ${pkText})`,
      params: [],
    });
  }

  for (let i = 0; i < models.length; i++) {
    const [name, model] = models[i]!;
    if (!model.indexes) continue;
    for (let j = 0; j < model.indexes.length; j++) {
      const idx = model.indexes[j]!;
      const fields = Array.isArray(idx.field) ? idx.field : [idx.field];
      const formatted: Fragment[] = [];
      for (let k = 0; k < fields.length; k++) {
        const f = fields[k]!;
        formatted.push({
          text: `${id(f, q).text}${idx.order ? ` ${idx.order.toUpperCase()}` : ""}`,
          params: [],
        });
      }
      const ifne = ifNotExists ? "IF NOT EXISTS " : "";
      stmts.push({
        text: `CREATE INDEX ${ifne}${id(`idx_${name}_${j}`, q).text} ON ${id(name, q).text} (${join(formatted, ", ").text})`,
        params: [],
      });
    }
  }

  return stmts;
}
