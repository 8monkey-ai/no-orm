import type { Cursor, FieldName, Model, SortBy, Where } from "../../types";

export type RowData = Record<string, unknown>;
export type Project<T, F extends FieldName<T>> = [F] extends [never] ? T : Pick<T, F>;

export type WhereLeaf<T = Record<string, unknown>> = Extract<Where<T>, { field: unknown }>;

export interface WhereVisitor<R, T = Record<string, unknown>> {
  leaf: (node: WhereLeaf<T>) => R;
  and: (children: R[]) => R;
  or: (children: R[]) => R;
}

type PaginationCriterion<T> = {
  field: FieldName<T>;
  direction: "asc" | "desc";
  path?: string[];
  value: unknown;
};

/**
 * Iterative fold over a Where AST. The traversal is backend-agnostic;
 * adapters supply per-node callbacks that produce their own result type
 * (Sql fragment, boolean, MongoDB filter object, etc.).
 */
export function walkWhere<R, T = Record<string, unknown>>(
  clause: Where<T>,
  visitor: WhereVisitor<R, T>,
): R {
  const stack: { clause: Where<T>; processed: boolean }[] = [{ clause, processed: false }];
  const results: R[] = [];

  while (stack.length > 0) {
    const item = stack.pop()!;
    const c = item.clause;

    if ("and" in c || "or" in c) {
      const children = "and" in c ? c.and : c.or;

      if (item.processed) {
        const parts: R[] = [];
        for (let i = 0; i < children.length; i++) parts.push(results.pop()!);
        parts.reverse();
        results.push("and" in c ? visitor.and(parts) : visitor.or(parts));
      } else {
        stack.push({ clause: c, processed: true });
        for (let i = children.length - 1; i >= 0; i--) {
          stack.push({ clause: children[i]!, processed: false });
        }
      }
      continue;
    }

    results.push(visitor.leaf(c));
  }

  return results[0]!;
}

// --- Schema & Logic Helpers ---

export function getPrimaryKeyFieldNames(model: Model): string[] {
  return Array.isArray(model.primaryKey) ? model.primaryKey : [model.primaryKey];
}

/**
 * Extracts primary key values from a data object based on the model schema.
 */
export function getPrimaryKeyValues(
  model: Model,
  data: Record<string, unknown>,
): Record<string, unknown> {
  const primaryKeyFieldNames = getPrimaryKeyFieldNames(model);
  const values: Record<string, unknown> = {};
  for (let i = 0; i < primaryKeyFieldNames.length; i++) {
    const field = primaryKeyFieldNames[i]!;
    if (!(field in data)) {
      throw new Error(`Missing primary key field: ${field}`);
    }
    values[field] = data[field];
  }
  return values;
}

/**
 * Builds a 'Where' filter targeting the primary key of a specific record.
 */
export function buildPrimaryKeyFilter<T = Record<string, unknown>>(
  model: Model,
  source: Record<string, unknown>,
): Where<T> {
  const primaryKeyFieldNames = getPrimaryKeyFieldNames(model);
  if (primaryKeyFieldNames.length === 1) {
    const field = primaryKeyFieldNames[0]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- field name from schema is guaranteed to be in T
    return { field: field as FieldName<T>, op: "eq" as const, value: source[field] };
  }

  const clauses: Where<T>[] = [];
  for (let i = 0; i < primaryKeyFieldNames.length; i++) {
    const field = primaryKeyFieldNames[i]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- field name from schema is guaranteed to be in T
    clauses.push({ field: field as FieldName<T>, op: "eq" as const, value: source[field] });
  }
  return { and: clauses };
}

export function assertNoPrimaryKeyUpdates(model: Model, data: Record<string, unknown>): void {
  const primaryKeyFieldNames = getPrimaryKeyFieldNames(model);
  for (let i = 0; i < primaryKeyFieldNames.length; i++) {
    const field = primaryKeyFieldNames[i]!;
    if (Object.prototype.hasOwnProperty.call(data, field)) {
      throw new Error("Primary key updates are not supported.");
    }
  }
}

/**
 * Maps database numeric values to JS numbers.
 */
export function mapNumeric(value: unknown): number | null {
  return value === null || value === undefined ? null : Number(value);
}

// --- Value & Comparison Helpers ---

function isRecord(val: unknown): val is Record<string, unknown> {
  return typeof val === "object" && val !== null;
}

/**
 * Extracts a value from a record, supporting nested JSON paths.
 */
export function getNestedValue(
  record: Record<string, unknown>,
  field: string,
  path?: string[],
): unknown {
  let val: unknown = record[field];
  if (path !== undefined && path.length > 0) {
    for (let i = 0; i < path.length; i++) {
      if (!isRecord(val)) return undefined;
      val = val[path[i]!];
    }
  }
  return val;
}

export function getPaginationFilter<T = Record<string, unknown>>(
  cursor: Cursor<T>,
  sortBy?: SortBy<T>[],
): Where<T> | undefined {
  const criteria = getPaginationCriteria(cursor, sortBy);
  if (criteria.length === 0) return undefined;

  const orClauses: Where<T>[] = [];

  for (let i = 0; i < criteria.length; i++) {
    const andClauses: Where<T>[] = [];
    for (let j = 0; j < i; j++) {
      const prev = criteria[j]!;
      andClauses.push({
        field: prev.field,
        path: prev.path,
        op: "eq",
        value: prev.value,
      });
    }
    const curr = criteria[i]!;
    andClauses.push({
      field: curr.field,
      path: curr.path,
      op: curr.direction === "desc" ? "lt" : "gt",
      value: curr.value,
    });
    orClauses.push({ and: andClauses });
  }

  return orClauses.length === 1 ? orClauses[0] : { or: orClauses };
}

/**
 * Normalizes pagination criteria from a cursor and optional sort parameters.
 */
export function getPaginationCriteria<T = Record<string, unknown>>(
  cursor: Cursor<T>,
  sortBy?: SortBy<T>[],
): PaginationCriterion<T>[] {
  const criteria: PaginationCriterion<T>[] = [];
  if (sortBy !== undefined && sortBy.length > 0) {
    for (let i = 0; i < sortBy.length; i++) {
      const s = sortBy[i]!;
      let found: (typeof cursor.after)[number] | undefined;
      for (let j = 0; j < cursor.after.length; j++) {
        const e = cursor.after[j]!;
        if (e.field === s.field && pathsEqual(e.path, s.path)) {
          found = e;
          break;
        }
      }
      if (found !== undefined) {
        criteria.push({
          field: s.field,
          direction: s.direction ?? "asc",
          path: s.path,
          value: found.value,
        });
      }
    }
  } else {
    for (let i = 0; i < cursor.after.length; i++) {
      const e = cursor.after[i]!;
      criteria.push({ field: e.field, direction: "asc", path: e.path, value: e.value });
    }
  }
  return criteria;
}

function pathsEqual(a: string[] | undefined, b: string[] | undefined): boolean {
  if (a === undefined && b === undefined) return true;
  if (a === undefined || b === undefined) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

export function fnv1aHash(s: string): string {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ (s.codePointAt(i) ?? 0), 16777619);
  return Math.abs(h).toString(16).padStart(8, "0");
}
