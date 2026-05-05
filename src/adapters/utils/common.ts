import type { Cursor, FieldName, Model, SortBy, Where } from "../../types";

export type WhereLeaf<T = Record<string, unknown>> = Extract<Where<T>, { field: unknown }>;

export interface WhereVisitor<R, T = Record<string, unknown>> {
  leaf: (node: WhereLeaf<T>) => R;
  and: (children: R[]) => R;
  or: (children: R[]) => R;
}

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

export function getPrimaryKeyFields(model: Model): string[] {
  return Array.isArray(model.primaryKey) ? model.primaryKey : [model.primaryKey];
}

/**
 * Extracts primary key values from a data object based on the model schema.
 */
export function getPrimaryKeyValues(
  model: Model,
  data: Record<string, unknown>,
): Record<string, unknown> {
  const primaryKeyFields = getPrimaryKeyFields(model);
  const values: Record<string, unknown> = {};
  for (let i = 0; i < primaryKeyFields.length; i++) {
    const field = primaryKeyFields[i]!;
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
  const primaryKeyFields = getPrimaryKeyFields(model);
  if (primaryKeyFields.length === 1) {
    const field = primaryKeyFields[0]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- field name from schema is guaranteed to be in T
    return { field: field as FieldName<T>, op: "eq" as const, value: source[field] };
  }

  const clauses: Where<T>[] = [];
  for (let i = 0; i < primaryKeyFields.length; i++) {
    const field = primaryKeyFields[i]!;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- field name from schema is guaranteed to be in T
    clauses.push({ field: field as FieldName<T>, op: "eq" as const, value: source[field] });
  }
  return { and: clauses };
}

export function assertNoPrimaryKeyUpdates(model: Model, data: Record<string, unknown>): void {
  const primaryKeyFields = getPrimaryKeyFields(model);
  for (let i = 0; i < primaryKeyFields.length; i++) {
    const field = primaryKeyFields[i]!;
    if (data[field] !== undefined) {
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
      if (typeof val !== "object" || val === null || Array.isArray(val)) return undefined;
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- val is checked to be an object and not null above
      val = (val as Record<string, unknown>)[path[i]!];
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

  const cursorValues = cursor.after as Record<string, unknown>;
  const orClauses: Where<T>[] = [];

  for (let i = 0; i < criteria.length; i++) {
    const andClauses: Where<T>[] = [];
    for (let j = 0; j < i; j++) {
      const prev = criteria[j]!;
      andClauses.push({
        // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- criteria field is guaranteed to be in T
        field: prev.field as FieldName<T>,
        path: prev.path,
        op: "eq",
        value: cursorValues[prev.field],
      });
    }
    const curr = criteria[i]!;
    andClauses.push({
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- criteria field is guaranteed to be in T
      field: curr.field as FieldName<T>,
      path: curr.path,
      op: curr.direction === "desc" ? "lt" : "gt",
      value: cursorValues[curr.field],
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
): { field: string; direction: "asc" | "desc"; path?: string[] }[] {
  const cursorValues = cursor.after as Record<string, unknown>;
  const criteria = [];
  if (sortBy !== undefined && sortBy.length > 0) {
    for (let i = 0; i < sortBy.length; i++) {
      const s = sortBy[i]!;
      if (cursorValues[s.field] !== undefined) {
        criteria.push({ field: s.field, direction: s.direction ?? "asc", path: s.path });
      }
    }
  } else {
    const keys = Object.keys(cursorValues);
    for (let i = 0; i < keys.length; i++) {
      criteria.push({ field: keys[i]!, direction: "asc" as const, path: undefined });
    }
  }
  return criteria;
}
