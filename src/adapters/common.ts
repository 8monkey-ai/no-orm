import type { Model, Where } from "../types";

// --- Schema & Logic Helpers ---

export function getPrimaryKeyFields(model: Model): string[] {
  return Array.isArray(model.primaryKey) ? model.primaryKey : [model.primaryKey];
}

/**
 * Extracts primary key values from a data object based on the model schema.
 */
export function getIdentityValues(
  model: Model,
  data: Record<string, unknown>,
): Record<string, unknown> {
  const pkFields = getPrimaryKeyFields(model);
  const values: Record<string, unknown> = {};
  for (let i = 0; i < pkFields.length; i++) {
    const field = pkFields[i]!;
    if (!(field in data)) {
      throw new Error(`Missing primary key field: ${field}`);
    }
    values[field] = data[field];
  }
  return values;
}

/**
 * Builds a 'Where' filter targeting the primary key of a specific record.
 * Returns Where<Record<string, unknown>> — callers cast to Where<T> at the boundary.
 */
export function buildIdentityFilter(model: Model, source: Record<string, unknown>): Where {
  const pkFields = getPrimaryKeyFields(model);
  if (pkFields.length === 1) {
    const field = pkFields[0]!;
    return { field, op: "eq" as const, value: source[field] };
  }

  const clauses: Where[] = [];
  for (let i = 0; i < pkFields.length; i++) {
    const field = pkFields[i]!;
    clauses.push({ field, op: "eq" as const, value: source[field] });
  }
  return { and: clauses };
}

export function assertNoPrimaryKeyUpdates(model: Model, data: Record<string, unknown>): void {
  const pkFields = getPrimaryKeyFields(model);
  for (let i = 0; i < pkFields.length; i++) {
    const field = pkFields[i]!;
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
