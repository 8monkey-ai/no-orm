import type { Model, Where } from "../types";

// --- Type Guards ---

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function isNonEmptyString(v: unknown): v is string {
  return typeof v === "string" && v !== "";
}

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

export function validateJsonPath(path: string[]): string[] {
  for (let i = 0; i < path.length; i++) {
    const segment = path[i]!;
    for (let j = 0; j < segment.length; j++) {
      const c = segment.codePointAt(j);
      if (c === undefined) {
        throw new Error(`Invalid JSON path segment: ${segment}`);
      }
      const isAlpha = (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
      const isDigit = c >= 48 && c <= 57;
      const isUnderscore = c === 95;
      if (!isAlpha && !isDigit && !isUnderscore) {
        throw new Error(`Invalid JSON path segment: ${segment}`);
      }
    }
  }
  return path;
}

/**
 * Builds a 'Where' filter targeting the primary key of a specific record.
 * Returns Where<Record<string, unknown>> — callers cast to Where<T> at the boundary.
 */
export function buildIdentityFilter(
  model: Model,
  source: Record<string, unknown>,
): Where {
  const pkFields = getPrimaryKeyFields(model);
  if (pkFields.length === 0) {
    throw new Error("Model has no primary key defined.");
  }
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

export function assertNoPrimaryKeyUpdates(
  model: Model,
  data: Record<string, unknown>,
): void {
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
