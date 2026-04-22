import type { FieldName, Model, Where } from "../types";

// --- Type Guards ---

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function isNonEmptyString(v: unknown): v is string {
  return typeof v === "string" && v !== "";
}

// --- SQL Helpers ---

export function quote(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

export function escapeLiteral(val: string): string {
  return val.replaceAll("'", "''");
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
    const val = data[field];
    if (val === undefined) {
      throw new Error(`Missing primary key field: ${field}`);
    }
    values[field] = val;
  }
  return values;
}

export function validateJsonPath(path: string[]): string[] {
  for (let i = 0; i < path.length; i++) {
    const segment = path[i]!;
    // Faster validation without regex
    for (let j = 0; j < segment.length; j++) {
      const c = segment.codePointAt(j);
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
 */
export function buildIdentityFilter<T extends Record<string, unknown>>(
  model: Model,
  source: Record<string, unknown>,
): Where<T> {
  const pkFields = getPrimaryKeyFields(model);
  const clauses: Where<T>[] = [];
  for (let i = 0; i < pkFields.length; i++) {
    const field = pkFields[i]!;
    clauses.push({
      field: field as FieldName<T>,
      op: "eq" as const,
      value: source[field],
    });
  }

  if (clauses.length === 1) {
    return clauses[0]!;
  }

  return { and: clauses };
}

export function assertNoPrimaryKeyUpdates(
  model: Model,
  data: Partial<Record<string, unknown>>,
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
