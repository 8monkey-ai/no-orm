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
export function getIdentityValues<T extends Record<string, unknown>>(
  model: Model,
  data: T,
): Partial<T> {
  const pkFields = getPrimaryKeyFields(model);
  const values: Partial<T> = {};
  for (let i = 0; i < pkFields.length; i++) {
    const field = pkFields[i]!;
    if (!(field in data)) {
      throw new Error(`Missing primary key field: ${field}`);
    }
    const val = data[field as keyof T];
    values[field as keyof T] = val;
  }
  return values;
}

export function validateJsonPath(path: string[]): string[] {
  for (let i = 0; i < path.length; i++) {
    const segment = path[i]!;
    // Faster validation without regex
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
 */
export function buildIdentityFilter<T extends Record<string, unknown>>(
  model: Model,
  source: Partial<T>,
): Where<T> {
  const pkFields = getPrimaryKeyFields(model);
  if (pkFields.length === 0) {
    throw new Error("Model has no primary key defined.");
  }
  if (pkFields.length === 1) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
    const field = pkFields[0]! as FieldName<T>;
    return {
      field,
      op: "eq" as const,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      value: (source as Record<FieldName<T>, unknown>)[field],
    };
  }

  const clauses: Where<T>[] = [];
  for (let i = 0; i < pkFields.length; i++) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
    const field = pkFields[i]! as FieldName<T>;
    clauses.push({
      field,
      op: "eq" as const,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      value: (source as Record<FieldName<T>, unknown>)[field],
    });
  }

  return { and: clauses };
}
export function assertNoPrimaryKeyUpdates<T extends Record<string, unknown>>(
  model: Model,
  data: Partial<T>,
): void {
  const pkFields = getPrimaryKeyFields(model);
  for (let i = 0; i < pkFields.length; i++) {
    const field = pkFields[i]!;
    if ((data as Record<string, unknown>)[field] !== undefined) {
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
