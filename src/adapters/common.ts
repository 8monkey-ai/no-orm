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

const JSON_PATH_SEGMENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

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
  for (const field of pkFields) {
    const val = data[field];
    if (val === undefined) {
      throw new Error(`Missing primary key field: ${field}`);
    }
    values[field] = val;
  }
  return values;
}

export function validateJsonPath(path: string[]): string[] {
  for (const segment of path) {
    if (!JSON_PATH_SEGMENT.test(segment)) {
      throw new Error(`Invalid JSON path segment: ${segment}`);
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
  const clauses = pkFields.map((field) => {
    // field is string from getPrimaryKeyFields, narrowing to FieldName<T> is safe
    const fieldName = field as FieldName<T>; // eslint-disable-line @typescript-eslint/no-unsafe-type-assertion
    const leaf: Where<T> = {
      field: fieldName,
      op: "eq" as const,
      value: source[field],
    };
    return leaf;
  });

  if (clauses.length === 1) {
    return clauses[0]!;
  }

  return { and: clauses };
}

export function assertNoPrimaryKeyUpdates(
  model: Model,
  data: Partial<Record<string, unknown>>,
): void {
  for (const field of getPrimaryKeyFields(model)) {
    if (data[field] !== undefined) {
      // Primary-key rewrites are intentionally out of scope for v1 because they
      // complicate refetch, conflict handling, and adapter parity.
      throw new Error("Primary key updates are not supported.");
    }
  }
}

/**
 * Maps database numeric values to JS numbers.
 * Note: bigint is intentionally not supported in v1 to keep the core tiny.
 */
export function mapNumeric(value: unknown): number | null {
  return value === null || value === undefined ? null : Number(value);
}
