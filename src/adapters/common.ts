import type { FieldName, Model, Where, WhereWithoutPath } from "../types";

// --- Type Guards ---

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function isValidField<T>(field: unknown): field is FieldName<T> {
  return typeof field === "string" && field !== "";
}

export function isStringKey(key: unknown): key is string {
  return typeof key === "string" && key !== "";
}

export function isModelType<T>(obj: unknown): obj is T {
  return typeof obj === "object" && obj !== null;
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

export function validateJsonPath(path: string[]): string[] {
  for (const segment of path) {
    if (!JSON_PATH_SEGMENT.test(segment)) {
      throw new Error(`Invalid JSON path segment: ${segment}`);
    }
  }
  return path;
}

export function extractEqualityWhere<T>(where: WhereWithoutPath<T>): Map<string, unknown> {
  const values = new Map<string, unknown>();

  const visit = (clause: WhereWithoutPath<T>): void => {
    if ("and" in clause) {
      for (const child of clause.and) {
        visit(child);
      }
      return;
    }

    if ("or" in clause) {
      // Upsert needs one deterministic conflict key. Allowing OR conditions would
      // make the conflict target ambiguous across all adapters, not just SQL ones.
      throw new Error("Upsert 'where' clause does not support 'or' conditions.");
    }

    if (clause.path !== undefined) {
      // Path-based filters are query semantics, not stable identity semantics.
      // Keeping them out of upsert avoids backend-specific conflict behavior.
      throw new Error("Upsert 'where' clause does not support JSON paths.");
    }

    if (clause.op !== "eq") {
      // v1 upsert is intentionally conservative: equality on identity fields only.
      throw new Error("Upsert 'where' clause only supports 'eq' conditions.");
    }

    const existing = values.get(clause.field);
    if (existing !== undefined && existing !== clause.value) {
      throw new Error(`Conflicting upsert values for field ${clause.field}.`);
    }
    values.set(clause.field, clause.value);
  };

  visit(where);
  return values;
}

export function getPrimaryKeyWhereValues<T>(
  model: Model,
  where: WhereWithoutPath<T>,
): Record<string, unknown> {
  const equalityWhere = extractEqualityWhere(where);
  const pkFields = getPrimaryKeyFields(model);
  const values: Record<string, unknown> = {};

  if (equalityWhere.size !== pkFields.length) {
    // We currently support primary-key based upserts only. This keeps the same
    // rule across memory, SQLite, and Postgres instead of inventing per-backend
    // uniqueness semantics in v1.
    throw new Error("Upsert requires equality filters for every primary key field.");
  }

  for (const field of pkFields) {
    if (!equalityWhere.has(field)) {
      throw new Error("Upsert requires equality filters for every primary key field.");
    }
    values[field] = equalityWhere.get(field);
  }

  return values;
}

export function buildPrimaryKeyWhere<T extends Record<string, unknown>>(
  model: Model,
  source: Record<string, unknown>,
): Where<T> {
  const pkFields = getPrimaryKeyFields(model);
  const clauses = pkFields.map((field) => ({
    // The schema is the source of truth for valid field names here.
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    field: field as FieldName<T>,
    op: "eq" as const,
    value: source[field],
  }));

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
