/**
 * no-orm Core v1: Canonical Schema and Adapter Specification
 */

// --- SCHEMA SPEC V1 (#2) ---

export type Schema = Record<string, Model>;

export interface Model {
  fields: Record<string, Field>;
  primaryKey: string | string[];
  indexes?: Index[];
}

export interface Field {
  type: FieldType;
  nullable?: boolean;
  max?: number; // Only for string
}

export type FieldType = "string" | "number" | "boolean" | "timestamp" | "json" | "json[]";
// Note: "number" and "timestamp" intentionally exclude bigint support in v1 to keep the core tiny.

export interface Index {
  field: string | string[];
  order?: "asc" | "desc";
}

// --- TYPE INFERENCE V1 (#1) ---

export type InferModel<M extends Model> = {
  [K in keyof M["fields"] as M["fields"][K]["nullable"] extends true ? K : never]?: ResolveTSValue<
    M["fields"][K]["type"]
  > | null;
} & {
  [K in keyof M["fields"] as M["fields"][K]["nullable"] extends true ? never : K]: ResolveTSValue<
    M["fields"][K]["type"]
  >;
} & Record<string, unknown>;

type ResolveTSValue<T extends FieldType> = T extends "string"
  ? string
  : T extends "number"
    ? number
    : T extends "boolean"
      ? boolean
      : T extends "timestamp"
        ? number
        : T extends "json"
          ? Record<string, unknown> // Note: Defaults to object record, may need casting for JSON arrays
          : T extends "json[]"
            ? unknown[]
            : never;

// --- ADAPTER SPEC V1 (#3) ---

export interface Adapter<S extends Schema = Schema> {
  /**
   * Initializes the database schema. Should be idempotent.
   */
  migrate?(args: { schema: S }): Promise<void>;

  /**
   * Executes a callback within a database transaction.
   * Implementation may vary by adapter (e.g., in-memory vs SQL).
   */
  transaction?<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T>;

  /**
   * Inserts a new record.
   * @throws Error if a record with the same primary key already exists.
   */
  create<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }): Promise<T>;

  /**
   * Updates a single record matching the mandatory 'where' clause.
   * Primary key fields in 'data' are forbidden or ignored to prevent identity swaps.
   * @returns The updated record, or null if no record matched 'where'.
   */
  update<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null>;

  /**
   * Updates multiple records matching the 'where' clause.
   * Primary key fields in 'data' are forbidden or ignored.
   * @returns The number of records updated.
   */
  updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
    data: Partial<T>;
  }): Promise<number>;

  /**
   * Atomic insert-or-update.
   * Uses the primary key extracted from 'create' to check for existence.
   * If the record exists, 'update' is applied only if it satisfies the optional 'where' predicate.
   * If the record does not exist, 'create' is applied.
   */
  upsert?<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }): Promise<T>;

  /**
   * Deletes a single record matching the 'where' clause.
   */
  delete<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void>;

  /**
   * Deletes multiple records matching the 'where' clause.
   * @returns The number of records deleted.
   */
  deleteMany?<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number>;

  /**
   * Finds the first record matching the 'where' clause.
   */
  find<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null>;

  /**
   * Finds all records matching the 'where' clause with sorting and pagination support.
   */
  findMany<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }): Promise<T[]>;

  /**
   * Returns the count of records matching the 'where' clause.
   */
  count?<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number>;
}

export type FieldName<T> = Extract<keyof T, string>;

export type Select<T> = ReadonlyArray<FieldName<T>>;

export type Where<T = Record<string, unknown>> =
  | {
      field: FieldName<T>;
      path?: string[];
      op: "eq" | "ne" | "gt" | "gte" | "lt" | "lte";
      value: unknown;
    }
  | {
      field: FieldName<T>;
      path?: string[];
      op: "in" | "not_in";
      value: unknown[];
    }
  | {
      and: Where<T>[];
    }
  | {
      or: Where<T>[];
    };

export interface SortBy<T = Record<string, unknown>> {
  field: FieldName<T>;
  path?: string[];
  direction?: "asc" | "desc";
}

export interface Cursor<T = Record<string, unknown>> {
  after: Partial<Record<FieldName<T>, unknown>>;
}
