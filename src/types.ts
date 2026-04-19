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

export type FieldType =
  | "string"
  | "number"
  | "boolean"
  | "timestamp"
  | "json"
  | "json[]";

export interface Index {
  field: string | string[];
  order?: "asc" | "desc";
}

// --- TYPE INFERENCE V1 (#1) ---

export type InferModel<M extends Model> = {
  [K in keyof M["fields"]]: M["fields"][K]["nullable"] extends true
    ? ResolveTSValue<M["fields"][K]["type"]> | null
    : ResolveTSValue<M["fields"][K]["type"]>;
};

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
  migrate?(args: { schema: S }): Promise<void>;

  transaction?<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T>;

  create<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }): Promise<T>;

  update<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null>;

  updateMany<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
    data: Partial<T>;
  }): Promise<number>;

  upsert?<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: WhereWithoutPath<T>;
    create: T;
    update: Partial<T>;
    select?: Select<T>;
  }): Promise<T>;

  delete<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void>;

  deleteMany?<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number>;

  find<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null>;

  findMany<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }): Promise<T[]>;

  count?<K extends keyof S & string, T = InferModel<S[K]>>(args: {
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
      op: "eq" | "ne";
      value: unknown;
    }
  | {
      field: FieldName<T>;
      path?: string[];
      op: "gt" | "gte" | "lt" | "lte";
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

export type WhereWithoutPath<T = Record<string, unknown>> = Omit<Where<T>, "path"> & {
  path?: never;
};

export interface SortBy<T = Record<string, unknown>> {
  field: FieldName<T>;
  path?: string[];
  direction?: "asc" | "desc";
}

export interface Cursor<T = Record<string, unknown>> {
  after: Partial<Record<FieldName<T>, unknown>>;
}
