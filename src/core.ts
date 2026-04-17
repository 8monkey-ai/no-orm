/**
 * no-orm Core v1: Canonical Schema and Adapter Specification
 */

// --- SCHEMA SPEC V1 (#2) ---

export type Schema = Record<string, Model>;

export interface Model {
  fields: Record<string, Field>;
  primaryKey: {
    fields: [string, ...string[]];
  };
  indexes?: Index[];
}

export interface Field {
  type: FieldType;
  nullable?: boolean;
}

export type FieldType =
  | { type: "string"; max?: number }
  | { type: "number" }
  | { type: "boolean" }
  | { type: "timestamp" }
  | { type: "json" };

export interface Index {
  fields: [IndexField, ...IndexField[]];
}

export interface IndexField {
  field: string;
  order?: "asc" | "desc";
}

// --- TYPE INFERENCE V1 (#1) ---

export type InferModel<M extends Model> = {
  [K in keyof M["fields"]]: M["fields"][K]["nullable"] extends true
    ? ResolveTSValue<M["fields"][K]["type"]> | null
    : ResolveTSValue<M["fields"][K]["type"]>;
};

type ResolveTSValue<T extends FieldType> = T["type"] extends "string"
  ? string
  : T["type"] extends "number"
    ? number
    : T["type"] extends "boolean"
      ? boolean
      : T["type"] extends "timestamp"
        ? number
        : T["type"] extends "json"
          ? Record<string, unknown> // Note: Defaults to object record, may need casting for JSON arrays
          : never;

// --- ADAPTER SPEC V1 (#3) ---

export interface Adapter {
  migrate?(args: { schema: Schema }): Promise<void>;

  transaction?<T>(fn: (tx: Adapter) => Promise<T>): Promise<T>;

  create<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    data: T;
    select?: Select<T>;
  }): Promise<T>;

  update<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null>;

  updateMany<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
    data: Partial<T>;
  }): Promise<number>;

  upsert?<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
    create: T;
    update: Partial<T>;
    select?: Select<T>;
  }): Promise<T>;

  delete<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
  }): Promise<void>;

  deleteMany?<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
  }): Promise<number>;

  find<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null>;

  findMany<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }): Promise<T[]>;

  count?<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
  }): Promise<number>;
}

export type FieldName<T> = Extract<keyof T, string>;

export type Select<T> = ReadonlyArray<FieldName<T>>;

export type Where<T = Record<string, unknown>> =
  | {
      field: FieldName<T>;
      op: "eq" | "ne";
      value: unknown;
    }
  | {
      field: FieldName<T>;
      op: "gt" | "gte" | "lt" | "lte";
      value: unknown;
    }
  | {
      field: FieldName<T>;
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
  direction?: "asc" | "desc";
}

export interface Cursor<T = Record<string, unknown>> {
  after: Partial<Record<FieldName<T>, unknown>>;
}
