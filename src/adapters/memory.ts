import type {
  Adapter,
  Cursor,
  InferModel,
  Schema,
  Select,
  SortBy,
  Where,
} from "../types";
import {
  assertNoPrimaryKeyUpdates,
  getIdentityValues,
  getPrimaryKeyFields,
  isRecord,
} from "./common";

type Comparable = string | number;
type RowData = Record<string, unknown>;

/**
 * A zero-dependency, in-memory implementation of the no-orm Adapter interface.
 * Useful for testing, development, and small-scale caching.
 */
export class MemoryAdapter<S extends Schema = Schema> implements Adapter<S> {
  private storage = new Map<string, Map<string, Record<string, unknown>>>();

  constructor(private schema: S) {}

  migrate(_args: { schema: S }): Promise<void> {
    for (const name of Object.keys(this.schema)) {
      if (!this.storage.has(name)) {
        this.storage.set(name, new Map());
      }
    }
    return Promise.resolve();
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    // Basic execution for V1. In-memory snapshots for true isolation
    // are deferred to future versions.
    return fn(this);
  }

  create<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }): Promise<T> {
    const { model, data, select } = args;
    const modelStorage = this.getModelStorage<K, T>(model);
    const pkValue = this.getPrimaryKeyString(model, data);

    if (modelStorage.has(pkValue)) {
      throw new Error(`Record with primary key ${pkValue} already exists in ${model}`);
    }

    // Optimization: Avoid object spread in hot path
    const record = Object.assign({}, data) as T;
    modelStorage.set(pkValue, record);

    return Promise.resolve(this.applySelect(record, select));
  }

  find<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null> {
    const { model, where, select } = args;
    const modelStorage = this.getModelStorage<K, T>(model);

    for (const record of modelStorage.values()) {
      if (this.evaluateWhere(where, record as T)) {
        return Promise.resolve(this.applySelect(record as T, select));
      }
    }

    return Promise.resolve(null);
  }

  findMany<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }): Promise<T[]> {
    const { model, where, select, sortBy, limit, offset, cursor } = args;
    const modelStorage = this.getModelStorage<K, T>(model);

    let results = Array.from(modelStorage.values()) as T[];

    if (where) {
      results = results.filter((record) => this.evaluateWhere(where, record));
    }

    if (cursor) {
      const cursorValues = cursor.after as Record<string, unknown>;
      const sortCriteria =
        sortBy !== undefined && sortBy.length > 0
          ? sortBy
              .filter((sort) => cursorValues[sort.field] !== undefined)
              .map((sort) => ({
                field: sort.field as string,
                direction: sort.direction ?? "asc",
                path: sort.path,
              }))
          : Object.keys(cursor.after).map((field) => ({
              field,
              direction: "asc" as const,
              path: undefined,
            }));

      if (sortCriteria.length > 0) {
        results = results.filter((record) => {
          // Lexicographic keyset pagination:
          // (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
          for (let i = 0; i < sortCriteria.length; i++) {
            let allPreviousEqual = true;
            for (let j = 0; j < i; j++) {
              const prev = sortCriteria[j]!;
              const recordVal = this.getValue(record as RowData, prev.field, prev.path);
              const cursorVal = cursorValues[prev.field];
              if (this.compareValues(recordVal, cursorVal) !== 0) {
                allPreviousEqual = false;
                break;
              }
            }

            if (!allPreviousEqual) continue;

            const current = sortCriteria[i]!;
            const recordVal = this.getValue(record as RowData, current.field, current.path);
            const cursorVal = cursorValues[current.field];
            const comp = this.compareValues(recordVal, cursorVal);

            if (current.direction === "desc") {
              if (comp < 0) return true;
            } else if (comp > 0) {
              return true;
            }

            // If this was the last criteria and it's equal, it doesn't satisfy "after"
          }
          return false;
        });
      }
    }

    if (sortBy) {
      results.sort((a, b) => {
        for (const { field, direction, path } of sortBy) {
          const valA = this.getValue(a as RowData, field as string, path);
          const valB = this.getValue(b as RowData, field as string, path);
          if (valA === valB) continue;
          const factor = direction === "desc" ? -1 : 1;
          if (valA === undefined || valB === undefined) return 0;
          const comparison = this.compareValues(valA, valB);
          if (comparison === 0) continue;
          return comparison * factor;
        }
        return 0;
      });
    }

    const start = offset ?? 0;
    const end = limit === undefined ? undefined : start + limit;
    results = results.slice(start, end);

    return Promise.resolve(results.map((record) => this.applySelect(record, select)));
  }

  update<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);
    const modelStorage = this.getModelStorage<K, T>(model);

    for (const [pk, record] of modelStorage.entries()) {
      if (this.evaluateWhere(where, record as T)) {
        // Optimization: Create a new object to avoid mutating internal storage reference
        const updated = Object.assign({}, record, data) as T;
        modelStorage.set(pk, updated);
        return Promise.resolve(this.applySelect(updated, undefined));
      }
    }

    return Promise.resolve(null);
  }

  updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);
    const modelStorage = this.getModelStorage<K, T>(model);
    let count = 0;

    for (const [pk, record] of modelStorage.entries()) {
      if (where === undefined || this.evaluateWhere(where, record as T)) {
        // Optimization: Create a new object to avoid mutating internal storage reference
        const updated = Object.assign({}, record, data) as T;
        modelStorage.set(pk, updated);
        count++;
      }
    }

    return Promise.resolve(count);
  }

  upsert<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }): Promise<T> {
    const { model, create, update, where, select } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, update);

    const pkValue = this.getPrimaryKeyString(model, create);
    const modelStorage = this.getModelStorage<K, T>(model);
    const existing = modelStorage.get(pkValue);

    if (existing) {
      // Use optional where predicate
      if (where === undefined || this.evaluateWhere(where, existing as T)) {
        const updated = Object.assign({}, existing, update) as T;
        modelStorage.set(pkValue, updated);
        return Promise.resolve(this.applySelect(updated, select));
      }
      return Promise.resolve(this.applySelect(existing as T, select));
    }

    return this.create({ model, data: create, select });
  }

  delete<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage<K, T>(model);

    for (const [pk, record] of modelStorage.entries()) {
      if (this.evaluateWhere(where, record as T)) {
        modelStorage.delete(pk);
        return Promise.resolve();
      }
    }
    return Promise.resolve();
  }

  deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage<K, T>(model);
    let count = 0;

    for (const [pk, record] of modelStorage.entries()) {
      if (where === undefined || this.evaluateWhere(where, record as T)) {
        modelStorage.delete(pk);
        count++;
      }
    }

    return Promise.resolve(count);
  }

  count<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage<K, T>(model);

    if (!where) return Promise.resolve(modelStorage.size);

    let count = 0;
    for (const record of modelStorage.values()) {
      if (this.evaluateWhere(where, record as T)) {
        count++;
      }
    }
    return Promise.resolve(count);
  }

  // --- Helpers ---

  private getModelStorage<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(model: K): Map<string, Record<string, unknown>> {
    const storage = this.storage.get(model);
    if (!storage) {
      throw new Error(`Model ${model} not initialized. Call migrate() first.`);
    }
    return storage;
  }

  private getModel<K extends keyof S & string>(model: K): S[K] {
    const modelSpec = this.schema[model];
    if (modelSpec === undefined) {
      throw new Error(`Model ${model} not found in schema`);
    }
    return modelSpec;
  }

  private getPrimaryKeyString(modelName: string, data: Record<string, unknown>): string {
    const model = this.getModel(modelName as keyof S & string);
    const pkValues = getIdentityValues(model, data);
    return getPrimaryKeyFields(model)
      .map((field) => String(pkValues[field]))
      .join("|");
  }

  private applySelect<T extends RowData>(record: T, select?: Select<T>): T {
    const result: Partial<T> = {};

    if (select === undefined) {
      // Always return a shallow clone to match DB snapshot behavior
      return Object.assign(result, record) as T;
    }

    for (const field of select) {
      const val = record[field];
      // Normalize undefined to null to match SQL behavior
      result[field] = val === undefined ? (null as any) : val;
    }

    return result as T;
  }

  private getValue(record: RowData, field: string, path?: string[]): unknown {
    let value = record[field];
    if (path && path.length > 0) {
      for (const segment of path) {
        if (!isRecord(value)) {
          return undefined;
        }
        value = (value as Record<string, unknown>)[segment];
      }
    }
    return value;
  }

  private evaluateWhere<T extends RowData>(where: Where<T>, record: T): boolean {
    if ("and" in where) {
      return where.and.every((w) => this.evaluateWhere(w, record));
    }
    if ("or" in where) {
      return where.or.some((w) => this.evaluateWhere(w, record));
    }

    const leaf = where as { field: string; op: string; value: unknown; path?: string[] };
    const { field, op, value, path } = leaf;
    const recordValue = this.getValue(record, field, path);

    switch (op) {
      case "eq":
        return recordValue === value;
      case "ne":
        return recordValue !== value;
      case "gt":
        return this.compareValues(recordValue, value) > 0;
      case "gte":
        return this.compareValues(recordValue, value) >= 0;
      case "lt":
        return this.compareValues(recordValue, value) < 0;
      case "lte":
        return this.compareValues(recordValue, value) <= 0;
      case "in":
        return Array.isArray(value) && value.includes(recordValue);
      case "not_in":
        return Array.isArray(value) && !value.includes(recordValue);
      default:
        return false;
    }
  }

  private compareValues(left: unknown, right: unknown): number {
    if (left === right) return 0;
    if (!this.isComparable(left) || !this.isComparable(right)) return 0;
    if (typeof left !== typeof right) return 0;
    return (left as any) < (right as any) ? -1 : 1;
  }

  private isComparable(value: unknown): value is Comparable {
    return typeof value === "string" || typeof value === "number";
  }
}
