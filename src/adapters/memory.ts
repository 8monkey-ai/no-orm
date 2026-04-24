import { LRUCache } from "lru-cache";

import type { Adapter, Cursor, InferModel, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  getIdentityValues,
  getNestedValue,
  getPaginationCriteria,
  getPrimaryKeyFields,
} from "./common";

type RowData = Record<string, unknown>;
type ModelCache = LRUCache<string, RowData>;

const DEFAULT_MAX_SIZE = 1000;

export interface MemoryAdapterOptions {
  maxSize?: number;
}

export class MemoryAdapter<S extends Schema = Schema> implements Adapter<S> {
  private storage = new Map<keyof S, ModelCache>();

  constructor(
    private schema: S,
    private options?: MemoryAdapterOptions,
  ) {}

  migrate(): Promise<void> {
    const keys = Object.keys(this.schema) as (keyof S)[];
    for (const key of keys) {
      if (!this.storage.has(key)) {
        this.storage.set(key, new LRUCache({ max: this.options?.maxSize ?? DEFAULT_MAX_SIZE }));
      }
    }
    return Promise.resolve();
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return fn(this);
  }

  create<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }): Promise<T> {
    const { model, data, select } = args;
    const cache = this.getModelStorage(model);
    const pkValue = this.getPrimaryKeyString(model, data);

    if (cache.has(pkValue)) {
      throw new Error(`Record with primary key ${pkValue} already exists in ${model}`);
    }

    const record: RowData = Object.assign({}, data);
    cache.set(pkValue, record);
    return Promise.resolve(this.applySelect<T>(record, select));
  }

  find<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null> {
    const { model, where, select } = args;
    const cache = this.getModelStorage(model);

    for (const [, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) {
        return Promise.resolve(this.applySelect<T>(value, select));
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
    const cache = this.getModelStorage(model);

    let results: RowData[] = [];
    for (const [, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) {
        results.push(value);
      }
    }

    if (cursor !== undefined) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SortBy<T>.field is a subtype of string, safe for internal use
      results = this.applyCursor(results, cursor, sortBy as SortBy[] | undefined);
    }

    if (sortBy !== undefined && sortBy.length > 0) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- same as above
      results = this.applySort(results, sortBy as SortBy[]);
    }

    const start = offset ?? 0;
    const end = limit === undefined ? results.length : start + limit;
    const out: T[] = [];
    for (let i = start; i < end && i < results.length; i++) {
      out.push(this.applySelect<T>(results[i]!, select));
    }
    return Promise.resolve(out);
  }

  update<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null> {
    const { model, where, data } = args;
    assertNoPrimaryKeyUpdates(this.getModel(model), data);
    const cache = this.getModelStorage(model);

    for (const [key, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) {
        const updated: RowData = Object.assign({}, value, data);
        cache.set(key, updated);
        return Promise.resolve(this.applySelect<T>(updated));
      }
    }
    return Promise.resolve(null);
  }

  updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model, where, data } = args;
    assertNoPrimaryKeyUpdates(this.getModel(model), data);
    const cache = this.getModelStorage(model);

    // Collect first, then mutate — avoids mutation during iteration
    const matches: { key: string; value: RowData }[] = [];
    for (const [key, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) {
        matches.push({ key, value });
      }
    }
    for (let i = 0; i < matches.length; i++) {
      const m = matches[i]!;
      cache.set(m.key, Object.assign({}, m.value, data));
    }
    return Promise.resolve(matches.length);
  }

  upsert<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
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
    const cache = this.getModelStorage(model);
    const existing = cache.get(pkValue);

    if (existing !== undefined) {
      if (this.matchesWhere(where, existing)) {
        const updated: RowData = Object.assign({}, existing, update);
        cache.set(pkValue, updated);
        return Promise.resolve(this.applySelect<T>(updated, select));
      }
      return Promise.resolve(this.applySelect<T>(existing, select));
    }

    return this.create({ model, data: create, select });
  }

  delete<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void> {
    const { model, where } = args;
    const cache = this.getModelStorage(model);

    for (const [key, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) {
        cache.delete(key);
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
    const cache = this.getModelStorage(model);
    const toDelete: string[] = [];

    for (const [key, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) {
        toDelete.push(key);
      }
    }
    for (let i = 0; i < toDelete.length; i++) {
      cache.delete(toDelete[i]!);
    }
    return Promise.resolve(toDelete.length);
  }

  count<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    const cache = this.getModelStorage(model);

    if (where === undefined) return Promise.resolve(cache.size);

    let count = 0;
    for (const [, value] of cache.entries()) {
      if (this.matchesWhere(where, value)) count++;
    }
    return Promise.resolve(count);
  }

  // --- Private helpers ---

  private getModelStorage(model: string): ModelCache {
    const storage = this.storage.get(model);
    if (storage === undefined) {
      throw new Error(`Model ${model} not initialized. Call migrate() first.`);
    }
    return storage;
  }

  private getModel(model: string): S[keyof S & string] {
    const spec = this.schema[model as keyof S & string];
    if (spec === undefined) throw new Error(`Model ${model} not found in schema`);
    return spec;
  }

  private getPrimaryKeyString(modelName: string, data: Record<string, unknown>): string {
    const model = this.getModel(modelName);
    const pkValues = getIdentityValues(model, data);
    const pkFields = getPrimaryKeyFields(model);
    let res = "";
    for (let i = 0; i < pkFields.length; i++) {
      if (i > 0) res += "|";
      const val = pkValues[pkFields[i]!];
      if (val !== null && val !== undefined) {
        if (typeof val === "object") {
          res += JSON.stringify(val);
        } else if (typeof val === "string" || typeof val === "number" || typeof val === "boolean") {
          res += String(val);
        }
      }
    }
    return res;
  }

  /**
   * Checks if a record matches a Where filter.
   * Accepts Where<T> for any T — since RowData is Record<string, unknown>,
   * all field names are valid string keys. This avoids repeated generic casts.
   */
  private matchesWhere(where: Where | undefined, record: RowData): boolean {
    if (where === undefined) return true;
    return this.evaluateWhere(where, record);
  }

  private evaluateWhere(where: Where, record: RowData): boolean {
    if ("and" in where) {
      for (let i = 0; i < where.and.length; i++) {
        if (!this.evaluateWhere(where.and[i]!, record)) return false;
      }
      return true;
    }
    if ("or" in where) {
      for (let i = 0; i < where.or.length; i++) {
        if (this.evaluateWhere(where.or[i]!, record)) return true;
      }
      return false;
    }

    const recordVal = getNestedValue(record, where.field, where.path);

    switch (where.op) {
      case "eq":
        return recordVal === where.value;
      case "ne":
        return recordVal !== where.value;
      case "gt":
        return compareValues(recordVal, where.value) > 0;
      case "gte":
        return compareValues(recordVal, where.value) >= 0;
      case "lt":
        return compareValues(recordVal, where.value) < 0;
      case "lte":
        return compareValues(recordVal, where.value) <= 0;
      case "in":
        return Array.isArray(where.value) && where.value.includes(recordVal);
      case "not_in":
        return Array.isArray(where.value) && !where.value.includes(recordVal);
    }
    return false;
  }

  /**
   * Projects a record to the selected fields, returning a shallow copy.
   * The `as T` casts are intentional: storage holds RowData but the adapter
   * interface promises T. This is the single boundary where the cast occurs.
   */
  private applySelect<T extends Record<string, unknown>>(record: RowData, select?: Select<T>): T {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- RowData -> T at adapter boundary
    if (select === undefined) return Object.assign({}, record) as T;
    const res: RowData = {};
    for (let i = 0; i < select.length; i++) {
      const k = select[i]!;
      res[k] = record[k] ?? null;
    }
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- RowData -> T at adapter boundary
    return res as T;
  }

  private applyCursor(results: RowData[], cursor: Cursor, sortBy?: SortBy[]): RowData[] {
    const cursorValues = cursor.after as Record<string, unknown>;
    const criteria = getPaginationCriteria(cursor, sortBy);

    if (criteria.length === 0) return results;

    const filtered: RowData[] = [];
    for (let i = 0; i < results.length; i++) {
      const record = results[i]!;
      let match = false;
      // Lexicographic keyset pagination:
      // (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
      for (let j = 0; j < criteria.length; j++) {
        const curr = criteria[j]!;
        const recordVal = getNestedValue(record, curr.field, curr.path);
        const cursorVal = cursorValues[curr.field];
        const comp = compareValues(recordVal, cursorVal);

        if (comp === 0) continue;
        if (curr.direction === "desc" ? comp < 0 : comp > 0) {
          match = true;
        }
        break;
      }
      if (match) filtered.push(record);
    }
    return filtered;
  }

  private applySort(results: RowData[], sortBy: SortBy[]): RowData[] {
    return results.toSorted((a, b) => {
      for (let i = 0; i < sortBy.length; i++) {
        const s = sortBy[i]!;
        const valA = getNestedValue(a, s.field, s.path);
        const valB = getNestedValue(b, s.field, s.path);
        if (valA === valB) continue;
        const comparison = compareValues(valA, valB);
        if (comparison === 0) continue;
        return s.direction === "desc" ? -comparison : comparison;
      }
      return 0;
    });
  }
}

/**
 * Null-safe comparison of primitive values.
 * Treats null/undefined as the smallest possible values.
 */
function compareValues(left: unknown, right: unknown): number {
  if (left === right) return 0;
  if (left === undefined || left === null) return -1;
  if (right === undefined || right === null) return 1;
  if (typeof left !== typeof right) return 0;
  if (typeof left === "string" && typeof right === "string") {
    return left < right ? -1 : left > right ? 1 : 0;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : left > right ? 1 : 0;
  }
  return 0;
}
