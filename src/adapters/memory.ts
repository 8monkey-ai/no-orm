import { createRequire } from "module";
const require = createRequire(import.meta.url);
// eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
const LRUCache = require("lru-cache");

import type { Adapter, Cursor, InferModel, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  getIdentityValues,
  getPrimaryKeyFields,
  isRecord,
} from "./common";

type RowData = Record<string, unknown>;

const DEFAULT_MAX_SIZE = 1000;

interface LRU {
  has(key: string): boolean;
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  delete(key: string): void;
  del(key: string): void;
  forEach(cb: (value: unknown, key: string) => void): void;
  entries?(): IterableIterator<[string, unknown]>;
  keys?(): string[];
  peek?(key: string): unknown;
  size: number;
}

export interface MemoryAdapterOptions {
  maxSize?: number;
}

export class MemoryAdapter<S extends Schema = Schema> implements Adapter<S> {
  private storage = new Map<keyof S, LRU>();

  constructor(
    private schema: S,
    private options?: MemoryAdapterOptions,
  ) {}

  migrate(): Promise<void> {
    const keys = Object.keys(this.schema) as (keyof S)[];
    for (const key of keys) {
      const existing = this.storage.get(key);
      if (existing === undefined) {
        const max = this.options?.maxSize ?? DEFAULT_MAX_SIZE;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-member-access
        const LRUClass = (LRUCache.default ?? LRUCache) as new (o: { max: number }) => LRU;
        this.storage.set(key, new LRUClass({ max }));
      }
    }
    return Promise.resolve();
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return fn(this);
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    await Promise.resolve();
    const { model, data, select } = args;
    const modelStorage = this.getModelStorage(model);
    const pkValue = this.getPrimaryKeyString(model, data);

    if (modelStorage.has(pkValue)) {
      throw new Error(`Record with primary key ${pkValue} already exists in ${model}`);
    }

    const record: RowData = { ...data };
    modelStorage.set(pkValue, record);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
    return this.applySelect(record as T, select);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    await Promise.resolve();
    const { model, where, select } = args;
    const modelStorage = this.getModelStorage(model);

    for (const [, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        (where === undefined || this.evaluateWhere(where as unknown as Where, value))
      ) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        return this.applySelect(value as T, select);
      }
    }

    return null;
  }

  async findMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }): Promise<T[]> {
    await Promise.resolve();
    const { model, where, select, sortBy, limit, offset, cursor } = args;
    const modelStorage = this.getModelStorage(model);

    const results: T[] = [];
    for (const [, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        (where === undefined || this.evaluateWhere(where as unknown as Where, value))
      ) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        results.push(value as T);
      }
    }

    let processedResults = [...results];

    if (cursor !== undefined) {
      const cursorValues = cursor.after as Record<string, unknown>;
      const criteria: { field: string; direction: "asc" | "desc"; path?: string[] }[] = [];
      if (sortBy !== undefined && sortBy.length > 0) {
        for (let i = 0; i < sortBy.length; i++) {
          const s = sortBy[i]!;
          if (cursorValues[s.field] !== undefined) {
            criteria.push({ field: s.field, direction: s.direction ?? "asc", path: s.path });
          }
        }
      } else {
        const keys = Object.keys(cursorValues);
        for (let i = 0; i < keys.length; i++) {
          const k = keys[i]!;
          criteria.push({ field: k, direction: "asc", path: undefined });
        }
      }

      if (criteria.length > 0) {
        const filtered: T[] = [];
        for (let i = 0; i < processedResults.length; i++) {
          const record = processedResults[i]!;
          let match = false;
          for (let j = 0; j < criteria.length; j++) {
            const curr = criteria[j]!;
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
            const recordVal = this.getValue(record as unknown as RowData, curr.field, curr.path);
            const cursorVal = cursorValues[curr.field];
            const comp = this.compareValues(recordVal, cursorVal);

            if (comp === 0) continue;
            if (curr.direction === "desc" ? comp < 0 : comp > 0) {
              match = true;
            }
            break;
          }
          if (match) filtered.push(record);
        }
        processedResults = filtered;
      }
    }

    if (sortBy !== undefined && sortBy.length > 0) {
      processedResults.sort((a, b) => {
        for (let i = 0; i < sortBy.length; i++) {
          const s = sortBy[i]!;
          const field = s.field;
          const direction = s.direction;
          const path = s.path;
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
          const valA = this.getValue(a as unknown as RowData, field, path);
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
          const valB = this.getValue(b as unknown as RowData, field, path);
          if (valA === valB) continue;
          const comparison = this.compareValues(valA, valB);
          if (comparison === 0) continue;
          return direction === "desc" ? -comparison : comparison;
        }
        return 0;
      });
    }

    const start = offset ?? 0;
    const end = limit === undefined ? processedResults.length : start + limit;
    const paginatedResults: T[] = [];
    for (let i = start; i < end && i < processedResults.length; i++) {
      paginatedResults.push(this.applySelect(processedResults[i]!, select));
    }

    return paginatedResults;
  }

  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; data: Partial<T> }): Promise<T | null> {
    await Promise.resolve();
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);
    const modelStorage = this.getModelStorage(model);

    for (const [key, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        this.evaluateWhere(where as unknown as Where, value)
      ) {
        const updated: RowData = { ...(value as object), ...(data as object) };
        modelStorage.set(key, updated);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        return this.applySelect(updated as T);
      }
    }

    return null;
  }

  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    await Promise.resolve();
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);
    const modelStorage = this.getModelStorage(model);

    const updates: { key: string; record: T }[] = [];
    for (const [key, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        (where === undefined || this.evaluateWhere(where as unknown as Where, value))
      ) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        updates.push({ key, record: value as T });
      }
    }

    for (let i = 0; i < updates.length; i++) {
      const item = updates[i]!;
      const key = item.key;
      const record = item.record;
      const updated: RowData = { ...(record as object), ...(data as object) };
      modelStorage.set(key, updated);
    }

    return updates.length;
  }

  async upsert<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }): Promise<T> {
    await Promise.resolve();
    const { model, create, update, where, select } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, update);

    const pkValue = this.getPrimaryKeyString(model, create);
    const modelStorage = this.getModelStorage(model);
    const existing = modelStorage.get(pkValue);

    if (existing !== undefined && isRecord(existing)) {
      if (
        where === undefined ||
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        this.evaluateWhere(where as unknown as Where, existing)
      ) {
        const updated: RowData = { ...(existing as object), ...(update as object) };
        modelStorage.set(pkValue, updated);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        return this.applySelect(updated as T, select);
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      return this.applySelect(existing as T, select);
    }

    return this.create({ model, data: create, select });
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    await Promise.resolve();
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);

    for (const [key, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        this.evaluateWhere(where as unknown as Where, value)
      ) {
        this.lruDelete(modelStorage, key);
        return;
      }
    }
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    await Promise.resolve();
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);
    const toDelete: string[] = [];

    for (const [key, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        (where === undefined || this.evaluateWhere(where as unknown as Where, value))
      ) {
        toDelete.push(key);
      }
    }

    for (let i = 0; i < toDelete.length; i++) {
      this.lruDelete(modelStorage, toDelete[i]!);
    }

    return toDelete.length;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    await Promise.resolve();
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);

    if (where === undefined) return modelStorage.size;

    let count = 0;
    for (const [, value] of this.getEntries(modelStorage)) {
      if (
        isRecord(value) &&
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        this.evaluateWhere(where as unknown as Where, value)
      ) {
        count++;
      }
    }
    return count;
  }

  private lruDelete(lru: LRU, key: string): void {
    if (typeof lru.delete === "function") {
      lru.delete(key);
    } else if (typeof lru.del === "function") {
      lru.del(key);
    }
  }

  private *getEntries(lru: LRU): IterableIterator<[string, unknown]> {
    if (typeof lru.entries === "function") {
      yield* lru.entries();
    } else if (typeof lru.keys === "function") {
      const keys = lru.keys();
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i]!;
        const v = typeof lru.peek === "function" ? lru.peek(k) : lru.get(k);
        yield [k, v];
      }
    } else {
      const entries: [string, unknown][] = [];
      lru.forEach((v, k) => entries.push([k, v]));
      yield* entries;
    }
  }

  private getModelStorage<K extends keyof S & string>(model: K): LRU {
    const storage = this.storage.get(model);
    if (storage === undefined)
      throw new Error(`Model ${model} not initialized. Call migrate() first.`);
    return storage;
  }

  private getModel<K extends keyof S & string>(model: K): S[K] {
    const spec = this.schema[model];
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
      const pkField = pkFields[i]!;
      const val = pkValues[pkField];
      if (val !== null && val !== undefined) {
        if (typeof val === "object") {
          res += JSON.stringify(val);
        } else {
          // eslint-disable-next-line @typescript-eslint/no-base-to-string
          res += String(val);
        }
      }
    }
    return res;
  }

  private applySelect<T extends RowData>(record: T, select?: Select<T>): T {
    if (select === undefined) {
      return { ...record };
    }
    const res: RowData = {};
    for (let i = 0; i < select.length; i++) {
      const k = select[i]!;
      res[k] = record[k] ?? null;
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
    return res as T;
  }

  private getValue(record: RowData, field: string, path?: string[]): unknown {
    let val = record[field];
    if (path !== undefined && path.length > 0) {
      for (let i = 0; i < path.length; i++) {
        if (!isRecord(val)) return undefined;
        const subKey = path[i]!;
        val = val[subKey];
      }
    }
    return val;
  }

  private evaluateWhere(where: Where, record: RowData): boolean {
    if ("and" in where) {
      const and = where.and;
      for (let i = 0; i < and.length; i++) {
        if (!this.evaluateWhere(and[i]!, record)) return false;
      }
      return true;
    }
    if ("or" in where) {
      const or = where.or;
      for (let i = 0; i < or.length; i++) {
        if (this.evaluateWhere(or[i]!, record)) return true;
      }
      return false;
    }

    const field = where.field;
    const op = where.op;
    const value = where.value;
    const path = where.path;
    const recordVal = this.getValue(record, field, path);

    switch (op) {
      case "eq":
        return recordVal === value;
      case "ne":
        return recordVal !== value;
      case "gt":
        return this.compareValues(recordVal, value) > 0;
      case "gte":
        return this.compareValues(recordVal, value) >= 0;
      case "lt":
        return this.compareValues(recordVal, value) < 0;
      case "lte":
        return this.compareValues(recordVal, value) <= 0;
      case "in":
        return Array.isArray(value) && value.includes(recordVal);
      case "not_in":
        return Array.isArray(value) && !value.includes(recordVal);
    }
    return false;
  }

  private compareValues(left: unknown, right: unknown): number {
    if (left === right) return 0;
    if (left === undefined) return -1;
    if (right === undefined) return 1;
    if (left === null) return -1;
    if (right === null) return 1;
    if (typeof left !== typeof right) return 0;
    if (typeof left === "string" && typeof right === "string") {
      if (left < right) return -1;
      if (left > right) return 1;
    }
    if (typeof left === "number" && typeof right === "number") {
      if (left < right) return -1;
      if (left > right) return 1;
    }
    return 0;
  }
}
