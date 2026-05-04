import { LRUCache } from "lru-cache";

import type { Adapter, Cursor, InferModel, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  getNestedValue,
  getPaginationFilter,
  getPrimaryKeyFields,
  getPrimaryKeyValues,
} from "./utils/common";

type RowData = Record<string, unknown>;

const DEFAULT_MAX_ITEMS = 1000;

export interface MemoryAdapterOptions {
  maxItems?: number;
}

/**
 * In-memory adapter with bounded global storage and high-performance indexed scans.
 *
 * Technical Design:
 * - Table Storage: Per-table arrays (Heaps) allow for O(1) indexed scans.
 * - PK Index: Per-table Maps for O(1) primary key lookups.
 * - Global Eviction: A single LRUCache tracks all rows across all tables to enforce maxItems.
 * - O(1) Removals: Uses an index map and swap-and-pop to remove evicted rows without array shifts.
 */
export class MemoryAdapter<S extends Schema> implements Adapter<S> {
  private tables = new Map<keyof S, RowData[]>();
  private pkIndexes = new Map<keyof S, Map<string, RowData>>();
  private indexMap = new Map<RowData, number>();
  private globalLRU: LRUCache<RowData, keyof S & string>;

  constructor(
    private schema: S,
    private options?: MemoryAdapterOptions,
  ) {
    this.globalLRU = new LRUCache<RowData, keyof S & string>({
      max: this.options?.maxItems ?? DEFAULT_MAX_ITEMS,
      dispose: (model, row, reason) => {
        if (reason === "evict" || reason === "set") {
          this.removeFromTable(row, model);
        }
      },
    });

    const keys = Object.keys(this.schema) as (keyof S)[];
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!;
      this.tables.set(key, []);
      this.pkIndexes.set(key, new Map());
    }
  }

  migrate(): Promise<void> {
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
    const pkIndex = this.pkIndexes.get(model)!;
    const pkValue = this.getPrimaryKeyString(model, data);

    if (pkIndex.has(pkValue)) {
      throw new Error(`Record with primary key ${pkValue} already exists in ${model}`);
    }

    const record: RowData = Object.assign({}, data);
    const heap = this.tables.get(model)!;

    // Add to storage
    const index = heap.length;
    heap.push(record);
    pkIndex.set(pkValue, record);
    this.indexMap.set(record, index);

    // Add to global LRU for eviction tracking
    this.globalLRU.set(record, model);

    return Promise.resolve(this.applySelect(record, select));
  }

  find<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null> {
    const { model, where, select } = args;

    // Fast path: PK lookup
    const primaryKeyFields = getPrimaryKeyFields(this.schema[model]!);
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- checking for field-based clause
    const w = where as { field?: string; op?: string; value?: unknown };
    if (
      w.field !== undefined &&
      primaryKeyFields.length === 1 &&
      w.field === primaryKeyFields[0] &&
      w.op === "eq"
    ) {
      const pkValue = String(w.value);
      const row = this.pkIndexes.get(model)!.get(pkValue);
      if (row && this.matchesWhere(where, row)) {
        this.globalLRU.get(row); // Touch for LRU
        return Promise.resolve(this.applySelect(row, select));
      }
    }

    const heap = this.tables.get(model)!;
    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        this.globalLRU.get(value); // Touch for LRU
        return Promise.resolve(this.applySelect(value, select));
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
    const heap = this.tables.get(model)!;

    const results: RowData[] = [];
    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        results.push(value);
      }
    }

    let out: RowData[] = results;
    if (cursor !== undefined) {
      out = this.applyCursor(out, cursor, sortBy);
    }

    if (sortBy !== undefined && sortBy.length > 0) {
      out = this.applySort(out, sortBy);
    }

    const start = offset ?? 0;
    const end = limit === undefined ? out.length : start + limit;
    const final: T[] = [];
    for (let i = start; i < end && i < out.length; i++) {
      const r = out[i]!;
      this.globalLRU.get(r); // Touch for LRU
      final.push(this.applySelect<T>(r, select));
    }
    return Promise.resolve(final);
  }

  /**
   * Updates the first record matching the criteria. Primary key updates are rejected.
   */
  update<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null> {
    const { model, where, data } = args;
    assertNoPrimaryKeyUpdates(this.schema[model]!, data);
    const heap = this.tables.get(model)!;

    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        const updated: RowData = Object.assign(value, data);
        this.globalLRU.set(updated, model); // Update in LRU
        return Promise.resolve(this.applySelect<T>(updated));
      }
    }
    return Promise.resolve(null);
  }

  /**
   * Updates all records matching the criteria. Primary key updates are rejected.
   */
  updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model, where, data } = args;
    assertNoPrimaryKeyUpdates(this.schema[model]!, data);
    const heap = this.tables.get(model)!;

    let count = 0;
    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        Object.assign(value, data);
        this.globalLRU.set(value, model); // Update in LRU
        count++;
      }
    }
    return Promise.resolve(count);
  }

  /**
   * Performs an atomic insert-or-update.
   *
   * Conflicts are always handled on the Primary Key. If `where` is provided, the record
   * is only updated if the condition is met (acting as a predicate). Primary key
   * updates are rejected.
   */
  upsert<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  }): Promise<T> {
    const { model, create, update, where, select } = args;
    const pkValue = this.getPrimaryKeyString(model, create);
    const existing = this.pkIndexes.get(model)!.get(pkValue);

    if (existing !== undefined) {
      if (this.matchesWhere(where, existing)) {
        const updated: RowData = Object.assign(existing, update);
        this.globalLRU.set(updated, model);
        return Promise.resolve(this.applySelect(updated, select));
      }
      this.globalLRU.get(existing);
      return Promise.resolve(this.applySelect(existing, select));
    }

    return this.create({ model, data: create, select });
  }

  delete<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void> {
    const { model, where } = args;
    const heap = this.tables.get(model)!;

    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        this.globalLRU.delete(value);
        this.removeFromTable(value, model);
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
    const heap = this.tables.get(model)!;
    const toDelete: RowData[] = [];

    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        toDelete.push(value);
      }
    }
    for (let i = 0; i < toDelete.length; i++) {
      const row = toDelete[i]!;
      this.globalLRU.delete(row);
      this.removeFromTable(row, model);
    }
    return Promise.resolve(toDelete.length);
  }

  count<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    const heap = this.tables.get(model)!;

    if (where === undefined) {
      return Promise.resolve(heap.length);
    }

    let count = 0;
    for (let i = 0; i < heap.length; i++) {
      if (this.matchesWhere(where, heap[i]!)) count++;
    }
    return Promise.resolve(count);
  }

  // --- Private helpers ---

  private removeFromTable(row: RowData, model: keyof S & string) {
    const heap = this.tables.get(model);
    const pkIndex = this.pkIndexes.get(model);
    if (!heap || !pkIndex) return;

    const idx = this.indexMap.get(row);
    if (idx === undefined) return;

    // Swap-and-pop
    const lastRow = heap.at(-1)!;
    heap[idx] = lastRow;
    this.indexMap.set(lastRow, idx);
    heap.pop();

    // Cleanup indexes
    this.indexMap.delete(row);
    const pkValue = this.getPrimaryKeyString(model, row);
    pkIndex.delete(pkValue);
  }

  private getPrimaryKeyString(modelName: string, data: Record<string, unknown>): string {
    const modelSpec = this.schema[modelName as keyof S & string]!;
    const primaryKeyValues = getPrimaryKeyValues(modelSpec, data);
    const primaryKeyFields = getPrimaryKeyFields(modelSpec);
    let res = "";
    for (let i = 0; i < primaryKeyFields.length; i++) {
      if (i > 0) res += "|";
      const val = primaryKeyValues[primaryKeyFields[i]!];
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

  private matchesWhere<T extends Record<string, unknown>>(
    where: Where<T> | undefined,
    record: RowData,
  ): boolean {
    if (where === undefined) return true;
    return this.evaluateWhere(where, record);
  }

  private evaluateWhere<T extends Record<string, unknown>>(
    where: Where<T>,
    record: RowData,
  ): boolean {
    if ("and" in where) {
      const and = (where as { and: Where<T>[] }).and;
      for (let i = 0; i < and.length; i++) {
        if (!this.evaluateWhere(and[i]!, record)) return false;
      }
      return true;
    }
    if ("or" in where) {
      const or = (where as { or: Where<T>[] }).or;
      for (let i = 0; i < or.length; i++) {
        if (this.evaluateWhere(or[i]!, record)) return true;
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

  private applySelect<T extends Record<string, unknown>>(record: RowData, select?: Select<T>): T {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- record matches shape of T
    if (select === undefined) return Object.assign({}, record) as T;
    const res: RowData = {};
    for (let i = 0; i < select.length; i++) {
      const k = select[i]!;
      res[k as string] = record[k as string] ?? null;
    }
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- projection matches T
    return res as T;
  }

  private applyCursor<T extends Record<string, unknown>>(
    results: RowData[],
    cursor: Cursor<T>,
    sortBy?: SortBy<T>[],
  ): RowData[] {
    const paginationWhere = getPaginationFilter(cursor, sortBy);
    if (!paginationWhere) return results;

    const filtered: RowData[] = [];
    for (let i = 0; i < results.length; i++) {
      const record = results[i]!;
      if (this.evaluateWhere(paginationWhere, record)) {
        filtered.push(record);
      }
    }
    return filtered;
  }

  private applySort<T extends Record<string, unknown>>(
    results: RowData[],
    sortBy: SortBy<T>[],
  ): RowData[] {
    const sorted = results.slice();
    // eslint-disable-next-line unicorn/no-array-sort -- sorting a shallow copy
    sorted.sort((a, b) => {
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
    return sorted;
  }
}

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
