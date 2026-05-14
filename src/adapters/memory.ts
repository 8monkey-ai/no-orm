import { LRUCache } from "lru-cache";

import type { Adapter, Cursor, FieldName, InferModel, Schema, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  getNestedValue,
  getPaginationFilter,
  getPrimaryKeyFieldNames,
  getPrimaryKeyValues,
  walkWhere,
  type Project,
  type RowData,
} from "./utils/common";

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
  private tables = new Map<keyof S & string, RowData[]>();
  private pkIndexes = new Map<keyof S & string, Map<string, RowData>>();
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

    const keys = Object.keys(this.schema) as (keyof S & string)[];
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
    const snapshot = new Map<keyof S & string, RowData[]>();
    for (const [model, rows] of this.tables) {
      snapshot.set(
        model,
        rows.map((row) => structuredClone(row)),
      );
    }

    return fn(this).catch((err: unknown) => {
      for (const [model] of this.tables) {
        this.tables.get(model)!.length = 0;
        this.pkIndexes.get(model)!.clear();
      }
      this.indexMap.clear();
      this.globalLRU.clear();

      for (const [model, rows] of snapshot) {
        const heap = this.tables.get(model)!;
        const pkIndex = this.pkIndexes.get(model)!;
        for (const row of rows) {
          const idx = heap.length;
          heap.push(row);
          pkIndex.set(this.getPrimaryKeyHash(model, row), row);
          this.indexMap.set(row, idx);
          this.globalLRU.set(row, model);
        }
      }

      throw err;
    });
  }

  create<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    data: InferModel<S[K]>;
    select?: readonly F[];
  }): Promise<[F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>> {
    type Row = InferModel<S[K]>;
    const { model, data, select } = args;
    this.assertNoUnknownFields(model, data as Record<string, unknown>);
    const pkIndex = this.pkIndexes.get(model)!;
    const pkValue = this.getPrimaryKeyHash(model, data as Record<string, unknown>);

    if (pkIndex.has(pkValue)) {
      return Promise.reject(
        new Error(`Record with primary key ${pkValue} already exists in ${model}`),
      );
    }

    const record: RowData = Object.assign({}, data);
    const heap = this.tables.get(model)!;

    const index = heap.length;
    heap.push(record);
    pkIndex.set(pkValue, record);
    this.indexMap.set(record, index);
    this.globalLRU.set(record, model);

    return Promise.resolve(this.mapFromRecord<Row, F>(record, select));
  }

  find<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    where: Where<InferModel<S[K]>>;
    select?: readonly F[];
  }): Promise<([F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>) | null> {
    type Row = InferModel<S[K]>;
    const { model, where, select } = args;

    // Fast path: PK lookup
    const primaryKeyFieldNames = getPrimaryKeyFieldNames(this.schema[model]!);
    if (
      "field" in where &&
      primaryKeyFieldNames.length === 1 &&
      where.field === primaryKeyFieldNames[0] &&
      where.op === "eq"
    ) {
      const pkValue = JSON.stringify([where.value ?? null]);
      const row = this.pkIndexes.get(model)!.get(pkValue);
      if (row && this.matchesWhere(where, row)) {
        this.globalLRU.get(row); // Touch for LRU
        return Promise.resolve(this.mapFromRecord<Row, F>(row, select));
      }
    }

    const heap = this.tables.get(model)!;
    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        this.globalLRU.get(value); // Touch for LRU
        return Promise.resolve(this.mapFromRecord<Row, F>(value, select));
      }
    }
    return Promise.resolve(null);
  }

  findMany<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
    select?: readonly F[];
    sortBy?: SortBy<InferModel<S[K]>>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<InferModel<S[K]>>;
  }): Promise<([F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>)[]> {
    type Row = InferModel<S[K]>;
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
      out = this.filterByCursor(out, cursor, sortBy);
    }

    if (sortBy !== undefined && sortBy.length > 0) {
      out = this.applySort(out, sortBy);
    }

    const start = offset ?? 0;
    const end = limit === undefined ? out.length : start + limit;
    const final: Project<Row, F>[] = [];
    for (let i = start; i < end && i < out.length; i++) {
      const r = out[i]!;
      this.globalLRU.get(r); // Touch for LRU
      final.push(this.mapFromRecord<Row, F>(r, select));
    }
    return Promise.resolve(final);
  }

  private definedPatch(data: Record<string, unknown>): RowData {
    const patch: RowData = {};
    const keys = Object.keys(data);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!;
      if (data[key] !== undefined) patch[key] = data[key];
    }
    return patch;
  }

  /**
   * Updates the first record matching the criteria. Primary key updates are rejected.
   */
  update<K extends keyof S & string>(args: {
    model: K;
    where: Where<InferModel<S[K]>>;
    data: Partial<InferModel<S[K]>>;
  }): Promise<InferModel<S[K]> | null> {
    type Row = InferModel<S[K]>;
    const { model, where, data } = args;
    const patch = this.definedPatch(data as Record<string, unknown>);
    assertNoPrimaryKeyUpdates(this.schema[model]!, patch);
    this.assertNoUnknownFields(model, patch);
    const heap = this.tables.get(model)!;

    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        const updated: RowData = Object.assign(value, patch);
        this.globalLRU.get(updated); // Touch for LRU
        return Promise.resolve(this.mapFromRecord<Row>(updated));
      }
    }
    return Promise.resolve(null);
  }

  /**
   * Updates all records matching the criteria. Primary key updates are rejected.
   */
  updateMany<K extends keyof S & string>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
    data: Partial<InferModel<S[K]>>;
  }): Promise<number> {
    const { model, where, data } = args;
    const patch = this.definedPatch(data as Record<string, unknown>);
    assertNoPrimaryKeyUpdates(this.schema[model]!, patch);
    this.assertNoUnknownFields(model, patch);
    if (Object.keys(patch).length === 0) return Promise.resolve(0);
    const heap = this.tables.get(model)!;

    let count = 0;
    for (let i = 0; i < heap.length; i++) {
      const value = heap[i]!;
      if (this.matchesWhere(where, value)) {
        Object.assign(value, patch);
        this.globalLRU.get(value); // Touch for LRU
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
  upsert<K extends keyof S & string, F extends FieldName<InferModel<S[K]>> = never>(args: {
    model: K;
    create: InferModel<S[K]>;
    update: Partial<InferModel<S[K]>>;
    where?: Where<InferModel<S[K]>>;
    select?: readonly F[];
  }): Promise<[F] extends [never] ? InferModel<S[K]> : Pick<InferModel<S[K]>, F>> {
    type Row = InferModel<S[K]>;
    const { model, create, update, where, select } = args;
    const patch = this.definedPatch(update as Record<string, unknown>);
    assertNoPrimaryKeyUpdates(this.schema[model]!, patch);
    this.assertNoUnknownFields(model, create as Record<string, unknown>);
    this.assertNoUnknownFields(model, patch);
    const pkValue = this.getPrimaryKeyHash(model, create as Record<string, unknown>);
    const existing = this.pkIndexes.get(model)!.get(pkValue);

    if (existing !== undefined) {
      if (this.matchesWhere(where, existing)) {
        const updated: RowData = Object.assign(existing, patch);
        this.globalLRU.get(updated); // Touch for LRU
        return Promise.resolve(this.mapFromRecord<Row, F>(updated, select));
      }
      this.globalLRU.get(existing);
      return Promise.resolve(this.mapFromRecord<Row, F>(existing, select));
    }

    return this.create({ model, data: create, select });
  }

  delete<K extends keyof S & string>(args: {
    model: K;
    where: Where<InferModel<S[K]>>;
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

  deleteMany<K extends keyof S & string>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
  }): Promise<number> {
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

  count<K extends keyof S & string>(args: {
    model: K;
    where?: Where<InferModel<S[K]>>;
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

  private assertNoUnknownFields(model: keyof S & string, data: Record<string, unknown>): void {
    const knownFields = this.schema[model]!.fields;
    for (const key of Object.keys(data)) {
      if (!(key in knownFields)) {
        throw new Error(`Unknown field "${key}" in model "${model}"`);
      }
    }
  }

  private removeFromTable(row: RowData, model: keyof S & string) {
    const heap = this.tables.get(model);
    const pkIndex = this.pkIndexes.get(model);
    if (!heap || !pkIndex) return;

    const idx = this.indexMap.get(row);
    if (idx === undefined) return;

    // Swap-and-pop
    if (idx !== heap.length - 1) {
      const lastRow = heap.at(-1)!;
      heap[idx] = lastRow;
      this.indexMap.set(lastRow, idx);
    }
    heap.pop();

    this.indexMap.delete(row);
    const pkValue = this.getPrimaryKeyHash(model, row);
    pkIndex.delete(pkValue);
  }

  private getPrimaryKeyHash(modelName: keyof S & string, data: Record<string, unknown>): string {
    const modelSpec = this.schema[modelName]!;
    const primaryKeyValues = getPrimaryKeyValues(modelSpec, data);
    const primaryKeyFieldNames = getPrimaryKeyFieldNames(modelSpec);
    const tuple: unknown[] = [];
    for (let i = 0; i < primaryKeyFieldNames.length; i++) {
      tuple.push(primaryKeyValues[primaryKeyFieldNames[i]!] ?? null);
    }
    return JSON.stringify(tuple);
  }

  private matchesWhere<T extends Record<string, unknown>>(
    where: Where<T> | undefined,
    record: RowData,
  ): boolean {
    if (where === undefined) return true;
    return walkWhere<boolean, T>(where, {
      and: (children) => children.every(Boolean),
      or: (children) => children.some(Boolean),
      leaf: (c) => {
        const recordVal = getNestedValue(record, c.field, c.path);
        const opStr: string = c.op;
        switch (c.op) {
          case "eq":
            return recordVal === c.value;
          case "ne":
            return recordVal !== c.value;
          case "gt":
            return compareValues(recordVal, c.value) > 0;
          case "gte":
            return compareValues(recordVal, c.value) >= 0;
          case "lt":
            return compareValues(recordVal, c.value) < 0;
          case "lte":
            return compareValues(recordVal, c.value) <= 0;
          case "in":
            return c.value.includes(recordVal);
          case "not_in":
            return !c.value.includes(recordVal);
          default:
            throw new Error(`Unsupported operator: ${opStr}`);
        }
      },
    });
  }

  private mapFromRecord<T extends RowData, F extends FieldName<T> = never>(
    record: RowData,
    select?: readonly F[],
  ): Project<T, F> {
    let res: RowData;
    if (select === undefined) {
      res = Object.assign({}, record);
    } else {
      res = {};
      for (let i = 0; i < select.length; i++) {
        const k = select[i]!;
        res[k] = record[k] ?? null;
      }
    }
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- adapter storage rows match the requested schema row or projection type
    return res as Project<T, F>;
  }

  private filterByCursor<T extends RowData>(
    results: RowData[],
    cursor: Cursor<T>,
    sortBy?: SortBy<T>[],
  ): RowData[] {
    const paginationWhere = getPaginationFilter(cursor, sortBy);
    if (!paginationWhere) return results;

    const filtered: RowData[] = [];
    for (let i = 0; i < results.length; i++) {
      const record = results[i]!;
      if (this.matchesWhere(paginationWhere, record)) {
        filtered.push(record);
      }
    }
    return filtered;
  }

  private applySort<T extends RowData>(results: RowData[], sortBy: SortBy<T>[]): RowData[] {
    const sorted = results.slice();
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
