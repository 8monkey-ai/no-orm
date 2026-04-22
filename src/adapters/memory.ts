import type {
  Adapter,
  Cursor,
  FieldName,
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
  mapNumeric,
} from "./common";

type Comparable = string | number;
type RowData = Record<string, unknown>;

let LRU: any;
const lruPromise = import("lru-cache")
  .then((m) => {
    LRU = m.LRUCache;
  })
  .catch(() => {});

export interface MemoryAdapterOptions {
  maxSize?: number;
}

export class MemoryAdapter<S extends Schema = Schema> implements Adapter<S> {
  private storage = new Map<string, any>();
  private options: MemoryAdapterOptions;

  constructor(
    private schema: S,
    options: MemoryAdapterOptions = {},
  ) {
    this.options = options;
  }

  async migrate(): Promise<void> {
    await lruPromise;
    const keys = Object.keys(this.schema);
    for (let i = 0; i < keys.length; i++) {
      const name = keys[i]!;
      if (!this.storage.has(name)) {
        if (this.options.maxSize && LRU) {
          this.storage.set(name, new LRU({ max: this.options.maxSize }));
        } else {
          this.storage.set(name, new Map());
        }
      }
    }
  }

  async transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return fn(this);
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    const { model, data, select } = args;
    const modelStorage = this.getModelStorage(model);
    const pkValue = this.getPrimaryKeyString(model, data);

    if (modelStorage.has(pkValue)) {
      throw new Error(`Record with primary key ${pkValue} already exists in ${model}`);
    }

    const record = Object.assign({}, data);
    modelStorage.set(pkValue, record);

    return this.applySelect(record as any, select);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model, where, select } = args;
    const modelStorage = this.getModelStorage(model);

    const values = Array.from(modelStorage.values());
    for (let i = 0; i < values.length; i++) {
      const record = values[i] as T;
      if (this.evaluateWhere(where, record)) {
        return this.applySelect(record, select);
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
    const { model, where, select, sortBy, limit, offset, cursor } = args;
    const modelStorage = this.getModelStorage(model);

    let results: T[] = [];
    const rawValues = Array.from(modelStorage.values());
    for (let i = 0; i < rawValues.length; i++) {
      const record = rawValues[i] as T;
      if (!where || this.evaluateWhere(where, record)) {
        results.push(record);
      }
    }

    if (cursor) {
      const cursorValues = cursor.after as Record<string, unknown>;
      const criteria = [];
      if (sortBy && sortBy.length > 0) {
        for (let i = 0; i < sortBy.length; i++) {
          const s = sortBy[i]!;
          if (cursorValues[s.field] !== undefined) {
            criteria.push({ field: s.field, direction: s.direction ?? "asc", path: s.path });
          }
        }
      } else {
        const keys = Object.keys(cursorValues);
        for (let i = 0; i < keys.length; i++) {
          criteria.push({ field: keys[i]!, direction: "asc" as const, path: undefined });
        }
      }

      if (criteria.length > 0) {
        const filtered: T[] = [];
        for (let i = 0; i < results.length; i++) {
          const record = results[i]!;
          let match = false;
          // Lexicographic keyset pagination:
          // (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
          for (let j = 0; j < criteria.length; j++) {
            const curr = criteria[j]!;
            const recordVal = this.getValue(record, curr.field as any, curr.path);
            const cursorVal = cursorValues[curr.field as any];
            const comp = this.compareValues(recordVal, cursorVal);

            if (comp === 0) continue;
            if (curr.direction === "desc" ? comp < 0 : comp > 0) {
              match = true;
            }
            break;
          }
          if (match) filtered.push(record);
        }
        results = filtered;
      }
    }

    if (sortBy && sortBy.length > 0) {
      results.sort((a, b) => {
        for (let i = 0; i < sortBy.length; i++) {
          const { field, direction, path } = sortBy[i]!;
          const valA = this.getValue(a, field, path);
          const valB = this.getValue(b, field, path);
          if (valA === valB) continue;
          const comparison = this.compareValues(valA, valB);
          if (comparison === 0) continue;
          return direction === "desc" ? -comparison : comparison;
        }
        return 0;
      });
    }

    const start = offset ?? 0;
    const end = limit === undefined ? results.length : start + limit;
    const finalResults: T[] = [];
    for (let i = start; i < end && i < results.length; i++) {
      finalResults.push(this.applySelect(results[i]!, select));
    }

    return finalResults;
  }

  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; data: Partial<T> }): Promise<T | null> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);
    const modelStorage = this.getModelStorage(model);

    const entries = Array.from(modelStorage.entries());
    for (let i = 0; i < entries.length; i++) {
      const [pk, record] = entries[i]!;
      const modelRecord = record as T;
      if (this.evaluateWhere(where, modelRecord)) {
        const updated = Object.assign({}, modelRecord, data);
        modelStorage.set(pk, updated);
        return this.applySelect(updated as any);
      }
    }

    return null;
  }

  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);
    const modelStorage = this.getModelStorage(model);
    let count = 0;

    const entries = Array.from(modelStorage.entries());
    for (let i = 0; i < entries.length; i++) {
      const [pk, record] = entries[i]!;
      const modelRecord = record as T;
      if (where === undefined || this.evaluateWhere(where, modelRecord)) {
        const updated = Object.assign({}, modelRecord, data);
        modelStorage.set(pk, updated);
        count++;
      }
    }

    return count;
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
    const { model, create, update, where, select } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, update);

    const pkValue = this.getPrimaryKeyString(model, create);
    const modelStorage = this.getModelStorage(model);
    const existing = modelStorage.get(pkValue);

    if (existing !== undefined) {
      const modelExisting = existing as T;
      if (where === undefined || this.evaluateWhere(where, modelExisting)) {
        const updated = Object.assign({}, modelExisting, update);
        modelStorage.set(pkValue, updated);
        return this.applySelect(updated as any, select);
      }
      return this.applySelect(modelExisting, select);
    }

    return this.create({ model, data: create, select });
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);

    const entries = Array.from(modelStorage.entries());
    for (let i = 0; i < entries.length; i++) {
      const [pk, record] = entries[i]!;
      if (this.evaluateWhere(where, record as T)) {
        modelStorage.delete(pk);
        return;
      }
    }
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);
    let count = 0;

    const entries = Array.from(modelStorage.entries());
    for (let i = 0; i < entries.length; i++) {
      const [pk, record] = entries[i]!;
      if (where === undefined || this.evaluateWhere(where, record as T)) {
        modelStorage.delete(pk);
        count++;
      }
    }

    return count;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);

    if (where === undefined) return modelStorage.size;

    let count = 0;
    const values = Array.from(modelStorage.values());
    for (let i = 0; i < values.length; i++) {
      if (this.evaluateWhere(where, values[i] as T)) {
        count++;
      }
    }
    return count;
  }

  private getModelStorage(model: string): any {
    const storage = this.storage.get(model);
    if (!storage) throw new Error(`Model ${model} not initialized. Call migrate() first.`);
    return storage;
  }

  private getModel(model: string): S[keyof S] {
    const spec = this.schema[model];
    if (!spec) throw new Error(`Model ${model} not found in schema`);
    return spec;
  }

  private getPrimaryKeyString(modelName: string, data: Record<string, unknown>): string {
    const model = this.getModel(modelName);
    const pkValues = getIdentityValues(model, data);
    const pkFields = getPrimaryKeyFields(model);
    let res = "";
    for (let i = 0; i < pkFields.length; i++) {
      if (i > 0) res += "|";
      res += String(pkValues[pkFields[i]!] ?? "");
    }
    return res;
  }

  private applySelect<T extends RowData>(record: T, select?: Select<T>): T {
    if (!select) return Object.assign({}, record);
    const res = {} as T;
    for (let i = 0; i < select.length; i++) {
      const k = select[i]! as string;
      (res as any)[k] = record[k] ?? null;
    }
    return res;
  }

  private getValue(record: any, field: string, path?: string[]): unknown {
    let val = record[field];
    if (path && path.length > 0) {
      for (let i = 0; i < path.length; i++) {
        if (!isRecord(val)) return undefined;
        val = val[path[i]!];
      }
    }
    return val;
  }

  private evaluateWhere(where: Where<any>, record: any): boolean {
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

    const { field, op, value, path } = where as any;
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
    if (left === null || left === undefined) return -1;
    if (right === null || right === undefined) return 1;
    if (typeof left !== typeof right) return 0;
    if (typeof left === "string" || typeof left === "number") {
      if (left < (right as any)) return -1;
      if (left > (right as any)) return 1;
    }
    return 0;
  }
}
