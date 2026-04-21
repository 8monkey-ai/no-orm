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
} from "./common";

type Comparable = string | number;
type RowData = Record<string, unknown>;

/**
 * A zero-dependency, in-memory implementation of the no-orm Adapter interface.
 * Useful for testing, development, and small-scale caching.
 */
export class MemoryAdapter<S extends Schema = Schema> implements Adapter<S> {
  private storage = new Map<string, Map<string, RowData>>();

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
    const modelStorage = this.getModelStorage(model);
    const pkValue = this.getPrimaryKeyString(model, data);

    if (modelStorage.has(pkValue)) {
      throw new Error(`Record with primary key ${pkValue} already exists in ${model}`);
    }

    // Optimization: Avoid object spread in hot path
    const record = Object.assign({}, data);
    modelStorage.set(pkValue, record);

    return Promise.resolve(this.applySelect(this.asModel(record), select));
  }

  find<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null> {
    const { model, where, select } = args;
    const modelStorage = this.getModelStorage(model);

    for (const record of modelStorage.values()) {
      const modelRecord = this.asModel<T>(record);
      if (this.evaluateWhere(where, modelRecord)) {
        return Promise.resolve(this.applySelect(modelRecord, select));
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
    const modelStorage = this.getModelStorage(model);

    let results = Array.from(modelStorage.values()).map((r) => this.asModel<T>(r));

    if (where) {
      results = results.filter((record) => this.evaluateWhere(where, record));
    }

    if (cursor) {
      const cursorValues = cursor.after;
      const sortCriteria =
        sortBy !== undefined && sortBy.length > 0
          ? sortBy
              .filter((sort) => cursorValues[sort.field] !== undefined)
              .map((sort) => ({
                field: sort.field,
                direction: sort.direction ?? "asc",
                path: sort.path,
              }))
          : this.getFieldNames(cursor.after).map((field) => ({
              field,
              direction: "asc" as const,
              path: undefined,
            }));

      if (sortCriteria.length > 0) {
        results = results.filter((record) => {
          // Lexicographic keyset pagination:
          // (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
          for (const current of sortCriteria) {
            const recordVal = this.getValue(record, current.field, current.path);
            const cursorVal = cursorValues[current.field];
            const comp = this.compareValues(recordVal, cursorVal);

            if (comp === 0) continue;

            return current.direction === "desc" ? comp < 0 : comp > 0;
          }
          return false;
        });
      }
    }

    if (sortBy) {
      results.sort((a, b) => {
        for (const { field, direction, path } of sortBy) {
          const valA = this.getValue(a, field, path);
          const valB = this.getValue(b, field, path);
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
    const modelStorage = this.getModelStorage(model);

    for (const [pk, record] of modelStorage.entries()) {
      const modelRecord = this.asModel<T>(record);
      if (this.evaluateWhere(where, modelRecord)) {
        // Optimization: Create a new object to avoid mutating internal storage reference
        const updated = Object.assign({}, modelRecord, data);
        modelStorage.set(pk, updated);
        return Promise.resolve(this.applySelect(this.asModel(updated)));
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
    const modelStorage = this.getModelStorage(model);
    let count = 0;

    for (const [pk, record] of modelStorage.entries()) {
      const modelRecord = this.asModel<T>(record);
      if (where === undefined || this.evaluateWhere(where, modelRecord)) {
        // Optimization: Create a new object to avoid mutating internal storage reference
        const updated = Object.assign({}, modelRecord, data);
        modelStorage.set(pk, updated);
        count++;
      }
    }

    return Promise.resolve(count);
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
    const modelStorage = this.getModelStorage(model);
    const existing = modelStorage.get(pkValue);

    if (existing !== undefined) {
      const modelExisting = this.asModel<T>(existing);
      // Use optional where predicate
      if (where === undefined || this.evaluateWhere(where, modelExisting)) {
        const updated = Object.assign({}, modelExisting, update);
        modelStorage.set(pkValue, updated);
        return Promise.resolve(this.applySelect(this.asModel(updated), select));
      }
      return Promise.resolve(this.applySelect(modelExisting, select));
    }

    return this.create({ model, data: create, select });
  }

  delete<K extends keyof S & string, T extends Record<string, unknown> = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void> {
    const { model, where } = args;
    const modelStorage = this.getModelStorage(model);

    for (const [pk, record] of modelStorage.entries()) {
      if (this.evaluateWhere(where, this.asModel<T>(record))) {
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
    const modelStorage = this.getModelStorage(model);
    let count = 0;

    for (const [pk, record] of modelStorage.entries()) {
      if (where === undefined || this.evaluateWhere(where, this.asModel<T>(record))) {
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
    const modelStorage = this.getModelStorage(model);

    if (where === undefined) return Promise.resolve(modelStorage.size);

    let count = 0;
    for (const record of modelStorage.values()) {
      if (this.evaluateWhere(where, this.asModel<T>(record))) {
        count++;
      }
    }
    return Promise.resolve(count);
  }

  // --- Helpers ---

  private getFieldNames<T>(obj: Partial<Record<FieldName<T>, unknown>>): FieldName<T>[] {
    // Object.keys always returns string[], narrowing to FieldName<T> is safe - keys come from typed cursor
    return Object.keys(obj) as FieldName<T>[]; // eslint-disable-line @typescript-eslint/no-unsafe-type-assertion
  }

  private asModel<T extends RowData>(record: RowData): T {
    // Internal storage only contains RowData from previous operations; T is inferred from call site
    return record as T; // eslint-disable-line @typescript-eslint/no-unsafe-type-assertion
  }

  private getModelStorage(model: string): Map<string, RowData> {
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
    if (select === undefined) {
      return Object.assign({}, record);
    }

    // Empty object to be populated with selected fields only
    const result = {} as T; // eslint-disable-line @typescript-eslint/no-unsafe-type-assertion
    for (const field of select) {
      const fieldName = field as FieldName<T>;
      const val = record[fieldName];
      this.setField(result, fieldName, val ?? null);
    }

    return result;
  }

  private setField<T extends RowData, K extends FieldName<T>>(obj: T, key: K, value: unknown): void {
    // Value is either from the record or defaulted to null - both are valid for T[K]
    obj[key] = value as T[K]; // eslint-disable-line @typescript-eslint/no-unsafe-type-assertion
  }

  private getValue<T extends RowData>(record: T, field: FieldName<T>, path?: string[]): unknown {
    let value: unknown = record[field];
    if (path && path.length > 0) {
      for (const segment of path) {
        if (!isRecord(value)) {
          return undefined;
        }
        value = value[segment];
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

    if ("field" in where && "op" in where) {
      const { field, op, value, path } = where;
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
      }
    }
    return false;
  }

  private compareValues(left: unknown, right: unknown): number {
    if (left === right) return 0;
    if (left === null || left === undefined) return -1;
    if (right === null || right === undefined) return 1;
    if (typeof left !== typeof right) return 0;
    if (this.isComparable(left) && this.isComparable(right)) {
      if (left < right) return -1;
      if (left > right) return 1;
    }
    return 0;
  }

  private isComparable(value: unknown): value is Comparable {
    return typeof value === "string" || typeof value === "number";
  }
}
