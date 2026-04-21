import type { Database as BunDatabase } from "bun:sqlite";

import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type { Adapter, Cursor, Field, InferModel, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildIdentityFilter,
  getIdentityValues,
  getPrimaryKeyFields,
  isRecord,
  JSON_PATH_INDEX,
  mapNumeric,
  quote,
  validateJsonPath,
} from "./common";

export type SqliteValue = string | number | Uint8Array | null;

/**
 * Clean interface for SQLite execution.
 */
interface SqliteExecutor {
  run(sql: string, params: SqliteValue[]): Promise<{ changes: number }>;
  get(sql: string, params: SqliteValue[]): Promise<Record<string, unknown> | null>;
  all(sql: string, params: SqliteValue[]): Promise<Record<string, unknown>[]>;
}

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database | SqliteExecutor;

/**
 * Internal interface for synchronous SQLite drivers.
 * Both Bun and Better-Sqlite3 expose `prepare` which returns a statement with run/get/all.
 */
interface SyncStatement {
  run(...params: unknown[]): unknown;
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown;
}

interface SyncDriver {
  prepare(sql: string): SyncStatement;
}

/**
 * Helper to wrap synchronous statements and handle casting.
 * All environment sniffing and "any" usage is localized here.
 */
function createExecutor(driver: SqliteDriver): SqliteExecutor {
  // If it's already an executor, return it
  if (objIsObject(driver) && "run" in driver && "get" in driver && "all" in driver) {
    // Already implements SqliteExecutor interface
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    return driver as SqliteExecutor;
  }

  // Sniff for Sync Drivers (Bun or Better-Sqlite3)
  const isSync = checkIsSyncDriver(driver);

  if (isSync) {
    // Duck-typing check: runtime inspection cannot narrow TypeScript union types
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    const syncDb = driver as SyncDriver;
    return {
      run: (sql, params) => {
        const stmt = syncDb.prepare(sql);
        const result = stmt.run(...params);
        return Promise.resolve({
          changes:
            isRecord(result) && typeof result["changes"] === "number" ? result["changes"] : 0,
        });
      },
      get: (sql, params) => {
        const stmt = syncDb.prepare(sql);
        const row = stmt.get(...params);
        return Promise.resolve(isRecord(row) ? row : null);
      },
      all: (sql, params) => {
        const stmt = syncDb.prepare(sql);
        const rows = stmt.all(...params);
        return Promise.resolve(
          Array.isArray(rows)
            ? rows.filter((item): item is Record<string, unknown> => isRecord(item))
            : [],
        );
      },
    };
  }

  // Otherwise assume it matches SqliteDatabase (async sqlite driver)
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
  return driver as SqliteExecutor;
}

function objIsObject(obj: unknown): obj is Record<string, unknown> {
  return obj !== null && typeof obj === "object";
}

function checkIsSyncDriver(obj: unknown): boolean {
  if (!objIsObject(obj)) return false;
  if (!("prepare" in obj) || typeof obj["prepare"] !== "function") return false;
  const hasAsyncGet = "get" in obj && typeof obj["get"] === "function";
  if (hasAsyncGet) {
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    const getFn = obj["get"] as { constructor: { name: string } };
    return getFn.constructor.name !== "AsyncFunction";
  }
  return true;
}

export class SqliteAdapter<S extends Schema = Schema> implements Adapter<S> {
  private executor: SqliteExecutor;
  private savepointCounter = 0;
  // Top-level SQLite transactions on one shared connection must be serialized.
  private transactionQueue = Promise.resolve();
  private isTransaction = false;

  constructor(
    private schema: S,
    driver: SqliteDriver,
    _isTransaction = false,
  ) {
    this.executor = createExecutor(driver);
    this.isTransaction = _isTransaction;
  }

  async migrate(): Promise<void> {
    for (const [name, model] of Object.entries(this.schema)) {
      const columns = Object.entries(model.fields).map(([fieldName, field]) => {
        const nullable = field.nullable === true ? "" : " NOT NULL";
        return `${quote(fieldName)} ${this.mapFieldType(field)}${nullable}`;
      });

      const pk = `PRIMARY KEY (${getPrimaryKeyFields(model)
        .map((field) => quote(field))
        .join(", ")})`;

      // oxlint-disable-next-line eslint/no-await-in-loop
      await this.executor.run(
        `CREATE TABLE IF NOT EXISTS ${quote(name)} (${columns.join(", ")}, ${pk})`,
        [],
      );

      if (model.indexes === undefined) continue;

      for (let i = 0; i < model.indexes.length; i++) {
        const index = model.indexes[i];
        if (index === undefined) continue;

        const fields = (Array.isArray(index.field) ? index.field : [index.field])
          .map((field) => `${quote(field)}${index.order ? ` ${index.order.toUpperCase()}` : ""}`)
          .join(", ");

        // oxlint-disable-next-line eslint/no-await-in-loop
        await this.executor.run(
          `CREATE INDEX IF NOT EXISTS ${quote(`idx_${name}_${i}`)} ON ${quote(name)} (${fields})`,
          [],
        );
      }
    }
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    const { model, data, select } = args;
    const modelSpec = this.getModel(model);
    const mappedData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(mappedData);
    const placeholders = fields.map(() => "?").join(", ");
    const columns = fields.map((field) => quote(field)).join(", ");
    const params = fields.map((field) => this.toSqliteValue(mappedData[field]));

    await this.executor.run(
      `INSERT INTO ${quote(model)} (${columns}) VALUES (${placeholders})`,
      params,
    );

    const result = await this.find<K, T>({
      model,
      where: buildIdentityFilter(modelSpec, data),
      select,
    });

    if (result === null) {
      throw new Error("Failed to refetch created record.");
    }
    return result;
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model, where, select } = args;
    const builtWhere = this.buildWhere(model, where);
    const sql = `SELECT ${this.buildSelect(select)} FROM ${quote(model)} WHERE ${builtWhere.sql} LIMIT 1`;
    const row = await this.executor.get(sql, builtWhere.params);
    return row === null ? null : this.mapRow(model, row, select);
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
    const parts: string[] = [`SELECT ${this.buildSelect(select)} FROM ${quote(model)}`];
    const params: SqliteValue[] = [];

    if (where !== undefined || cursor !== undefined) {
      const builtWhere = this.buildWhere(model, where, cursor, sortBy);
      parts.push(`WHERE ${builtWhere.sql}`);
      params.push(...builtWhere.params);
    }

    if (sortBy !== undefined && sortBy.length > 0) {
      const order = sortBy
        .map((sort) => {
          const expr = this.buildColumnExpr(model, sort.field as string, sort.path);
          return `${expr} ${(sort.direction ?? "asc").toUpperCase()}`;
        })
        .join(", ");
      parts.push(`ORDER BY ${order}`);
    }

    if (limit !== undefined) {
      parts.push("LIMIT ?");
      params.push(limit);
    }

    if (offset !== undefined) {
      parts.push("OFFSET ?");
      params.push(offset);
    }

    const rows = await this.executor.all(parts.join(" "), params);
    return rows.map((row) => this.mapRow(model, row, select));
  }

  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; data: Partial<T> }): Promise<T | null> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);

    const existing = await this.find<K, T>({ model, where });
    if (existing === null) {
      return null;
    }

    const mappedData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(mappedData);
    if (fields.length === 0) {
      return existing;
    }

    const assignments = fields.map((field) => `${quote(field)} = ?`).join(", ");
    const params = fields.map((field) => this.toSqliteValue(mappedData[field]));
    const primaryKeyWhere: Where<T> = buildIdentityFilter(modelSpec, existing);
    const builtWhere = this.buildWhere(model, primaryKeyWhere);

    await this.executor.run(`UPDATE ${quote(model)} SET ${assignments} WHERE ${builtWhere.sql}`, [
      ...params,
      ...builtWhere.params,
    ]);

    return this.find<K, T>({ model, where: primaryKeyWhere });
  }

  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);

    const mappedData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(mappedData);
    if (fields.length === 0) {
      return 0;
    }

    const assignments = fields.map((field) => `${quote(field)} = ?`).join(", ");
    const params = fields.map((field) => this.toSqliteValue(mappedData[field]));
    let sql = `UPDATE ${quote(model)} SET ${assignments}`;

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const result = await this.executor.run(sql, params);
    return result.changes;
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
    const identityValues = getIdentityValues(modelSpec, create);
    assertNoPrimaryKeyUpdates(modelSpec, update);

    const mappedCreate = this.mapInput(modelSpec.fields, create);
    const createFields = Object.keys(mappedCreate);
    const mappedUpdate = this.mapInput(modelSpec.fields, update);
    const updateFields = Object.keys(mappedUpdate);
    const primaryKeyFields = getPrimaryKeyFields(modelSpec);

    const conflictColumns = primaryKeyFields.map((field) => quote(field)).join(", ");
    const insertColumns = createFields.map((field) => quote(field)).join(", ");
    const insertPlaceholders = createFields.map(() => "?").join(", ");

    let updateClause =
      updateFields.length > 0
        ? updateFields.map((field) => `${quote(field)} = ?`).join(", ")
        : `${quote(primaryKeyFields[0]!)} = excluded.${quote(primaryKeyFields[0]!)}`;

    const params =
      updateFields.length > 0
        ? [
            ...createFields.map((field) => this.toSqliteValue(mappedCreate[field])),
            ...updateFields.map((field) => this.toSqliteValue(mappedUpdate[field])),
          ]
        : createFields.map((field) => this.toSqliteValue(mappedCreate[field]));

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where);
      updateClause += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    await this.executor.run(
      `INSERT INTO ${quote(model)} (${insertColumns}) VALUES (${insertPlaceholders}) ON CONFLICT(${conflictColumns}) DO UPDATE SET ${updateClause}`,
      params,
    );

    const result = await this.find<K, T>({
      model,
      where: buildIdentityFilter(modelSpec, identityValues),
      select,
    });

    if (result === null) {
      throw new Error("Failed to refetch upserted record.");
    }
    return result;
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    const existing = await this.find<K, T>({ model: args.model, where: args.where });
    if (existing === null) {
      return;
    }

    const builtWhere = this.buildWhere(
      args.model,
      buildIdentityFilter(this.getModel(args.model), existing),
    );
    await this.executor.run(
      `DELETE FROM ${quote(args.model)} WHERE ${builtWhere.sql}`,
      builtWhere.params,
    );
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    let sql = `DELETE FROM ${quote(model)}`;
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const result = await this.executor.run(sql, params);
    return result.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    let sql = `SELECT COUNT(*) as count FROM ${quote(model)}`;
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const result = await this.executor.get(sql, params);
    return isRecord(result) && typeof result["count"] === "number" ? result["count"] : 0;
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    if (this.isTransaction) {
      return this.runSavepoint(this.executor, fn);
    }
    return this.withTransactionLock(() => this.runSavepoint(this.executor, fn));
  }

  private async runSavepoint<T>(
    db: SqliteExecutor,
    fn: (tx: Adapter<S>) => Promise<T>,
  ): Promise<T> {
    // Nested transactions stay on the current connection and use savepoints.
    const savepoint = quote(`sp_${this.savepointCounter++}`);
    const txAdapter = new SqliteAdapter(this.schema, db, true);

    await db.run(`SAVEPOINT ${savepoint}`, []);
    try {
      const result = await fn(txAdapter);
      await db.run(`RELEASE SAVEPOINT ${savepoint}`, []);
      return result;
    } catch (error) {
      await db.run(`ROLLBACK TO SAVEPOINT ${savepoint}`, []);
      throw error;
    }
  }

  private async withTransactionLock<T>(fn: () => Promise<T>): Promise<T> {
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const previous = this.transactionQueue;
    this.transactionQueue = previous.then(() => current);

    await previous;
    try {
      return await fn();
    } finally {
      release();
    }
  }

  private getModel<K extends keyof S & string>(model: K): S[K] {
    const modelSpec = this.schema[model];
    if (modelSpec === undefined) {
      throw new Error(`Model ${model} not found in schema.`);
    }
    return modelSpec;
  }

  private mapFieldType(field: Field): string {
    switch (field.type) {
      case "string":
        return field.max === undefined ? "TEXT" : `VARCHAR(${field.max})`;
      case "number":
        return "REAL";
      case "boolean":
      case "timestamp":
        return "INTEGER";
      case "json":
      case "json[]":
        // SQLite has no dedicated JSON column type, so JSON is stored as TEXT.
        return "TEXT";
      default:
        return "TEXT";
    }
  }

  private buildSelect<T>(select?: Select<T>): string {
    return select === undefined ? "*" : select.map((field) => quote(field as string)).join(", ");
  }

  private buildColumnExpr(modelName: string, field: string, path?: string[]): string {
    if (path === undefined || path.length === 0) {
      return quote(field);
    }

    const model = this.schema[modelName as keyof S];
    const fieldSpec = model?.fields[field];
    if (fieldSpec?.type !== "json" && fieldSpec?.type !== "json[]") {
      throw new Error(`Cannot use JSON path on non-JSON field: ${field}`);
    }

    const jsonPath =
      "$" +
      validateJsonPath(path)
        .map((segment) => (JSON_PATH_INDEX.test(segment) ? `[${segment}]` : `.${segment}`))
        .join("");
    return `json_extract(${quote(field)}, '${jsonPath}')`;
  }

  private buildCursor<T>(
    modelName: string,
    cursor: Cursor<T>,
    sortBy?: SortBy<T>[],
  ): { sql: string; params: SqliteValue[] } {
    type CursorSort = {
      field: Extract<keyof T, string>;
      direction: "asc" | "desc";
      path?: string[];
    };
    const cursorValues = cursor.after as Partial<Record<string, unknown>>;
    const sortCriteria: CursorSort[] =
      sortBy !== undefined && sortBy.length > 0
        ? sortBy
            .filter((sort) => cursorValues[sort.field] !== undefined)
            .map((sort) => ({
              field: sort.field,
              direction: sort.direction ?? "asc",
              path: sort.path,
            }))
        : Object.keys(cursor.after).map((field) => ({
            // Cursor keys come from the typed `Cursor<T>` surface.
            // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
            field: field as Extract<keyof T, string>,
            direction: "asc" as const,
            path: undefined,
          }));

    if (sortCriteria.length === 0) {
      return { sql: "", params: [] };
    }

    const orClauses: string[] = [];
    const params: SqliteValue[] = [];

    for (let i = 0; i < sortCriteria.length; i++) {
      const andClauses: string[] = [];

      for (let j = 0; j < i; j++) {
        const previous = sortCriteria[j]!;
        andClauses.push(`${this.buildColumnExpr(modelName, previous.field, previous.path)} = ?`);
        params.push(this.mapWhereValue(cursorValues[previous.field]));
      }

      const current = sortCriteria[i]!;
      andClauses.push(
        `${this.buildColumnExpr(modelName, current.field, current.path)} ${current.direction === "desc" ? "<" : ">"} ?`,
      );
      params.push(this.mapWhereValue(cursorValues[current.field]));
      orClauses.push(`(${andClauses.join(" AND ")})`);
    }

    return {
      sql: `(${orClauses.join(" OR ")})`,
      params,
    };
  }

  private buildWhere<T>(
    modelName: string,
    where?: Where<T>,
    cursor?: Cursor<T>,
    sortBy?: SortBy<T>[],
  ): { sql: string; params: SqliteValue[] } {
    const parts: string[] = [];
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const builtWhere = this.buildWhereRecursive(modelName, where);
      parts.push(builtWhere.sql);
      params.push(...builtWhere.params);
    }

    if (cursor !== undefined) {
      const builtCursor = this.buildCursor(modelName, cursor, sortBy);
      if (builtCursor.sql !== "") {
        parts.push(builtCursor.sql);
        params.push(...builtCursor.params);
      }
    }

    return {
      sql: parts.length > 0 ? parts.map((part) => `(${part})`).join(" AND ") : "1=1",
      params,
    };
  }

  private buildWhereRecursive<T>(
    modelName: string,
    where: Where<T>,
  ): { sql: string; params: SqliteValue[] } {
    if ("and" in where) {
      const parts = where.and.map((clause) => this.buildWhereRecursive(modelName, clause));
      return {
        sql: parts.map((part) => `(${part.sql})`).join(" AND "),
        params: parts.flatMap((part) => part.params),
      };
    }

    if ("or" in where) {
      const parts = where.or.map((clause) => this.buildWhereRecursive(modelName, clause));
      return {
        sql: parts.map((part) => `(${part.sql})`).join(" OR "),
        params: parts.flatMap((part) => part.params),
      };
    }

    const expr = this.buildColumnExpr(modelName, where.field as string, where.path);

    switch (where.op) {
      case "eq":
        if (where.value === null) {
          return { sql: `${expr} IS NULL`, params: [] };
        }
        return { sql: `${expr} = ?`, params: [this.mapWhereValue(where.value)] };
      case "ne":
        if (where.value === null) {
          return { sql: `${expr} IS NOT NULL`, params: [] };
        }
        return { sql: `${expr} != ?`, params: [this.mapWhereValue(where.value)] };
      case "gt":
        return { sql: `${expr} > ?`, params: [this.mapWhereValue(where.value)] };
      case "gte":
        return { sql: `${expr} >= ?`, params: [this.mapWhereValue(where.value)] };
      case "lt":
        return { sql: `${expr} < ?`, params: [this.mapWhereValue(where.value)] };
      case "lte":
        return { sql: `${expr} <= ?`, params: [this.mapWhereValue(where.value)] };
      case "in":
        if (where.value.length === 0) {
          return { sql: "1=0", params: [] };
        }
        return {
          sql: `${expr} IN (${where.value.map(() => "?").join(", ")})`,
          params: where.value.map((value) => this.mapWhereValue(value)),
        };
      case "not_in":
        if (where.value.length === 0) {
          return { sql: "1=1", params: [] };
        }
        return {
          sql: `${expr} NOT IN (${where.value.map(() => "?").join(", ")})`,
          params: where.value.map((value) => this.mapWhereValue(value)),
        };
      default:
        throw new Error(`Unsupported operator: ${(where as { op: string }).op}`);
    }
  }

  private mapInput(
    fields: Record<string, Field>,
    data: Record<string, unknown> | Partial<Record<string, unknown>>,
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {};

    for (const [fieldName, field] of Object.entries(fields)) {
      const value = data[fieldName];
      if (value === undefined) continue;
      if (value === null) {
        result[fieldName] = null;
        continue;
      }

      if (field.type === "json" || field.type === "json[]") {
        result[fieldName] = JSON.stringify(value);
      } else if (field.type === "boolean") {
        result[fieldName] = value === true ? 1 : 0;
      } else {
        result[fieldName] = value;
      }
    }

    return result;
  }

  private mapRow<K extends keyof S & string, T extends Record<string, unknown>>(
    model: K,
    row: Record<string, unknown>,
    select?: Select<T>,
  ): T {
    const fieldSpecs = this.getModel(model).fields;
    const output: Record<string, unknown> = {};
    const selectedFields =
      select === undefined ? Object.keys(row) : select.map((field) => field as string);

    for (const fieldName of selectedFields) {
      const fieldSpec = fieldSpecs[fieldName];
      const value = row[fieldName];

      if (fieldSpec === undefined || value === undefined || value === null) {
        output[fieldName] = value;
        continue;
      }

      if ((fieldSpec.type === "json" || fieldSpec.type === "json[]") && typeof value === "string") {
        output[fieldName] = JSON.parse(value);
      } else if (fieldSpec.type === "boolean") {
        output[fieldName] = value === 1 || value === true;
      } else if (fieldSpec.type === "number" || fieldSpec.type === "timestamp") {
        output[fieldName] = mapNumeric(value);
      } else {
        output[fieldName] = value;
      }
    }

    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    return output as T;
  }

  private mapWhereValue(value: unknown): SqliteValue {
    if (value === null) return null;
    if (typeof value === "boolean") return value ? 1 : 0;
    if (typeof value === "object" && !(value instanceof Uint8Array)) {
      return JSON.stringify(value);
    }
    return this.toSqliteValue(value);
  }

  private toSqliteValue(value: unknown): SqliteValue {
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      value instanceof Uint8Array ||
      value === null
    ) {
      return value as SqliteValue;
    }

    if (typeof value === "boolean") {
      return value ? 1 : 0;
    }

    return JSON.stringify(value);
  }
}
