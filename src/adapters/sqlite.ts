import { AsyncLocalStorage } from "node:async_hooks";
import type {
  Adapter,
  Cursor,
  FieldName,
  Schema,
  Select,
  SortBy,
  Where,
  WhereWithoutPath,
} from "../types";
import { isModelType, isRecord, isStringKey, isValidField } from "../utils/is";
import { escapeLiteral, quote } from "../utils/sql";
import type {
  NativeSqliteDriver,
  SqliteDatabase,
  SqliteValue,
} from "./sqlite.types";

const transactionStorage = new AsyncLocalStorage<SqliteDatabase>();

export class SqliteAdapter<S extends Schema = Schema> implements Adapter<S> {
  private db: SqliteDatabase;
  private spCounter = 0;

  constructor(
    private schema: S,
    database: SqliteDatabase | NativeSqliteDriver,
  ) {
    if ("prepare" in database) {
      this.db = this.wrapNativeDriver(database);
    } else {
      this.db = database;
    }
  }

  private get activeDb(): SqliteDatabase {
    return transactionStorage.getStore() ?? this.db;
  }

  private wrapNativeDriver(native: NativeSqliteDriver): SqliteDatabase {
    return {
      run: (sql, params) => {
        const stmt = native.prepare(sql);
        const result = stmt.run(...params);
        const changes =
          isRecord(result) && typeof result["changes"] === "number" ? result["changes"] : 0;
        return Promise.resolve({ changes });
      },
      get: (sql, params) => {
        const stmt = native.prepare(sql);
        const row = stmt.get(...params);
        return Promise.resolve(isRecord(row) ? row : null);
      },
      all: (sql, params) => {
        const stmt = native.prepare(sql);
        const rows = stmt.all(...params);
        return Promise.resolve(Array.isArray(rows) ? rows.filter((item) => isRecord(item)) : []);
      },
    };
  }

  async migrate(): Promise<void> {
    for (const [name, model] of Object.entries(this.schema)) {
      const columns = Object.entries(model.fields).map(([fieldName, field]) => {
        const type = this.mapType(field.type);
        const nullable = field.nullable === true ? "" : " NOT NULL";
        return `${quote(fieldName)} ${type}${nullable}`;
      });

      const pkFields = Array.isArray(model.primaryKey)
        ? model.primaryKey
        : [model.primaryKey];
      const pk = `PRIMARY KEY (${pkFields.map((f) => quote(f)).join(", ")})`;

      // Migrations (CREATE TABLE / CREATE INDEX) must be executed sequentially
      // to prevent database locking errors and ensure dependent objects exist.
      await this.activeDb.run(
        `CREATE TABLE IF NOT EXISTS ${quote(name)} (${columns.join(", ")}, ${pk})`,
        [],
      );

      if (model.indexes !== undefined) {
        for (let i = 0; i < model.indexes.length; i++) {
          const index = model.indexes[i];
          if (index === undefined) continue;
          const fields = Array.isArray(index.field) ? index.field : [index.field];
          const fieldList = fields
            .map((f) => `${quote(f)}${index.order ? ` ${index.order.toUpperCase()}` : ""}`)
            .join(", ");
          const indexName = `idx_${name}_${i}`;
          await this.activeDb.run(
            `CREATE INDEX IF NOT EXISTS ${quote(indexName)} ON ${quote(name)} (${fieldList})`,
            [],
          );
        }
      }
    }
  }

  async create<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    data: T;
    select?: Select<T>;
  }): Promise<T> {
    const { model, data, select } = args;
    const mappedData = this.mapInput(model, data);
    const fields = Object.keys(mappedData);

    const placeholders = Array.from({ length: fields.length }).fill("?").join(", ");
    const columns = fields.map((f) => quote(f)).join(", ");

    const params: SqliteValue[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      if (isStringKey(field)) params.push(mappedData[field] ?? null);
    }

    await this.activeDb.run(
      `INSERT INTO ${quote(model)} (${columns}) VALUES (${placeholders})`,
      params,
    );

    const modelSpec = this.schema[model];
    if (modelSpec === undefined) throw new Error(`Model ${model} not found in schema`);

    const pkFields = Array.isArray(modelSpec.primaryKey)
      ? modelSpec.primaryKey
      : [modelSpec.primaryKey];

    const where: Where<T>[] = [];
    for (let i = 0; i < pkFields.length; i++) {
      const f = pkFields[i];
      if (isValidField<T>(f)) {
        where.push({
          field: f,
          op: "eq",
          value: (data as any)[f],
        });
      }
    }

    const result = await this.find<K, T>({
      model,
      where: where.length === 1 && where[0] ? where[0] : { and: where },
      select,
    });

    if (result === null) throw new Error("Failed to refetch created record");
    return result;
  }

  async find<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null> {
    const { model, where, select } = args;
    const query = this.buildSelect(model, select);
    const { sql, params } = this.buildWhere(model, where);

    const fullSql = `${query} WHERE ${sql} LIMIT 1`;
    const row = await this.activeDb.get(fullSql, params);

    return row ? this.mapRow<T>(model, row) : null;
  }

  async findMany<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  }): Promise<T[]> {
    const { model, where, select, sortBy, limit, offset, cursor } = args;
    const query = this.buildSelect(model, select);
    const args_sql: SqliteValue[] = [];
    const sql_parts: string[] = [query];

    if (where !== undefined || cursor !== undefined) {
      const { sql, params } = this.buildWhere(model, where, cursor, sortBy);
      sql_parts.push(`WHERE ${sql}`);
      for (let i = 0; i < params.length; i++) {
        const param = params[i];
        if (param !== undefined) args_sql.push(param);
      }
    }

    if (sortBy !== undefined) {
      const order = sortBy
        .map((s) => {
          const col = this.buildColumnExpr(model, s.field as string, s.path);
          return `${col} ${s.direction?.toUpperCase() ?? "ASC"}`;
        })
        .join(", ");
      sql_parts.push(`ORDER BY ${order}`);
    }

    if (limit !== undefined) {
      sql_parts.push(`LIMIT ?`);
      args_sql.push(limit);
    }

    if (offset !== undefined) {
      sql_parts.push(`OFFSET ?`);
      args_sql.push(offset);
    }

    const rows = await this.activeDb.all(sql_parts.join(" "), args_sql);
    return rows.map((row) => this.mapRow<T>(model, row));
  }

  async update<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null> {
    const { model, where, data } = args;
    const mappedData = this.mapInput(model, data);
    const fields = Object.keys(mappedData);
    if (fields.length === 0) return this.find({ model, where });

    const setClause = fields.map((f) => `${quote(f)} = ?`).join(", ");
    const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);

    const params: SqliteValue[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      if (isStringKey(field)) params.push(mappedData[field] ?? null);
    }
    for (let i = 0; i < whereParams.length; i++) {
      const param = whereParams[i];
      if (param !== undefined) params.push(param);
    }

    await this.activeDb.run(`UPDATE ${quote(model)} SET ${setClause} WHERE ${whereSql}`, params);

    const modelSpec = this.schema[model];
    if (modelSpec === undefined) throw new Error(`Model ${model} not found in schema`);

    const pkFields = Array.isArray(modelSpec.primaryKey)
      ? modelSpec.primaryKey
      : [modelSpec.primaryKey];

    const preRead = await this.find<K, T>({ model, where });
    if (!preRead) return null;

    const pkWhere: Where<T>[] = [];
    for (const f of pkFields) {
      if (isValidField<T>(f)) {
        pkWhere.push({ field: f, op: "eq", value: (preRead as any)[f] });
      }
    }

    return this.find<K, T>({
      model,
      where: pkWhere.length === 1 && pkWhere[0] ? pkWhere[0] : { and: pkWhere },
    });
  }

  async updateMany<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
    data: Partial<T>;
  }): Promise<number> {
    const { model, where, data } = args;
    const mappedData = this.mapInput(model, data);
    const fields = Object.keys(mappedData);
    if (fields.length === 0) return 0;

    const setClause = fields.map((f) => `${quote(f)} = ?`).join(", ");
    const args_sql: SqliteValue[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      if (isStringKey(field)) args_sql.push(mappedData[field] ?? null);
    }

    let sql = `UPDATE ${quote(model)} SET ${setClause}`;
    if (where !== undefined) {
      const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);
      sql += ` WHERE ${whereSql}`;
      for (let i = 0; i < whereParams.length; i++) {
        const param = whereParams[i];
        if (param !== undefined) args_sql.push(param);
      }
    }

    const result = await this.activeDb.run(sql, args_sql);
    return result.changes;
  }

  async delete<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: Where<T>;
  }): Promise<void> {
    const { model, where } = args;
    const { sql, params } = this.buildWhere(model, where);
    await this.activeDb.run(`DELETE FROM ${quote(model)} WHERE ${sql}`, params);
  }

  async deleteMany<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    let sql = `DELETE FROM ${quote(model)}`;
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);
      sql += ` WHERE ${whereSql}`;
      for (let i = 0; i < whereParams.length; i++) {
        const param = whereParams[i];
        if (param !== undefined) params.push(param);
      }
    }

    const result = await this.activeDb.run(sql, params);
    return result.changes;
  }

  async count<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    let sql = `SELECT COUNT(*) as count FROM ${quote(model)}`;
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);
      sql += ` WHERE ${whereSql}`;
      for (let i = 0; i < whereParams.length; i++) {
        const param = whereParams[i];
        if (param !== undefined) params.push(param);
      }
    }

    const result = await this.activeDb.get(sql, params);
    const countVal = result?.["count"];
    return typeof countVal === "number" ? countVal : 0;
  }

  async upsert<K extends keyof S & string, T = InferModel<S[K]>>(args: {
    model: K;
    where: WhereWithoutPath<T>;
    create: T;
    update: Partial<T>;
    select?: Select<T>;
  }): Promise<T> {
    const { model, where, create, update, select } = args;
    const modelSpec = this.schema[model];
    if (modelSpec === undefined) throw new Error(`Model ${model} not found in schema`);

    const extractConflictTargets = (w: Where<T>): string[] => {
      if ("and" in w) {
        return w.and.flatMap((sub) => extractConflictTargets(sub));
      }
      if ("or" in w) throw new Error("Upsert 'where' clause does not support 'or' operator.");

      const leaf = w as { field: string; op: string };
      if (leaf.op !== "eq") throw new Error("Upsert 'where' clause only supports 'eq' operator.");
      return [quote(leaf.field)];
    };

    const conflictTargets = extractConflictTargets(where);
    if (conflictTargets.length === 0)
      throw new Error("Upsert requires at least one conflict column in the 'where' clause.");
    const conflictTargetSql = conflictTargets.join(", ");

    const mappedCreate = this.mapInput(model, create);
    const fields = Object.keys(mappedCreate);
    const columns = fields.map((f) => quote(f)).join(", ");
    const placeholders = fields.map(() => "?").join(", ");

    const mappedUpdate = this.mapInput(model, update);
    const updateFields = Object.keys(mappedUpdate);
    const updateClause = updateFields.map((f) => `${quote(f)} = ?`).join(", ");

    const sql = `INSERT INTO ${quote(model)} (${columns}) VALUES (${placeholders}) ON CONFLICT(${conflictTargetSql}) DO UPDATE SET ${updateClause}`;

    const params: SqliteValue[] = [];
    for (const f of fields) params.push(mappedCreate[f] ?? null);
    for (const f of updateFields) params.push(mappedUpdate[f] ?? null);

    await this.activeDb.run(sql, params);

    const pkFields = Array.isArray(modelSpec.primaryKey)
      ? modelSpec.primaryKey
      : [modelSpec.primaryKey];

    const pkWhere: Where<T>[] = [];
    for (const f of pkFields) {
      if (isValidField<T>(f)) {
        pkWhere.push({ field: f, op: "eq", value: (create as any)[f] });
      }
    }

    const result = await this.find<K, T>({
      model,
      where: pkWhere.length === 1 && pkWhere[0] ? pkWhere[0] : { and: pkWhere },
      select,
    });

    if (result === null) throw new Error("Failed to refetch upserted record");
    return result;
  }

  async transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    const sp = quote(`sp_${this.spCounter++}`);

    await this.activeDb.run(`SAVEPOINT ${sp}`, []);
    try {
      const result = await transactionStorage.run(this.activeDb, () => fn(this));
      await this.activeDb.run(`RELEASE SAVEPOINT ${sp}`, []);
      return result;
    } catch (error) {
      await this.activeDb.run(`ROLLBACK TO SAVEPOINT ${sp}`, []);
      throw error;
    }
  }

  private mapType(type: string): string {
    switch (type) {
      case "string":
        return "TEXT";
      case "number":
        return "REAL";
      case "boolean":
      case "timestamp":
        return "INTEGER";
      case "json":
      case "json[]":
        return "TEXT";
      default:
        return "TEXT";
    }
  }

  private buildSelect<T>(model: string, select?: Select<T>): string {
    if (select !== undefined) {
      return `SELECT ${select.map((f) => quote(f as string)).join(", ")} FROM ${quote(model)}`;
    }
    return `SELECT * FROM ${quote(model)}`;
  }

  private buildColumnExpr(modelName: string, field: string, path?: string[]): string {
    if (path !== undefined && path.length > 0) {
      const modelSpec = (this.schema as any)[modelName];
      const fieldSpec = modelSpec?.fields[field];
      if (fieldSpec?.type !== "json" && fieldSpec?.type !== "json[]") {
        throw new Error(`Cannot use 'path' filter on non-JSON field: ${field}`);
      }

      const jsonPath = `$.${path.join(".")}`;
      return `json_extract(${quote(field)}, '${escapeLiteral(jsonPath)}')`;
    }
    return quote(field);
  }

  private buildCursor<T>(
    modelName: string,
    cursor: Cursor<T>,
    sortBy?: SortBy<T>[],
  ): { sql: string; params: SqliteValue[] } {
    const entries = Object.entries(cursor.after);
    if (entries.length === 0) return { sql: "", params: [] };

    const sortCriteria: SortBy<T>[] =
      sortBy && sortBy.length > 0
        ? sortBy
        : entries.map(([field]) => {
            if (!isValidField<T>(field)) throw new Error("Invalid cursor field");
            return { field, direction: "asc" };
          });

    const validSorts = sortCriteria.filter((s) => cursor.after[s.field] !== undefined);
    const orParts: string[] = [];
    const cursorParams: SqliteValue[] = [];

    for (let i = 0; i < validSorts.length; i++) {
      const currentSort = validSorts[i]!;
      const andParts: string[] = [];

      for (let j = 0; j < i; j++) {
        const prevSort = validSorts[j]!;
        const colExpr = this.buildColumnExpr(modelName, prevSort.field as string, prevSort.path);
        andParts.push(`${colExpr} = ?`);
        cursorParams.push(this.mapWhereValue(cursor.after[prevSort.field]));
      }

      const op = currentSort.direction === "desc" ? "<" : ">";
      const colExpr = this.buildColumnExpr(
        modelName,
        currentSort.field as string,
        currentSort.path,
      );
      andParts.push(`${colExpr} ${op} ?`);
      cursorParams.push(this.mapWhereValue(cursor.after[currentSort.field]));

      orParts.push(`(${andParts.join(" AND ")})`);
    }

    return {
      sql: orParts.length > 0 ? `(${orParts.join(" OR ")})` : "",
      params: cursorParams,
    };
  }

  private buildWhere<T>(
    modelName: string,
    where?: Where<T>,
    cursor?: Cursor<T>,
    sortBy?: SortBy<T>[],
  ): { sql: string; params: SqliteValue[] } {
    const params: SqliteValue[] = [];
    const parts: string[] = [];

    if (where !== undefined) {
      const result = this.buildWhereRecursive(modelName, where);
      parts.push(result.sql);
      params.push(...result.params);
    }

    if (cursor !== undefined) {
      const cursorResult = this.buildCursor(modelName, cursor, sortBy);
      if (cursorResult.sql !== "") {
        parts.push(cursorResult.sql);
        params.push(...cursorResult.params);
      }
    }

    const sql = parts.length > 1 ? parts.map((p) => `(${p})`).join(" AND ") : (parts[0] ?? "1=1");
    return { sql, params };
  }

  private buildWhereRecursive<T>(
    modelName: string,
    where: Where<T>,
  ): { sql: string; params: SqliteValue[] } {
    if ("and" in where) {
      const parts = where.and.map((w) => this.buildWhereRecursive(modelName, w));
      return {
        sql: `(${parts.map((p) => p.sql).join(" AND ")})`,
        params: parts.flatMap((p) => p.params),
      };
    }

    if ("or" in where) {
      const parts = where.or.map((w) => this.buildWhereRecursive(modelName, w));
      return {
        sql: `(${parts.map((p) => p.sql).join(" OR ")})`,
        params: parts.flatMap((p) => p.params),
      };
    }

    const leaf = where as { field: string; path?: string[]; op: string; value: unknown };
    const { field, path, op, value } = leaf;
    const quotedField = this.buildColumnExpr(modelName, field, path);

    switch (op) {
      case "eq":
        return { sql: `${quotedField} = ?`, params: [this.mapWhereValue(value)] };
      case "ne":
        return { sql: `${quotedField} != ?`, params: [this.mapWhereValue(value)] };
      case "gt":
        return { sql: `${quotedField} > ?`, params: [this.mapWhereValue(value)] };
      case "gte":
        return { sql: `${quotedField} >= ?`, params: [this.mapWhereValue(value)] };
      case "lt":
        return { sql: `${quotedField} < ?`, params: [this.mapWhereValue(value)] };
      case "lte":
        return { sql: `${quotedField} <= ?`, params: [this.mapWhereValue(value)] };
      case "in": {
        const list = Array.isArray(value) ? value : [value];
        if (list.length === 0) return { sql: "1=0", params: [] };
        return {
          sql: `${quotedField} IN (${list.map(() => "?").join(", ")})`,
          params: list.map((v) => this.mapWhereValue(v)),
        };
      }
      case "not_in": {
        const list = Array.isArray(value) ? value : [value];
        if (list.length === 0) return { sql: "1=1", params: [] };
        return {
          sql: `${quotedField} NOT IN (${list.map(() => "?").join(", ")})`,
          params: list.map((v) => this.mapWhereValue(v)),
        };
      }
      default:
        throw new Error(`Unsupported operator: ${op}`);
    }
  }

  private mapWhereValue(value: unknown): SqliteValue {
    if (value === null) return null;
    if (typeof value === "boolean") return value ? 1 : 0;
    if (typeof value === "object" && !(value instanceof Uint8Array)) return JSON.stringify(value);
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "bigint" ||
      value instanceof Uint8Array
    ) {
      return value;
    }
    return JSON.stringify(value);
  }

  private mapInput(
    modelName: string,
    data: Record<string, unknown> | Partial<Record<string, unknown>>,
  ): Record<string, SqliteValue> {
    const model = this.schema[modelName];
    if (model === undefined) {
      if (isModelType<Record<string, SqliteValue>>(data)) return data;
      throw new Error("Invalid model payload");
    }

    const result: Record<string, SqliteValue> = {};
    for (const [fieldName, field] of Object.entries(model.fields)) {
      const val = data[fieldName];
      if (val === undefined) continue;
      if (val === null) {
        result[fieldName] = null;
        continue;
      }

      if (field.type === "json" || field.type === "json[]") {
        result[fieldName] = JSON.stringify(val);
      } else if (field.type === "boolean") {
        result[fieldName] = val === true ? 1 : 0;
      } else if (
        typeof val === "string" ||
        typeof val === "number" ||
        typeof val === "bigint" ||
        val instanceof Uint8Array
      ) {
        result[fieldName] = val;
      } else {
        result[fieldName] = JSON.stringify(val);
      }
    }
    return result;
  }

  private mapRow<T>(modelName: string, row: Record<string, unknown>): T {
    const model = (this.schema as any)[modelName];
    if (model === undefined) return row as T;

    for (const [fieldName, field] of Object.entries(model.fields as Record<string, any>)) {
      const val = row[fieldName];
      if (val === undefined || val === null) continue;

      if ((field.type === "json" || field.type === "json[]") && typeof val === "string") {
        try {
          row[fieldName] = JSON.parse(val);
        } catch {
          // Keep as string if parsing fails
        }
      } else if (field.type === "boolean") {
        row[fieldName] = val === 1 || val === true;
      } else if (field.type === "number" || field.type === "timestamp") {
        if (typeof val === "string") {
          row[fieldName] = Number(val);
        } else if (typeof val === "bigint") {
          row[fieldName] = Number(val);
        }
      }
    }
    return row as T;
  }
}
