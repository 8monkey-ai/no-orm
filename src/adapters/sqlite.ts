import type { Adapter, Cursor, FieldName, Schema, Select, SortBy, Where } from "../core";

export type SqliteValue = string | number | bigint | Uint8Array | null;

/**
 * The standard connection interface the Adapter expects.
 */
export interface SqliteDatabase {
  run(sql: string, params: SqliteValue[]): Promise<{ changes: number }>;
  get(sql: string, params: SqliteValue[]): Promise<Record<string, unknown> | null>;
  all(sql: string, params: SqliteValue[]): Promise<Record<string, unknown>[]>;
}

/**
 * Represents a raw native SQLite driver (like Bun or better-sqlite3).
 */
export interface NativeSqliteStatement {
  run(...params: SqliteValue[]): unknown;
  get(...params: SqliteValue[]): unknown;
  all(...params: SqliteValue[]): unknown;
}

export interface NativeSqliteDriver {
  prepare(sql: string): NativeSqliteStatement;
}

export class SqliteAdapter implements Adapter {
  private db: SqliteDatabase;

  constructor(
    private schema: Schema,
    database: SqliteDatabase | NativeSqliteDriver,
  ) {
    if ("prepare" in database) {
      this.db = this.wrapNativeDriver(database);
    } else {
      this.db = database;
    }
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
        const type = this.mapType(field.type.type);
        const nullable = field.nullable === true ? "" : " NOT NULL";
        return `${this.quote(fieldName)} ${type}${nullable}`;
      });

      const pk = `PRIMARY KEY (${model.primaryKey.fields.map((f) => this.quote(f)).join(", ")})`;

      // Migrations (CREATE TABLE / CREATE INDEX) must be executed sequentially
      // to prevent database locking errors and ensure dependent objects exist.
      // eslint-disable-next-line no-await-in-loop
      await this.db.run(
        `CREATE TABLE IF NOT EXISTS ${this.quote(name)} (${columns.join(", ")}, ${pk})`,
        [],
      );

      if (model.indexes !== undefined) {
        for (let i = 0; i < model.indexes.length; i++) {
          const index = model.indexes[i];
          if (index === undefined) continue;
          const fields = index.fields
            .map((f) => `${this.quote(f.field)}${f.order ? ` ${f.order.toUpperCase()}` : ""}`)
            .join(", ");
          const indexName = `idx_${name}_${i}`;
          // Migrations (CREATE TABLE / CREATE INDEX) must be executed sequentially
          // to prevent database locking errors and ensure dependent objects exist.
          // eslint-disable-next-line no-await-in-loop
          await this.db.run(
            `CREATE INDEX IF NOT EXISTS ${this.quote(indexName)} ON ${this.quote(name)} (${fields})`,
            [],
          );
        }
      }
    }
  }

  async create<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    data: T;
    select?: Select<T>;
  }): Promise<T> {
    const { model, data, select } = args;
    const mappedData = this.mapInput(model, data);
    const fields = Object.keys(mappedData);

    const placeholders = Array.from({ length: fields.length }).fill("?").join(", ");
    const columns = fields.map((f) => this.quote(f)).join(", ");

    const params: SqliteValue[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      if (isStringKey(field)) params.push(mappedData[field] ?? null);
    }

    await this.db.run(
      `INSERT INTO ${this.quote(model)} (${columns}) VALUES (${placeholders})`,
      params,
    );

    if (select !== undefined) {
      const modelSpec = this.schema[model];
      if (modelSpec === undefined) throw new Error(`Model ${model} not found in schema`);

      const pkFields = modelSpec.primaryKey.fields;
      const where: Where<T>[] = [];

      for (let i = 0; i < pkFields.length; i++) {
        const f = pkFields[i];
        if (isValidField<T>(f)) {
          where.push({
            field: f,
            op: "eq",
            value: data[f],
          });
        }
      }

      const result = await this.find<T>({
        model,
        where: where.length === 1 && where[0] ? where[0] : { and: where },
        select,
      });

      if (result === null) throw new Error("Failed to refetch created record");
      return result;
    }

    return data;
  }

  async find<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
    select?: Select<T>;
  }): Promise<T | null> {
    const { model, where, select } = args;
    const query = this.buildSelect(model, select);
    const { sql, params } = this.buildWhere(model, where);

    const fullSql = `${query} WHERE ${sql} LIMIT 1`;
    const row = await this.db.get(fullSql, params);

    return row ? this.mapRow<T>(model, row) : null;
  }

  async findMany<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
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

    const rows = await this.db.all(sql_parts.join(" "), args_sql);
    return rows.map((row) => this.mapRow<T>(model, row));
  }

  async update<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
    data: Partial<T>;
  }): Promise<T | null> {
    const { model, where, data } = args;
    const mappedData = this.mapInput(model, data);
    const fields = Object.keys(mappedData);
    const setClause = fields.map((f) => `${this.quote(f)} = ?`).join(", ");

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

    await this.db.run(`UPDATE ${this.quote(model)} SET ${setClause} WHERE ${whereSql}`, params);

    return this.find({ model, where });
  }

  async updateMany<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
    data: Partial<T>;
  }): Promise<number> {
    const { model, where, data } = args;
    const mappedData = this.mapInput(model, data);
    const fields = Object.keys(mappedData);
    const setClause = fields.map((f) => `${this.quote(f)} = ?`).join(", ");

    const args_sql: SqliteValue[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      if (isStringKey(field)) args_sql.push(mappedData[field] ?? null);
    }

    let sql = `UPDATE ${this.quote(model)} SET ${setClause}`;
    if (where !== undefined) {
      const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);
      sql += ` WHERE ${whereSql}`;
      for (let i = 0; i < whereParams.length; i++) {
        const param = whereParams[i];
        if (param !== undefined) args_sql.push(param);
      }
    }

    const result = await this.db.run(sql, args_sql);
    return result.changes;
  }

  async delete<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
  }): Promise<void> {
    const { model, where } = args;
    const { sql, params } = this.buildWhere(model, where);
    await this.db.run(`DELETE FROM ${this.quote(model)} WHERE ${sql}`, params);
  }

  async deleteMany<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    let sql = `DELETE FROM ${this.quote(model)}`;
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);
      sql += ` WHERE ${whereSql}`;
      for (let i = 0; i < whereParams.length; i++) {
        const param = whereParams[i];
        if (param !== undefined) params.push(param);
      }
    }

    const result = await this.db.run(sql, params);
    return result.changes;
  }

  async count<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where?: Where<T>;
  }): Promise<number> {
    const { model, where } = args;
    let sql = `SELECT COUNT(*) as count FROM ${this.quote(model)}`;
    const params: SqliteValue[] = [];

    if (where !== undefined) {
      const { sql: whereSql, params: whereParams } = this.buildWhere(model, where);
      sql += ` WHERE ${whereSql}`;
      for (let i = 0; i < whereParams.length; i++) {
        const param = whereParams[i];
        if (param !== undefined) params.push(param);
      }
    }

    const result = await this.db.get(sql, params);
    const countVal = result?.["count"];
    return typeof countVal === "number" ? countVal : 0;
  }

  async upsert<T extends Record<string, unknown> = Record<string, unknown>>(args: {
    model: string;
    where: Where<T>;
    create: T;
    update: Partial<T>;
    select?: Select<T>;
  }): Promise<T> {
    const { model, where, create, update, select } = args;
    const modelSpec = this.schema[model];
    if (modelSpec === undefined) throw new Error(`Model ${model} not found in schema`);

    const extractConflictTargets = (w: Where<T>): string[] => {
      if ("and" in w) {
        const parts: string[] = [];
        for (const sub of w.and) {
          parts.push(...extractConflictTargets(sub));
        }
        return parts;
      }
      if ("or" in w) throw new Error("Upsert 'where' clause does not support 'or' operator.");

      const leaf = w as { field: string; path?: string[]; op: string };
      if (leaf.op !== "eq") throw new Error("Upsert 'where' clause only supports 'eq' operator.");

      if (leaf.path && leaf.path.length > 0) {
        throw new Error("Upsert operations by JSON path are currently unsupported.");
      }

      return [this.quote(leaf.field)];
    };

    const conflictTargets = extractConflictTargets(where);
    if (conflictTargets.length === 0)
      throw new Error("Upsert requires at least one conflict column in the 'where' clause.");
    const conflictTargetSql = conflictTargets.join(", ");

    const mappedCreate = this.mapInput(model, create);
    const fields = Object.keys(mappedCreate);
    const columns = fields.map((f) => this.quote(f)).join(", ");
    const placeholders = fields.map(() => "?").join(", ");

    const mappedUpdate = this.mapInput(model, update);
    const updateFields = Object.keys(mappedUpdate);
    const updateClause = updateFields.map((f) => `${this.quote(f)} = ?`).join(", ");

    const sql = `INSERT INTO ${this.quote(model)} (${columns}) VALUES (${placeholders}) ON CONFLICT(${conflictTargetSql}) DO UPDATE SET ${updateClause}`;

    const params: SqliteValue[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i];
      if (isStringKey(field)) params.push(mappedCreate[field] ?? null);
    }
    for (let i = 0; i < updateFields.length; i++) {
      const field = updateFields[i];
      if (isStringKey(field)) params.push(mappedUpdate[field] ?? null);
    }

    await this.db.run(sql, params);

    const pkValuesWhere: Where<T>[] = [];
    for (let i = 0; i < modelSpec.primaryKey.fields.length; i++) {
      const f = modelSpec.primaryKey.fields[i];
      if (isValidField<T>(f)) {
        pkValuesWhere.push({
          field: f,
          op: "eq",
          value: create[f],
        });
      }
    }

    const result = await this.find<T>({
      model,
      where:
        pkValuesWhere.length === 1 && pkValuesWhere[0] ? pkValuesWhere[0] : { and: pkValuesWhere },
      select,
    });

    if (result === null) throw new Error("Failed to refetch upserted record");
    return result;
  }

  async transaction<T>(fn: (tx: Adapter) => Promise<T>): Promise<T> {
    const sp = this.quote(`sp_${Date.now()}_${Math.floor(Math.random() * 100000)}`);

    await this.db.run(`SAVEPOINT ${sp}`, []);
    try {
      const result = await fn(this);
      await this.db.run(`RELEASE SAVEPOINT ${sp}`, []);
      return result;
    } catch (error) {
      await this.db.run(`ROLLBACK TO SAVEPOINT ${sp}`, []);
      throw error;
    }
  }
  // --- Helpers ---

  private quote(name: string): string {
    return `"${name}"`;
  }

  private mapType(type: string): string {
    switch (type) {
      case "string":
        return "TEXT";
      case "number":
        return "REAL";
      case "boolean":
        return "INTEGER"; // SQLite stores booleans as 0 or 1
      case "timestamp":
        return "INTEGER"; // BIGINT/INTEGER for ms since epoch
      case "json":
        return "TEXT"; // Stored as string
      default:
        return "TEXT";
    }
  }

  private buildSelect<T>(model: string, select?: Select<T>): string {
    if (select !== undefined) {
      return `SELECT ${select.map((f) => this.quote(f)).join(", ")} FROM ${this.quote(model)}`;
    }
    return `SELECT * FROM ${this.quote(model)}`;
  }

  private buildColumnExpr(modelName: string, field: string, path?: string[]): string {
    if (path !== undefined && path.length > 0) {
      const modelSpec = this.schema[modelName];
      const fieldSpec = modelSpec?.fields[field];
      if (fieldSpec?.type.type !== "json") {
        throw new Error(`Cannot use 'path' filter on non-JSON field: ${field}`);
      }

      const jsonPath = `$.${path.join(".")}`;
      const escapedPath = jsonPath.replaceAll("'", "''");
      return `json_extract(${this.quote(field)}, '${escapedPath}')`;
    }
    return this.quote(field);
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

    const validSorts = sortCriteria.filter((s) => {
      return cursor.after[s.field] !== undefined;
    });

    const orParts: string[] = [];
    const cursorParams: SqliteValue[] = [];

    for (let i = 0; i < validSorts.length; i++) {
      const currentSort = validSorts[i];
      if (!currentSort) continue;

      const andParts: string[] = [];

      for (let j = 0; j < i; j++) {
        const prevSort = validSorts[j];
        if (!prevSort) continue;
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

    if (orParts.length === 0) return { sql: "", params: [] };

    return {
      sql: `(${orParts.join(" OR ")})`,
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
      this.appendParams(params, result.params);
    }

    if (cursor !== undefined) {
      const cursorResult = this.buildCursor(modelName, cursor, sortBy);
      if (cursorResult.sql !== "") {
        parts.push(cursorResult.sql);
        this.appendParams(params, cursorResult.params);
      }
    }

    const sql = parts.length > 1 ? parts.map((p) => `(${p})`).join(" AND ") : (parts[0] ?? "1=1");

    return { sql, params };
  }

  private appendParams(target: SqliteValue[], source: SqliteValue[]): void {
    for (let j = 0; j < source.length; j++) {
      const param = source[j];
      if (param !== undefined) target.push(param);
    }
  }

  private buildWhereRecursive<T>(
    modelName: string,
    where: Where<T>,
  ): { sql: string; params: SqliteValue[] } {
    if ("and" in where) {
      const parts = where.and.map((w) => this.buildWhereRecursive(modelName, w));
      const sql = `(${parts.map((p) => p.sql).join(" AND ")})`;
      const params: SqliteValue[] = [];
      for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        if (part) this.appendParams(params, part.params);
      }
      return { sql, params };
    }

    if ("or" in where) {
      const parts = where.or.map((w) => this.buildWhereRecursive(modelName, w));
      const sql = `(${parts.map((p) => p.sql).join(" OR ")})`;
      const params: SqliteValue[] = [];
      for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        if (part) this.appendParams(params, part.params);
      }
      return { sql, params };
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
        const params: SqliteValue[] = [];
        for (let i = 0; i < list.length; i++) {
          params.push(this.mapWhereValue(list[i]));
        }
        return {
          sql: `${quotedField} IN (${list.map(() => "?").join(", ")})`,
          params,
        };
      }
      case "not_in": {
        const list = Array.isArray(value) ? value : [value];
        const params: SqliteValue[] = [];
        for (let i = 0; i < list.length; i++) {
          params.push(this.mapWhereValue(list[i]));
        }
        return {
          sql: `${quotedField} NOT IN (${list.map(() => "?").join(", ")})`,
          params,
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

      if (field.type.type === "json") {
        result[fieldName] = JSON.stringify(val);
      } else if (field.type.type === "boolean") {
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
    const model = this.schema[modelName];
    if (model === undefined) {
      if (isModelType<T>(row)) return row;
      throw new Error("Invalid row data");
    }

    for (const [fieldName, field] of Object.entries(model.fields)) {
      const val = row[fieldName];
      if (val === undefined || val === null) continue;

      if (field.type.type === "json" && typeof val === "string") {
        try {
          row[fieldName] = JSON.parse(val);
        } catch {
          // Keep as string if parsing fails
        }
      } else if (field.type.type === "boolean") {
        row[fieldName] = val === 1 || val === true;
      }
    }

    if (isModelType<T>(row)) return row;
    throw new Error("Row does not conform to model bounds");
  }
}

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function isValidField<T>(field: unknown): field is FieldName<T> {
  return typeof field === "string" && field !== "";
}

function isStringKey(key: unknown): key is string {
  return typeof key === "string" && key !== "";
}

function isModelType<T>(obj: unknown): obj is T {
  return typeof obj === "object" && obj !== null;
}
