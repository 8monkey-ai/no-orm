import type { SQL } from "bun";
import type { Client as PgClient, Pool as PgPool, PoolClient as PgPoolClient } from "pg";

import type { Adapter, Cursor, Field, InferModel, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildIdentityFilter,
  escapeLiteral,
  getIdentityValues,
  getPrimaryKeyFields,
  isRecord,
  mapNumeric,
  quote,
  validateJsonPath,
} from "./common";

/**
 * Internal interface to abstract away the differences between pg and Bun SQL.
 */
interface PostgresExecutor {
  query(sql: string, params: unknown[]): Promise<Record<string, unknown>[]>;
  transaction<T>(fn: (executor: PostgresExecutor) => Promise<T>): Promise<T>;
}

export type PostgresDriver = SQL | PgClient | PgPool | PgPoolClient;

/**
 * Internal interface for Bun SQL.
 */
interface BunSQL {
  unsafe<T>(sql: string, params?: unknown[]): Promise<T>;
  transaction<T>(fn: (tx: BunSQL) => Promise<T>): Promise<T>;
}

function isBunSQL(obj: unknown): obj is BunSQL {
  return objIsObject(obj) && "unsafe" in obj && typeof obj["unsafe"] === "function";
}

function isPgPool(obj: unknown): obj is PgPool {
  return objIsObject(obj) && "connect" in obj && typeof obj["connect"] === "function";
}

function isPgClient(obj: unknown): obj is PgClient {
  return (
    objIsObject(obj) && "query" in obj && typeof obj["query"] === "function" && "release" in obj
  );
}

function objIsObject(obj: unknown): obj is Record<string, unknown> {
  return obj !== null && typeof obj === "object";
}

/**
 * Standardized way to wrap various Postgres drivers.
 * All driver-specific logic is localized here.
 */
function createExecutor(driver: PostgresDriver): PostgresExecutor {
  // Bun SQL Sniffing
  if (isBunSQL(driver)) {
    const sql = driver;
    return {
      query: async (querySql, params) => {
        const rows = await sql.unsafe<Record<string, unknown>[]>(querySql, params);
        return rows;
      },
      transaction: (fn) => {
        return sql.transaction((tx) => fn(createExecutor(tx as PostgresDriver)));
      },
    };
  }

  // pg Pool Sniffing
  if (isPgPool(driver)) {
    const pool = driver;
    return {
      query: async (querySql: string, params: unknown[]) => {
        const result = await pool.query(querySql, params);
        // pg's result.rows is any[], cast to proper type
        // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
        return result.rows as Record<string, unknown>[];
      },
      transaction: async (fn) => {
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          const result = await fn(createExecutor(client as PostgresDriver));
          await client.query("COMMIT");
          return result;
        } catch (e) {
          await client.query("ROLLBACK");
          throw e;
        } finally {
          client.release();
        }
      },
    };
  }

  // pg Client / PoolClient Sniffing
  if (isPgClient(driver)) {
    const client = driver;
    return {
      query: async (querySql: string, params: unknown[]) => {
        const result = await client.query(querySql, params);
        // pg's result.rows is any[], cast to proper type
        // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
        return result.rows as Record<string, unknown>[];
      },
      transaction: async (fn) => {
        await client.query("BEGIN");
        try {
          // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
          const result = await fn(createExecutor(client as PostgresDriver));
          await client.query("COMMIT");
          return result;
        } catch (e) {
          await client.query("ROLLBACK");
          throw e;
        }
      },
    };
  }

  throw new Error("Unsupported Postgres driver.");
}

export class PostgresAdapter<S extends Schema = Schema> implements Adapter<S> {
  private executor: PostgresExecutor;

  constructor(
    private schema: S,
    driver: PostgresDriver,
  ) {
    this.executor = createExecutor(driver);
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
      await this.executor.query(
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
        await this.executor.query(
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
    const insertData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(insertData);
    const values = fields.map((field) => insertData[field]);
    const placeholders = fields.map((_, index) => `$${index + 1}`).join(", ");
    const sql = `INSERT INTO ${quote(model)} (${fields.map((field) => quote(field)).join(", ")}) VALUES (${placeholders}) RETURNING ${this.buildSelect(select)}`;

    const rows = await this.executor.query(sql, values);
    const row = rows[0];
    if (row === undefined) {
      throw new Error("Failed to create record.");
    }
    return this.mapRow(model, row, select);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model, where, select } = args;
    const builtWhere = this.buildWhere(model, where);
    const sql = `SELECT ${this.buildSelect(select)} FROM ${quote(model)} WHERE ${builtWhere.sql} LIMIT 1`;
    const rows = await this.executor.query(sql, builtWhere.params);
    const row = rows[0];
    return row === undefined ? null : this.mapRow(model, row, select);
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
    const params: unknown[] = [];

    if (where !== undefined || cursor !== undefined) {
      const builtWhere = this.buildWhere(model, where, cursor, sortBy);
      parts.push(`WHERE ${builtWhere.sql}`);
      params.push(...builtWhere.params);
    }

    if (sortBy !== undefined && sortBy.length > 0) {
      parts.push(
        `ORDER BY ${sortBy
          .map(
            (sort) =>
              `${this.buildSortExpr(model, sort.field as string, sort.path)} ${(sort.direction ?? "asc").toUpperCase()}`,
          )
          .join(", ")}`,
      );
    }

    if (limit !== undefined) {
      parts.push(`LIMIT $${params.length + 1}`);
      params.push(limit);
    }

    if (offset !== undefined) {
      parts.push(`OFFSET $${params.length + 1}`);
      params.push(offset);
    }

    const rows = await this.executor.query(parts.join(" "), params);
    return rows.map((row) => this.mapRow(model, row, select));
  }

  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; data: Partial<T> }): Promise<T | null> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);

    const updateData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(updateData);
    if (fields.length === 0) {
      return this.find({ model, where });
    }

    const assignments = fields.map((field, index) => `${quote(field)} = $${index + 1}`).join(", ");
    const builtWhere = this.buildWhere(model, where, undefined, undefined, fields.length + 1);
    const sql = `UPDATE ${quote(model)} SET ${assignments} WHERE ${builtWhere.sql} RETURNING *`;
    const values = [...fields.map((field) => updateData[field]), ...builtWhere.params];
    const rows = await this.executor.query(sql, values);
    const row = rows[0];
    return row === undefined ? null : this.mapRow(model, row);
  }

  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model, where, data } = args;
    const modelSpec = this.getModel(model);
    assertNoPrimaryKeyUpdates(modelSpec, data);

    const updateData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(updateData);
    if (fields.length === 0) {
      return 0;
    }

    const assignments = fields.map((field, index) => `${quote(field)} = $${index + 1}`).join(", ");
    const params = fields.map((field) => updateData[field]);
    let sql = `UPDATE ${quote(model)} SET ${assignments}`;

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where, undefined, undefined, params.length + 1);
      sql += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const rows = await this.executor.query(`${sql} RETURNING 1 as touched`, params);
    return rows.length;
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

    const createData = this.mapInput(modelSpec.fields, create);
    const createFields = Object.keys(createData);
    const updateData = this.mapInput(modelSpec.fields, update);
    const updateFields = Object.keys(updateData);
    const pkFields = getPrimaryKeyFields(modelSpec);

    const insertValues = createFields.map((field) => createData[field]);
    const insertPlaceholders = createFields.map((_, index) => `$${index + 1}`).join(", ");
    const conflictTarget = pkFields.map((field) => quote(field)).join(", ");

    let updateClause =
      updateFields.length > 0
        ? updateFields
            .map(
              (field) =>
                `${quote(field)} = $${insertValues.length + updateFields.indexOf(field) + 1}`,
            )
            .join(", ")
        : `${quote(pkFields[0]!)} = EXCLUDED.${quote(pkFields[0]!)}`;

    const params =
      updateFields.length > 0
        ? [...insertValues, ...updateFields.map((field) => updateData[field])]
        : insertValues;

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where, undefined, undefined, params.length + 1);
      updateClause += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const sql = `INSERT INTO ${quote(model)} (${createFields.map((field) => quote(field)).join(", ")}) VALUES (${insertPlaceholders}) ON CONFLICT (${conflictTarget}) DO UPDATE SET ${updateClause} RETURNING ${this.buildSelect(select)}`;
    const rows = await this.executor.query(sql, params);
    const row = rows[0];

    if (row !== undefined) {
      return this.mapRow(model, row, select);
    }

    const existing = await this.find<K, T>({
      model,
      where: buildIdentityFilter(modelSpec, identityValues),
      select,
    });
    if (existing === null) {
      throw new Error("Failed to refetch upserted record.");
    }
    return existing;
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
    await this.executor.query(
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
    const params: unknown[] = [];

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const rows = await this.executor.query(`${sql} RETURNING 1 as touched`, params);
    return rows.length;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    let sql = `SELECT COUNT(*) as count FROM ${quote(model)}`;
    const params: unknown[] = [];

    if (where !== undefined) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      params.push(...builtWhere.params);
    }

    const rows = await this.executor.query(sql, params);
    const row = rows[0];
    return isRecord(row) && typeof row["count"] === "number"
      ? row["count"]
      : Number(row?.["count"] ?? 0);
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction((innerExecutor) => {
      // Driver is not used in transaction adapter, only executor is injected
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion, typescript/no-explicit-any
      const txAdapter = new PostgresAdapter(this.schema, null as unknown as PostgresDriver);
      txAdapter.executor = innerExecutor;
      return fn(txAdapter);
    });
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
        return "DOUBLE PRECISION";
      case "boolean":
        return "BOOLEAN";
      case "timestamp":
        return "BIGINT";
      case "json":
      case "json[]":
        return "JSONB";
      default:
        return "TEXT";
    }
  }

  private buildSelect<T>(select?: Select<T>): string {
    return select === undefined ? "*" : select.map((field) => quote(field as string)).join(", ");
  }

  private buildSortExpr(modelName: string, field: string, path?: string[]): string {
    if (path === undefined || path.length === 0) {
      return quote(field);
    }
    return this.buildColumnExpr(modelName, field, path);
  }

  private buildColumnExpr(
    modelName: string,
    field: string,
    path?: string[],
    value?: unknown,
  ): string {
    if (path === undefined || path.length === 0) {
      return quote(field);
    }

    const model = this.schema[modelName as keyof S];
    const fieldSpec = model?.fields[field];
    if (fieldSpec?.type !== "json" && fieldSpec?.type !== "json[]") {
      throw new Error(`Cannot use JSON path on non-JSON field: ${field}`);
    }

    const segments = validateJsonPath(path)
      .map((segment) => `'${escapeLiteral(segment)}'`)
      .join(", ");
    const baseExpr = `jsonb_extract_path_text(${quote(field)}, ${segments})`;

    if (typeof value === "number") {
      return `(${baseExpr})::double precision`;
    }
    if (typeof value === "boolean") {
      return `(${baseExpr})::boolean`;
    }
    return baseExpr;
  }

  private buildCursor<T>(
    modelName: string,
    cursor: Cursor<T>,
    sortBy?: SortBy<T>[],
    startIndex = 1,
  ): { sql: string; params: unknown[]; nextIndex: number } {
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
      return { sql: "", params: [], nextIndex: startIndex };
    }

    const orClauses: string[] = [];
    const params: unknown[] = [];
    let nextIndex = startIndex;

    for (let i = 0; i < sortCriteria.length; i++) {
      const andClauses: string[] = [];

      for (let j = 0; j < i; j++) {
        const previous = sortCriteria[j]!;
        andClauses.push(
          `${this.buildColumnExpr(modelName, previous.field, previous.path, cursorValues[previous.field])} = $${nextIndex}`,
        );
        params.push(cursorValues[previous.field]);
        nextIndex++;
      }

      const current = sortCriteria[i]!;
      andClauses.push(
        `${this.buildColumnExpr(modelName, current.field, current.path, cursorValues[current.field])} ${current.direction === "desc" ? "<" : ">"} $${nextIndex}`,
      );
      params.push(cursorValues[current.field]);
      nextIndex++;
      orClauses.push(`(${andClauses.join(" AND ")})`);
    }

    return {
      sql: `(${orClauses.join(" OR ")})`,
      params,
      nextIndex,
    };
  }

  private buildWhere<T>(
    modelName: string,
    where?: Where<T>,
    cursor?: Cursor<T>,
    sortBy?: SortBy<T>[],
    startIndex = 1,
  ): { sql: string; params: unknown[]; nextIndex: number } {
    const parts: string[] = [];
    const params: unknown[] = [];
    let nextIndex = startIndex;

    if (where !== undefined) {
      const builtWhere = this.buildWhereRecursive(modelName, where, nextIndex);
      parts.push(builtWhere.sql);
      params.push(...builtWhere.params);
      nextIndex = builtWhere.nextIndex;
    }

    if (cursor !== undefined) {
      const builtCursor = this.buildCursor(modelName, cursor, sortBy, nextIndex);
      if (builtCursor.sql !== "") {
        parts.push(builtCursor.sql);
        params.push(...builtCursor.params);
        nextIndex = builtCursor.nextIndex;
      }
    }

    return {
      sql: parts.length > 0 ? parts.map((part) => `(${part})`).join(" AND ") : "1=1",
      params,
      nextIndex,
    };
  }

  private buildWhereRecursive<T>(
    modelName: string,
    where: Where<T>,
    startIndex: number,
  ): { sql: string; params: unknown[]; nextIndex: number } {
    if ("and" in where) {
      const parts: string[] = [];
      const params: unknown[] = [];
      let nextIndex = startIndex;

      for (const clause of where.and) {
        const built = this.buildWhereRecursive(modelName, clause, nextIndex);
        parts.push(`(${built.sql})`);
        params.push(...built.params);
        nextIndex = built.nextIndex;
      }

      return { sql: parts.join(" AND "), params, nextIndex };
    }

    if ("or" in where) {
      const parts: string[] = [];
      const params: unknown[] = [];
      let nextIndex = startIndex;

      for (const clause of where.or) {
        const built = this.buildWhereRecursive(modelName, clause, nextIndex);
        parts.push(`(${built.sql})`);
        params.push(...built.params);
        nextIndex = built.nextIndex;
      }

      return { sql: parts.join(" OR "), params, nextIndex };
    }

    const expr = this.buildColumnExpr(modelName, where.field as string, where.path, where.value);
    const mappedValue = this.mapWhereValue(where.value);
    switch (where.op) {
      case "eq":
        if (where.value === null) {
          return { sql: `${expr} IS NULL`, params: [], nextIndex: startIndex };
        }
        return {
          sql: `${expr} = $${startIndex}`,
          params: [mappedValue],
          nextIndex: startIndex + 1,
        };
      case "ne":
        if (where.value === null) {
          return { sql: `${expr} IS NOT NULL`, params: [], nextIndex: startIndex };
        }
        return {
          sql: `${expr} != $${startIndex}`,
          params: [mappedValue],
          nextIndex: startIndex + 1,
        };
      case "gt":
        return {
          sql: `${expr} > $${startIndex}`,
          params: [mappedValue],
          nextIndex: startIndex + 1,
        };
      case "gte":
        return {
          sql: `${expr} >= $${startIndex}`,
          params: [mappedValue],
          nextIndex: startIndex + 1,
        };
      case "lt":
        return {
          sql: `${expr} < $${startIndex}`,
          params: [mappedValue],
          nextIndex: startIndex + 1,
        };
      case "lte":
        return {
          sql: `${expr} <= $${startIndex}`,
          params: [mappedValue],
          nextIndex: startIndex + 1,
        };
      case "in": {
        if (where.value.length === 0) {
          return { sql: "1=0", params: [], nextIndex: startIndex };
        }
        const inValues = where.value.map((v) => this.mapWhereValue(v));
        return {
          sql: `${expr} IN (${where.value.map((_, index) => `$${startIndex + index}`).join(", ")})`,
          params: inValues,
          nextIndex: startIndex + where.value.length,
        };
      }
      case "not_in": {
        if (where.value.length === 0) {
          return { sql: "1=1", params: [], nextIndex: startIndex };
        }
        const notInValues = where.value.map((v) => this.mapWhereValue(v));
        return {
          sql: `${expr} NOT IN (${where.value.map((_, index) => `$${startIndex + index}`).join(", ")})`,
          params: notInValues,
          nextIndex: startIndex + where.value.length,
        };
      }
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
        result[fieldName] = value;
      } else if (field.type === "boolean") {
        result[fieldName] = value === true;
      } else {
        result[fieldName] = value;
      }
    }

    return result;
  }

  private mapWhereValue(value: unknown): unknown {
    if (value === null) return null;
    if (typeof value === "boolean") return value;
    if (typeof value === "object" && value !== null) {
      return JSON.stringify(value);
    }
    return value;
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

      if (fieldSpec.type === "json" || fieldSpec.type === "json[]") {
        output[fieldName] = typeof value === "string" ? JSON.parse(value) : value;
      } else if (fieldSpec.type === "boolean") {
        output[fieldName] = value === true || value === 1;
      } else if (fieldSpec.type === "number" || fieldSpec.type === "timestamp") {
        output[fieldName] = mapNumeric(value);
      } else {
        output[fieldName] = value;
      }
    }

    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    return output as T;
  }
}
