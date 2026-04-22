import type { QueryExecutor, SqlDialect } from "../dialects/types";
import type { Adapter, Cursor, Field, InferModel, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildIdentityFilter,
  getIdentityValues,
  getPrimaryKeyFields,
  mapNumeric,
} from "./common";

export abstract class SqlAdapter<S extends Schema = Schema> implements Adapter<S> {
  constructor(
    protected schema: S,
    protected executor: QueryExecutor,
    protected dialect: SqlDialect,
  ) {}

  async migrate(): Promise<void> {
    const models = Object.entries(this.schema);
    const runPromises: Promise<unknown>[] = [];
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      const fields = Object.entries(model.fields);
      const columns: string[] = [];
      for (let j = 0; j < fields.length; j++) {
        const [fieldName, field] = fields[j]!;
        const nullable = field.nullable === true ? "" : " NOT NULL";

        columns.push(
          `${this.dialect.quote(fieldName)} ${this.dialect.mapFieldType(field)}${nullable}`,
        );
      }

      const pkFields = getPrimaryKeyFields(model);
      const quotedPkFields: string[] = [];
      for (let j = 0; j < pkFields.length; j++) {
        quotedPkFields.push(this.dialect.quote(pkFields[j]!));
      }
      const pk = `PRIMARY KEY (${quotedPkFields.join(", ")})`;

      runPromises.push(
        this.executor.run(
          `CREATE TABLE IF NOT EXISTS ${this.dialect.quote(name)} (${columns.join(", ")}, ${pk})`,
          [],
        ),
      );

      if (model.indexes) {
        for (let j = 0; j < model.indexes.length; j++) {
          const index = model.indexes[j]!;
          const indexFields = Array.isArray(index.field) ? index.field : [index.field];
          const formattedFields: string[] = [];
          for (let k = 0; k < indexFields.length; k++) {
            formattedFields.push(
              `${this.dialect.quote(indexFields[k]!)}${index.order ? ` ${index.order.toUpperCase()}` : ""}`,
            );
          }
          runPromises.push(
            this.executor.run(
              `CREATE INDEX IF NOT EXISTS ${this.dialect.quote(`idx_${name}_${j}`)} ON ${this.dialect.quote(name)} (${formattedFields.join(", ")})`,
              [],
            ),
          );
        }
      }
    }
    await Promise.all(runPromises);
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    const { model, data, select } = args;
    const modelSpec = this.getModel(model);

    const insertData = this.mapInput(modelSpec.fields, data);
    const fields = Object.keys(insertData);

    const quotedFields: string[] = [];
    const placeholders: string[] = [];
    const values: unknown[] = [];

    for (let i = 0; i < fields.length; i++) {
      const field = fields[i]!;
      quotedFields.push(this.dialect.quote(field));
      placeholders.push(this.dialect.placeholder(i));
      values.push(insertData[field]);
    }

    const sql = `INSERT INTO ${this.dialect.quote(model)} (${quotedFields.join(", ")}) VALUES (${placeholders.join(", ")}) RETURNING ${this.buildSelect(select)}`;
    const row = await this.executor.get(sql, values);

    if (!row) {
      // Fallback for drivers that don't support RETURNING
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      return (await this.find({
        model,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
        where: buildIdentityFilter(modelSpec, data) as unknown as Where<T>,
        select,
      })) as T;
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-return
    return this.mapRow(model, row, select);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model, where, select } = args;
    const builtWhere = this.buildWhere(model, where);
    const sql = `SELECT ${this.buildSelect(select)} FROM ${this.dialect.quote(model)} WHERE ${builtWhere.sql} LIMIT 1`;
    const row = await this.executor.get(sql, builtWhere.params);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-return
    return row ? this.mapRow(model, row, select) : null;
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
    const params: unknown[] = [];
    let sql = `SELECT ${this.buildSelect(select)} FROM ${this.dialect.quote(model)}`;

    const builtWhere = this.buildWhere(model, where, cursor, sortBy, params.length);
    if (builtWhere.sql !== "1=1") {
      sql += ` WHERE ${builtWhere.sql}`;
      for (let i = 0; i < builtWhere.params.length; i++) {
        params.push(builtWhere.params[i]);
      }
    }

    if (sortBy && sortBy.length > 0) {
      const sortParts: string[] = [];
      for (let i = 0; i < sortBy.length; i++) {
        const sort = sortBy[i]!;
        sortParts.push(
          `${this.buildColumnExpr(model, sort.field as string, sort.path)} ${(sort.direction ?? "asc").toUpperCase()}`,
        );
      }
      sql += ` ORDER BY ${sortParts.join(", ")}`;
    }

    if (limit !== undefined) {
      sql += ` LIMIT ${this.dialect.placeholder(params.length)}`;
      params.push(limit);
    }

    if (offset !== undefined) {
      sql += ` OFFSET ${this.dialect.placeholder(params.length)}`;
      params.push(offset);
    }

    const rows = await this.executor.all(sql, params);
    const result: T[] = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (row) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-argument
        result.push(this.mapRow(model, row, select));
      }
    }
    return result;
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

    const assignments: string[] = [];
    const params: unknown[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i]!;
      assignments.push(`${this.dialect.quote(field)} = ${this.dialect.placeholder(i)}`);
      params.push(updateData[field]);
    }

    const builtWhere = this.buildWhere(model, where, undefined, undefined, params.length);
    const sql = `UPDATE ${this.dialect.quote(model)} SET ${assignments.join(", ")} WHERE ${builtWhere.sql} RETURNING *`;
    for (let i = 0; i < builtWhere.params.length; i++) {
      params.push(builtWhere.params[i]);
    }

    const row = await this.executor.get(sql, params);
    if (!row) {
      // Check if it exists and return it if no changes were needed or RETURNING not supported
      return this.find({ model, where });
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-return
    return this.mapRow(model, row);
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
    if (fields.length === 0) return 0;

    const assignments: string[] = [];
    const params: unknown[] = [];
    for (let i = 0; i < fields.length; i++) {
      const field = fields[i]!;
      assignments.push(`${this.dialect.quote(field)} = ${this.dialect.placeholder(i)}`);
      params.push(updateData[field]);
    }

    let sql = `UPDATE ${this.dialect.quote(model)} SET ${assignments.join(", ")}`;
    if (where) {
      const builtWhere = this.buildWhere(model, where, undefined, undefined, params.length);
      sql += ` WHERE ${builtWhere.sql}`;
      for (let i = 0; i < builtWhere.params.length; i++) {
        params.push(builtWhere.params[i]);
      }
    }

    const res = await this.executor.run(sql, params);
    return res.changes;
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

    const createData = this.mapInput(modelSpec.fields, create);
    const createFields = Object.keys(createData);
    const updateData = this.mapInput(modelSpec.fields, update);
    const updateFields = Object.keys(updateData);
    const pkFields = getPrimaryKeyFields(modelSpec);

    const insertColumns: string[] = [];
    const insertPlaceholders: string[] = [];
    const params: unknown[] = [];
    for (let i = 0; i < createFields.length; i++) {
      const field = createFields[i]!;
      insertColumns.push(this.dialect.quote(field));
      insertPlaceholders.push(this.dialect.placeholder(i));
      params.push(createData[field]);
    }

    const updateColumns: string[] = [];
    for (let i = 0; i < updateFields.length; i++) {
      const field = updateFields[i]!;
      updateColumns.push(field);
      params.push(updateData[field]);
    }

    let whereSql = "";
    if (where) {
      const builtWhere = this.buildWhere(model, where, undefined, undefined, params.length);
      whereSql = builtWhere.sql;
      for (let i = 0; i < builtWhere.params.length; i++) {
        params.push(builtWhere.params[i]);
      }
    }

    if (this.dialect.upsert) {
      const { sql, params: upsertParams } = this.dialect.upsert({
        table: model,
        insertColumns,
        insertPlaceholders,
        updateColumns,
        conflictColumns: pkFields,
        select,
        whereSql,
      });
      const row = await this.executor.get(sql, upsertParams ?? params);
      if (row)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-return
        return this.mapRow(model, row, select);
    } else {
      // Generic fallback for ON CONFLICT (Postgres/SQLite)
      const conflictTarget = [];
      for (let i = 0; i < pkFields.length; i++)
        conflictTarget.push(this.dialect.quote(pkFields[i]!));

      let updateSet = "";
      if (updateFields.length > 0) {
        const sets = [];
        for (let i = 0; i < updateFields.length; i++) {
          const field = updateFields[i]!;
          sets.push(
            `${this.dialect.quote(field)} = ${this.dialect.placeholder(createFields.length + i)}`,
          );
        }
        updateSet = `DO UPDATE SET ${sets.join(", ")}`;
        if (whereSql) updateSet += ` WHERE ${whereSql}`;
      } else {
        updateSet = "DO NOTHING";
      }

      const sql = `INSERT INTO ${this.dialect.quote(model)} (${insertColumns.join(", ")}) VALUES (${insertPlaceholders.join(", ")}) ON CONFLICT (${conflictTarget.join(", ")}) ${updateSet} RETURNING ${this.buildSelect(select)}`;
      const row = await this.executor.get(sql, params);
      if (row)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-return
        return this.mapRow(model, row, select);
    }

    const identityValues = getIdentityValues(modelSpec, create);
    const existing = await this.find({
      model,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion
      where: buildIdentityFilter(modelSpec, identityValues) as unknown as Where<T>,
      select,
    });
    if (!existing) throw new Error("Failed to refetch upserted record.");
    return existing;
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    const { model, where } = args;
    const builtWhere = this.buildWhere(model, where);
    await this.executor.run(
      `DELETE FROM ${this.dialect.quote(model)} WHERE ${builtWhere.sql}`,
      builtWhere.params,
    );
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    let sql = `DELETE FROM ${this.dialect.quote(model)}`;
    const params: unknown[] = [];
    if (where) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      for (let i = 0; i < builtWhere.params.length; i++) params.push(builtWhere.params[i]);
    }
    const res = await this.executor.run(sql, params);
    return res.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model, where } = args;
    let sql = `SELECT COUNT(*) as count FROM ${this.dialect.quote(model)}`;
    const params: unknown[] = [];
    if (where) {
      const builtWhere = this.buildWhere(model, where);
      sql += ` WHERE ${builtWhere.sql}`;
      for (let i = 0; i < builtWhere.params.length; i++) params.push(builtWhere.params[i]);
    }
    const row = await this.executor.get(sql, params);
    if (!row) return 0;
    const count = row["count"];
    return typeof count === "number" ? count : Number(count ?? 0);
  }

  // --- HELPERS ---

  protected getModel<K extends keyof S & string>(model: K): S[K] {
    const spec = this.schema[model];
    if (!spec) throw new Error(`Model ${model} not found in schema.`);
    return spec;
  }

  protected buildSelect(select?: Select<any>): string {
    if (!select) return "*";
    const parts = [];
    for (let i = 0; i < select.length; i++) {
      parts.push(this.dialect.quote(select[i]!));
    }
    return parts.join(", ");
  }

  protected buildColumnExpr(
    modelName: string,
    fieldName: string,
    path?: string[],
    value?: unknown,
  ): string {
    if (!path || path.length === 0) return this.dialect.quote(fieldName);

    const model = this.getModel(modelName);
    const field = model.fields[fieldName];
    if (field?.type !== "json" && field?.type !== "json[]") {
      throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
    }

    const isNumeric = typeof value === "number";
    const isBoolean = typeof value === "boolean";
    return this.dialect.buildJsonExtract(this.dialect.quote(fieldName), path, isNumeric, isBoolean);
  }

  protected buildWhere(
    model: string,
    where?: Where<any>,
    cursor?: Cursor<any>,
    sortBy?: SortBy<any>[],
    startIndex = 0,
  ): { sql: string; params: unknown[] } {
    const parts: string[] = [];
    const params: unknown[] = [];
    let nextIndex = startIndex;

    if (where) {
      const built = this.buildWhereRecursive(model, where, nextIndex);
      parts.push(`(${built.sql})`);
      for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
      nextIndex += built.params.length;
    }

    if (cursor) {
      const built = this.buildCursor(model, cursor, sortBy, nextIndex);
      if (built.sql) {
        parts.push(`(${built.sql})`);
        for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
        nextIndex += built.params.length;
      }
    }

    return {
      sql: parts.length > 0 ? parts.join(" AND ") : "1=1",
      params,
    };
  }

  private buildWhereRecursive(
    model: string,
    where: Where<any>,
    startIndex: number,
  ): { sql: string; params: unknown[] } {
    if ("and" in where) {
      const parts = [];
      const params = [];
      let currentIdx = startIndex;
      for (let i = 0; i < where.and.length; i++) {
        const built = this.buildWhereRecursive(model, where.and[i]!, currentIdx);
        parts.push(`(${built.sql})`);
        for (let j = 0; j < built.params.length; j++) params.push(built.params[j]);
        currentIdx += built.params.length;
      }
      return { sql: parts.join(" AND "), params };
    }

    if ("or" in where) {
      const parts = [];
      const params = [];
      let currentIdx = startIndex;
      for (let i = 0; i < where.or.length; i++) {
        const built = this.buildWhereRecursive(model, where.or[i]!, currentIdx);
        parts.push(`(${built.sql})`);
        for (let j = 0; j < built.params.length; j++) params.push(built.params[j]);
        currentIdx += built.params.length;
      }
      return { sql: parts.join(" OR "), params };
    }

    const expr = this.buildColumnExpr(model, where.field, where.path, where.value);
    const mappedValue = this.mapWhereValue(where.value);

    switch (where.op) {
      case "eq":
        if (where.value === null) return { sql: `${expr} IS NULL`, params: [] };
        return { sql: `${expr} = ${this.dialect.placeholder(startIndex)}`, params: [mappedValue] };
      case "ne":
        if (where.value === null) return { sql: `${expr} IS NOT NULL`, params: [] };
        return { sql: `${expr} != ${this.dialect.placeholder(startIndex)}`, params: [mappedValue] };
      case "gt":
        return { sql: `${expr} > ${this.dialect.placeholder(startIndex)}`, params: [mappedValue] };
      case "gte":
        return { sql: `${expr} >= ${this.dialect.placeholder(startIndex)}`, params: [mappedValue] };
      case "lt":
        return { sql: `${expr} < ${this.dialect.placeholder(startIndex)}`, params: [mappedValue] };
      case "lte":
        return { sql: `${expr} <= ${this.dialect.placeholder(startIndex)}`, params: [mappedValue] };
      case "in": {
        if (where.value.length === 0) return { sql: "1=0", params: [] };
        const phs = [];
        const inParams = [];
        for (let i = 0; i < where.value.length; i++) {
          phs.push(this.dialect.placeholder(startIndex + i));
          inParams.push(this.mapWhereValue(where.value[i]));
        }
        return { sql: `${expr} IN (${phs.join(", ")})`, params: inParams };
      }
      case "not_in": {
        if (where.value.length === 0) return { sql: "1=1", params: [] };
        const phs = [];
        const inParams = [];
        for (let i = 0; i < where.value.length; i++) {
          phs.push(this.dialect.placeholder(startIndex + i));
          inParams.push(this.mapWhereValue(where.value[i]));
        }
        return { sql: `${expr} NOT IN (${phs.join(", ")})`, params: inParams };
      }
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion, @typescript-eslint/no-unsafe-member-access
    throw new Error(`Unsupported operator: ${String((where as Record<string, unknown>)["op"])}`);
  }

  private buildCursor(
    model: string,
    cursor: Cursor<any>,
    sortBy?: SortBy<any>[],
    startIndex = 0,
  ): { sql: string; params: unknown[] } {
    const cursorValues = cursor.after;
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

    if (criteria.length === 0) return { sql: "", params: [] };

    const orClauses = [];
    const params = [];
    let currentIdx = startIndex;

    for (let i = 0; i < criteria.length; i++) {
      const andClauses = [];
      for (let j = 0; j < i; j++) {
        const prev = criteria[j]!;
        andClauses.push(
          `${this.buildColumnExpr(model, prev.field, prev.path, cursorValues[prev.field])} = ${this.dialect.placeholder(currentIdx++)}`,
        );
        params.push(cursorValues[prev.field]);
      }
      const curr = criteria[i]!;
      const op = curr.direction === "desc" ? "<" : ">";
      // Lexicographic keyset pagination:
      // (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)
      andClauses.push(
        `${this.buildColumnExpr(model, curr.field, curr.path, cursorValues[curr.field])} ${op} ${this.dialect.placeholder(currentIdx++)}`,
      );
      params.push(cursorValues[curr.field]);
      orClauses.push(`(${andClauses.join(" AND ")})`);
    }

    return { sql: `(${orClauses.join(" OR ")})`, params };
  }

  protected mapInput(
    fields: Record<string, Field>,
    data: Record<string, unknown>,
  ): Record<string, unknown> {
    const res: Record<string, unknown> = {};
    const keys = Object.keys(data);
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const val = data[k];
      const spec = fields[k];
      if (val === undefined) continue;
      if (val === null) {
        res[k] = null;
        continue;
      }
      if (spec?.type === "json" || spec?.type === "json[]") {
        res[k] = JSON.stringify(val);
      } else if (spec?.type === "boolean") {
        res[k] = val === true ? 1 : 0;
      } else {
        res[k] = val;
      }
    }
    return res;
  }

  protected mapWhereValue(value: unknown): unknown {
    if (
      value === null ||
      typeof value === "boolean" ||
      typeof value === "number" ||
      typeof value === "string"
    )
      return value;
    return JSON.stringify(value);
  }

  protected mapRow(modelName: string, row: Record<string, unknown>, select?: Select<any>): any {
    const fields = this.getModel(modelName).fields;
    const res: Record<string, unknown> = {};
    const keys = select ?? Object.keys(row);

    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const val = row[k];
      const spec = fields[k];
      if (spec === undefined || val === undefined || val === null) {
        res[k] = val;
        continue;
      }
      if (spec.type === "json" || spec.type === "json[]") {
        res[k] = typeof val === "string" ? JSON.parse(val) : val;
      } else if (spec.type === "boolean") {
        res[k] = val === true || val === 1;
      } else if (spec.type === "number" || spec.type === "timestamp") {
        res[k] = mapNumeric(val);
      } else {
        res[k] = val;
      }
    }
    return res;
  }
}
