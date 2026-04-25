import type { Cursor, Field, Model, Schema, Select, SortBy, Where } from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildIdentityFilter,
  getIdentityValues,
  getPaginationFilter,
  getPrimaryKeyFields,
  mapNumeric,
} from "./common";

// --- Shared contracts for SQL executors and formatting ---

export interface QueryExecutor {
  all(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
  get(sql: string, params?: unknown[]): Promise<Record<string, unknown> | undefined>;
  run(sql: string, params?: unknown[]): Promise<{ changes: number }>;
  transaction<T>(fn: (executor: QueryExecutor) => Promise<T>): Promise<T>;
  readonly inTransaction: boolean;
}

export interface SqlFormat {
  placeholder(index: number): string;
  quote(identifier: string): string;
  mapFieldType(field: Field): string;
  jsonExtract(column: string, path: string[], isNumeric?: boolean, isBoolean?: boolean): string;
  /** Maps a boolean to its SQL parameter value. Defaults to pass-through if omitted. */
  mapBoolean?(value: boolean): unknown;
  /** Generates result projection, e.g. "RETURNING id, name" */
  formatReturning?(select: string): string;
  /**
   * Generates a full upsert statement.
   * This allows adapters to choose between ON CONFLICT, ON DUPLICATE KEY, or MERGE.
   */
  formatUpsert?(args: {
    table: string;
    insert: { columns: string[]; placeholders: string[] };
    update?: { assignments: string[]; where?: string };
    conflict: { columns: string[] };
    returning?: string;
  }): string;
}

export function isQueryExecutor(obj: unknown): obj is QueryExecutor {
  if (typeof obj !== "object" || obj === null) return false;
  return (
    "all" in obj &&
    "run" in obj &&
    typeof (obj as Record<string, unknown>)["all"] === "function" &&
    typeof (obj as Record<string, unknown>)["run"] === "function"
  );
}

// --- SQL Builders & Mappers ---

function getReturning(fmt: SqlFormat, select: string): string {
  if (fmt.formatReturning) return fmt.formatReturning(select);
  return `RETURNING ${select}`;
}

function getUpsert(
  fmt: SqlFormat,
  args: {
    table: string;
    insert: { columns: string[]; placeholders: string[] };
    update?: { assignments: string[]; where?: string };
    conflict: { columns: string[] };
    returning?: string;
  },
): string {
  if (fmt.formatUpsert) return fmt.formatUpsert(args);

  const conflict = `ON CONFLICT (${args.conflict.columns.join(", ")})`;
  let updateAction = "DO NOTHING";
  if (args.update) {
    updateAction = `DO UPDATE SET ${args.update.assignments.join(", ")}`;
    if (args.update.where !== undefined && args.update.where !== "") {
      updateAction += ` WHERE ${args.update.where}`;
    }
  }

  let sql = `INSERT INTO ${fmt.quote(args.table)} (${args.insert.columns.join(", ")}) VALUES (${args.insert.placeholders.join(", ")}) ${conflict} ${updateAction}`;
  if (args.returning !== undefined && args.returning !== "") {
    sql += ` ${args.returning}`;
  }
  return sql;
}

export function toSelect(fmt: SqlFormat, select?: Select<Record<string, unknown>>): string {
  if (!select) return "*";
  const parts: string[] = [];
  for (let i = 0; i < select.length; i++) {
    parts.push(fmt.quote(select[i]!));
  }
  return parts.join(", ");
}

export function toInput(
  fields: Record<string, Field>,
  data: Record<string, unknown>,
  fmt: SqlFormat,
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
      res[k] = fmt.mapBoolean ? fmt.mapBoolean(val === true) : val;
    } else {
      res[k] = val;
    }
  }
  return res;
}

export function toRow<T extends Record<string, unknown>>(
  model: Model,
  row: Record<string, unknown>,
  select?: Select<Record<string, unknown>>,
): T {
  const fields = model.fields;
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
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
  return res as T;
}

function toColumnExpr(
  fmt: SqlFormat,
  model: Model,
  fieldName: string,
  path?: string[],
  value?: unknown,
): string {
  if (!path || path.length === 0) return fmt.quote(fieldName);

  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }

  const isNumeric = typeof value === "number";
  const isBoolean = typeof value === "boolean";
  return fmt.jsonExtract(fmt.quote(fieldName), path, isNumeric, isBoolean);
}

function mapWhereValue(fmt: SqlFormat, val: unknown): unknown {
  if (val === null) return null;
  if (typeof val === "boolean") return fmt.mapBoolean ? fmt.mapBoolean(val) : val;
  if (typeof val === "number" || typeof val === "string") return val;
  return JSON.stringify(val);
}

function toWhereRecursive(
  fmt: SqlFormat,
  model: Model,
  where: Where,
  startIndex: number,
): { sql: string; params: unknown[] } {
  if ("and" in where) {
    const parts = [];
    const params = [];
    let currentIdx = startIndex;
    for (let i = 0; i < where.and.length; i++) {
      const built = toWhereRecursive(fmt, model, where.and[i]!, currentIdx);
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
      const built = toWhereRecursive(fmt, model, where.or[i]!, currentIdx);
      parts.push(`(${built.sql})`);
      for (let j = 0; j < built.params.length; j++) params.push(built.params[j]);
      currentIdx += built.params.length;
    }
    return { sql: parts.join(" OR "), params };
  }

  const expr = toColumnExpr(fmt, model, where.field, where.path, where.value);
  const mappedValue = mapWhereValue(fmt, where.value);

  switch (where.op) {
    case "eq":
      if (where.value === null) return { sql: `${expr} IS NULL`, params: [] };
      return { sql: `${expr} = ${fmt.placeholder(startIndex)}`, params: [mappedValue] };
    case "ne":
      if (where.value === null) return { sql: `${expr} IS NOT NULL`, params: [] };
      return { sql: `${expr} != ${fmt.placeholder(startIndex)}`, params: [mappedValue] };
    case "gt":
      return { sql: `${expr} > ${fmt.placeholder(startIndex)}`, params: [mappedValue] };
    case "gte":
      return { sql: `${expr} >= ${fmt.placeholder(startIndex)}`, params: [mappedValue] };
    case "lt":
      return { sql: `${expr} < ${fmt.placeholder(startIndex)}`, params: [mappedValue] };
    case "lte":
      return { sql: `${expr} <= ${fmt.placeholder(startIndex)}`, params: [mappedValue] };
    case "in": {
      if (where.value.length === 0) return { sql: "1=0", params: [] };
      const phs = [];
      const inParams = [];
      for (let i = 0; i < where.value.length; i++) {
        phs.push(fmt.placeholder(startIndex + i));
        inParams.push(mapWhereValue(fmt, where.value[i]));
      }
      return { sql: `${expr} IN (${phs.join(", ")})`, params: inParams };
    }
    case "not_in": {
      if (where.value.length === 0) return { sql: "1=1", params: [] };
      const phs = [];
      const inParams = [];
      for (let i = 0; i < where.value.length; i++) {
        phs.push(fmt.placeholder(startIndex + i));
        inParams.push(mapWhereValue(fmt, where.value[i]));
      }
      return { sql: `${expr} NOT IN (${phs.join(", ")})`, params: inParams };
    }
    default:
      throw new Error(`Unsupported operator: ${String((where as Record<string, unknown>)["op"])}`);
  }
}

export function toWhere(
  fmt: SqlFormat,
  model: Model,
  where?: Where,
  cursor?: Cursor,
  sortBy?: SortBy[],
  startIndex = 0,
): { sql: string; params: unknown[] } {
  const parts: string[] = [];
  const params: unknown[] = [];
  let nextIndex = startIndex;

  if (where) {
    const built = toWhereRecursive(fmt, model, where, nextIndex);
    parts.push(`(${built.sql})`);
    for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
    nextIndex += built.params.length;
  }

  if (cursor) {
    const paginationWhere = getPaginationFilter(cursor, sortBy);
    if (paginationWhere) {
      const built = toWhereRecursive(fmt, model, paginationWhere, nextIndex);
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

// --- Functional Helpers ---

export async function migrate(exec: QueryExecutor, schema: Schema, fmt: SqlFormat): Promise<void> {
  const models = Object.entries(schema);

  // Create tables first, then indexes — indexes depend on tables existing.
  // DDL must be sequential: some drivers don't support concurrent DDL on one connection.
  for (let i = 0; i < models.length; i++) {
    const [name, model] = models[i]!;
    const fields = Object.entries(model.fields);
    const columns: string[] = [];
    for (let j = 0; j < fields.length; j++) {
      const [fieldName, field] = fields[j]!;
      const nullable = field.nullable === true ? "" : " NOT NULL";
      columns.push(`${fmt.quote(fieldName)} ${fmt.mapFieldType(field)}${nullable}`);
    }

    const pkFields = getPrimaryKeyFields(model);
    const quotedPkFields: string[] = [];
    for (let j = 0; j < pkFields.length; j++) {
      quotedPkFields.push(fmt.quote(pkFields[j]!));
    }
    const pk = `PRIMARY KEY (${quotedPkFields.join(", ")})`;

    // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
    await exec.run(
      `CREATE TABLE IF NOT EXISTS ${fmt.quote(name)} (${columns.join(", ")}, ${pk})`,
      [],
    );
  }

  // Now create indexes
  for (let i = 0; i < models.length; i++) {
    const [name, model] = models[i]!;
    if (!model.indexes) continue;

    for (let j = 0; j < model.indexes.length; j++) {
      const index = model.indexes[j]!;
      const indexFields = Array.isArray(index.field) ? index.field : [index.field];
      const formattedFields: string[] = [];
      for (let k = 0; k < indexFields.length; k++) {
        formattedFields.push(
          `${fmt.quote(indexFields[k]!)}${index.order ? ` ${index.order.toUpperCase()}` : ""}`,
        );
      }
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      await exec.run(
        `CREATE INDEX IF NOT EXISTS ${fmt.quote(`idx_${name}_${j}`)} ON ${fmt.quote(name)} (${formattedFields.join(", ")})`,
        [],
      );
    }
  }
}

export async function create<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { data: T; select?: Select<T> },
): Promise<T> {
  const { data, select } = args;
  const insertData = toInput(model.fields, data, fmt);
  const fields = Object.keys(insertData);

  const quotedFields: string[] = [];
  const placeholders: string[] = [];
  const values: unknown[] = [];

  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]!;
    quotedFields.push(fmt.quote(field));
    placeholders.push(fmt.placeholder(i));
    values.push(insertData[field]);
  }

  const sqlSelect = toSelect(
    fmt,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Select<T> is a subtype of string[] because model keys are strings
    select as Select<Record<string, unknown>>,
  );
  const sql = `INSERT INTO ${fmt.quote(table)} (${quotedFields.join(", ")}) VALUES (${placeholders.join(", ")}) ${getReturning(fmt, sqlSelect)}`;
  const row = await exec.get(sql, values);

  if (!row) {
    const result = await find(exec, table, model, fmt, {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- field names in source T match model fields at runtime
      where: buildIdentityFilter(model, data) as Where<T>,
      select,
    });
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- result is T (or null) from find(), and we just inserted it
    return result as T;
  }

  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- row is mapped to T via toRow
  return toRow(model, row, select as Select<Record<string, unknown>>);
}

export async function find<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { where: Where<T>; select?: Select<T> },
): Promise<T | null> {
  const { where, select } = args;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
  const built = toWhere(fmt, model, where as Where);
  const sql = `SELECT ${toSelect(
    fmt,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Select<T> is a subtype of string[] because model keys are strings
    select as Select<Record<string, unknown>>,
  )} FROM ${fmt.quote(table)} WHERE ${built.sql} LIMIT 1`;
  const row = await exec.get(sql, built.params);
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- row is mapped to T via toRow
  return row ? toRow(model, row, select as Select<Record<string, unknown>>) : null;
}

export async function findMany<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: {
    where?: Where<T>;
    select?: Select<T>;
    sortBy?: SortBy<T>[];
    limit?: number;
    offset?: number;
    cursor?: Cursor<T>;
  },
): Promise<T[]> {
  const { where, select, sortBy, limit, offset, cursor } = args;
  const params: unknown[] = [];
  const sqlSelect = toSelect(
    fmt,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Select<T> is a subtype of string[] because model keys are strings
    select as Select<Record<string, unknown>>,
  );
  let sql = `SELECT ${sqlSelect} FROM ${fmt.quote(table)}`;

  const built = toWhere(
    fmt,
    model,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
    where as Where,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Cursor<T> is compatible with Cursor for SQL generation
    cursor as Cursor,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- SortBy<T>[] is compatible with SortBy[] for SQL generation
    sortBy as SortBy[] | undefined,
  );
  if (built.sql !== "1=1") {
    sql += ` WHERE ${built.sql}`;
    for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
  }

  if (sortBy && sortBy.length > 0) {
    const sortParts: string[] = [];
    for (let i = 0; i < sortBy.length; i++) {
      const s = sortBy[i]!;
      sortParts.push(
        `${toColumnExpr(fmt, model, s.field, s.path)} ${(s.direction ?? "asc").toUpperCase()}`,
      );
    }
    sql += ` ORDER BY ${sortParts.join(", ")}`;
  }

  if (limit !== undefined) {
    sql += ` LIMIT ${fmt.placeholder(params.length)}`;
    params.push(limit);
  }

  if (offset !== undefined) {
    sql += ` OFFSET ${fmt.placeholder(params.length)}`;
    params.push(offset);
  }

  const rows = await exec.all(sql, params);
  const result: T[] = [];
  for (let i = 0; i < rows.length; i++) {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- rows are mapped to T via toRow
    result.push(toRow(model, rows[i]!, select as Select<Record<string, unknown>>));
  }
  return result;
}

export async function update<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { where: Where<T>; data: Partial<T> },
): Promise<T | null> {
  const { where, data } = args;
  assertNoPrimaryKeyUpdates(model, data);

  const updateData = toInput(model.fields, data, fmt);
  const fields = Object.keys(updateData);
  if (fields.length === 0) return find(exec, table, model, fmt, { where });

  const assignments: string[] = [];
  const params: unknown[] = [];
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]!;
    assignments.push(`${fmt.quote(field)} = ${fmt.placeholder(i)}`);
    params.push(updateData[field]);
  }

  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
  const built = toWhere(fmt, model, where as Where, undefined, undefined, params.length);
  const sql = `UPDATE ${fmt.quote(table)} SET ${assignments.join(", ")} WHERE ${built.sql} ${getReturning(fmt, "*")}`;
  for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);

  const row = await exec.get(sql, params);
  if (!row) return find(exec, table, model, fmt, { where });
  return toRow(model, row);
}

export async function updateMany<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { where?: Where<T>; data: Partial<T> },
): Promise<number> {
  const { where, data } = args;
  assertNoPrimaryKeyUpdates(model, data);

  const updateData = toInput(model.fields, data, fmt);
  const fields = Object.keys(updateData);
  if (fields.length === 0) return 0;

  const assignments: string[] = [];
  const params: unknown[] = [];
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]!;
    assignments.push(`${fmt.quote(field)} = ${fmt.placeholder(i)}`);
    params.push(updateData[field]);
  }

  let sql = `UPDATE ${fmt.quote(table)} SET ${assignments.join(", ")}`;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
  const built = toWhere(fmt, model, where as Where, undefined, undefined, params.length);
  if (built.sql !== "1=1") {
    sql += ` WHERE ${built.sql}`;
    for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
  }

  const res = await exec.run(sql, params);
  return res.changes;
}

export async function upsert<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: {
    create: T;
    update: Partial<T>;
    where?: Where<T>;
    select?: Select<T>;
  },
): Promise<T> {
  const { create: cData, update: uData, where, select } = args;
  assertNoPrimaryKeyUpdates(model, uData);

  const createData = toInput(model.fields, cData, fmt);
  const createFields = Object.keys(createData);
  const updateData = toInput(model.fields, uData, fmt);
  const updateFields = Object.keys(updateData);
  const pkFields = getPrimaryKeyFields(model);

  const insertColumns: string[] = [];
  const insertPlaceholders: string[] = [];
  const params: unknown[] = [];
  for (let i = 0; i < createFields.length; i++) {
    const field = createFields[i]!;
    insertColumns.push(fmt.quote(field));
    insertPlaceholders.push(fmt.placeholder(i));
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
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
    const built = toWhere(fmt, model, where as Where, undefined, undefined, params.length);
    whereSql = built.sql;
    for (let i = 0; i < built.params.length; i++) params.push(built.params[i]);
  }

  const sqlSelect = toSelect(
    fmt,
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Select<T> is a subtype of string[] because model keys are strings
    select as Select<Record<string, unknown>>,
  );

  const assignments: string[] = [];
  for (let i = 0; i < updateFields.length; i++) {
    const field = updateFields[i]!;
    assignments.push(`${fmt.quote(field)} = ${fmt.placeholder(createFields.length + i)}`);
  }

  const sql = getUpsert(fmt, {
    table,
    insert: { columns: insertColumns, placeholders: insertPlaceholders },
    update: updateFields.length > 0 ? { assignments, where: whereSql || undefined } : undefined,
    conflict: { columns: pkFields.map((f) => fmt.quote(f)) },
    returning: getReturning(fmt, sqlSelect),
  });

  const row = await exec.get(sql, params);
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- row is mapped to T via toRow
  if (row) return toRow(model, row, select as Select<Record<string, unknown>>);

  const identityValues = getIdentityValues(model, cData);
  const existing = await find(exec, table, model, fmt, {
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- field names in identityValues match model fields at runtime
    where: buildIdentityFilter(model, identityValues) as Where<T>,
    select,
  });
  if (!existing) throw new Error("Failed to refetch upserted record.");
  return existing;
}

export async function remove<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { where: Where<T> },
): Promise<void> {
  const { where } = args;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
  const built = toWhere(fmt, model, where as Where);
  await exec.run(`DELETE FROM ${fmt.quote(table)} WHERE ${built.sql}`, built.params);
}

export async function removeMany<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { where?: Where<T> },
): Promise<number> {
  const { where } = args;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
  const built = toWhere(fmt, model, where as Where);
  let sql = `DELETE FROM ${fmt.quote(table)}`;
  if (built.sql !== "1=1") sql += ` WHERE ${built.sql}`;
  const res = await exec.run(sql, built.params);
  return res.changes;
}

export async function count<T extends Record<string, unknown>>(
  exec: QueryExecutor,
  table: string,
  model: Model,
  fmt: SqlFormat,
  args: { where?: Where<T> },
): Promise<number> {
  const { where } = args;
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- Where<T> is compatible with Where for SQL generation
  const built = toWhere(fmt, model, where as Where);
  let sql = `SELECT COUNT(*) as count FROM ${fmt.quote(table)}`;
  if (built.sql !== "1=1") sql += ` WHERE ${built.sql}`;
  const row = await exec.get(sql, built.params);
  if (!row) return 0;
  const val = row["count"];
  return typeof val === "number" ? val : Number(val ?? 0);
}

/**
 * FUTURE EXTENSION: GreptimeDB & MySQL
 *
 * Lessons learned from hebo-gateway and typical SQL quirks:
 *
 * 1. GreptimeDB:
 *    - Supports multiple protocols (Postgres, MySQL). When using Postgres wire protocol
 *      (via `PostgresAdapter`), use double quotes (") for identifiers. When using MySQL
 *      protocol, use backticks (`).
 *    - Mutation responses over Bun.SQL might not populate `count` but provide a `command` string
 *      like "OK 1". Parsed in `postgres.ts`.
 *    - JSON strings can contain Rust-style Unicode escapes (\u{xxxx}) which are invalid JSON.
 *      May need a `mapJsonOutput` hook in `SqlFormat`. Empty JSON strings ("") or "{}"
 *      should be normalized to {}. To avoid driver crashes, JSON columns might need
 *      to be cast to STRING on the wire (e.g. `col::STRING`).
 *    - DDL: `TIME INDEX` is mandatory. May also need `PARTITION BY`, `WITH` (e.g. merge_mode),
 *      or `SKIPPING INDEX` for performance optimizations.
 *    - JSON: Specialized functions like `json_get_string` might be required instead of
 *      standard Postgres operators like `->>`.
 *    - Batch inserts: When using a time index as part of uniqueness, adding a slight offset
 *      (e.g. +1ms) per item in a batch avoids timestamp collisions.
 *
 * 2. MySQL / MariaDB:
 *    - Quoting uses backticks (`) instead of double quotes ("). Note that backticks
 *      within identifiers might need to be escaped by doubling them (``).
 *    - Upsert uses `ON DUPLICATE KEY UPDATE` instead of `ON CONFLICT`.
 *    - Does not support `CREATE INDEX IF NOT EXISTS`. Must be handled via try/catch in `migrate`.
 *    - Does not support `RETURNING`. `create` and `upsert` must fall back to a second `find` call.
 *
 * 3. General SQL:
 *    - Some databases don't support parameters in `LIMIT` clauses. May need a `limitAsLiteral` flag.
 *    - May need to override `toInput`/`toRow` logic if per-driver parameter mapping is needed
 *      (e.g. Date to Number/BigInt, or BigInt to Number).
 *    - Indexing: Different types like `BRIN` might not support `ASC`/`DESC` or require `USING` syntax.
 */
