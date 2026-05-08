import { type Sql, sql, raw, id, placeholders } from "./sql";

export function buildSelectSql(opts: {
  table: string;
  select: readonly string[] | undefined;
  whereClause: Sql;
  orderByClause?: Sql;
  limit?: number;
  offset?: number;
}): Sql {
  const cols = opts.select && opts.select.length > 0 ? id(opts.select) : raw("*");
  let q = sql`SELECT ${cols} FROM ${id(opts.table)} WHERE ${opts.whereClause}`;
  if (opts.orderByClause !== undefined) q = sql`${q} ORDER BY ${opts.orderByClause}`;
  if (opts.limit !== undefined) q = sql`${q} LIMIT ${opts.limit}`;
  if (opts.offset !== undefined) q = sql`${q} OFFSET ${opts.offset}`;
  return q;
}

export function buildInsertSql(opts: {
  table: string;
  fields: readonly string[];
  values: unknown[];
  returning: readonly string[] | undefined;
}): Sql {
  const cols = opts.returning && opts.returning.length > 0 ? id(opts.returning) : raw("*");
  return sql`INSERT INTO ${id(opts.table)} (${id(opts.fields)}) VALUES (${placeholders(opts.values)}) RETURNING ${cols}`;
}

export function buildUpdateSql(opts: {
  table: string;
  setClause: Sql;
  whereClause: Sql;
  returning?: boolean;
}): Sql {
  const base = sql`UPDATE ${id(opts.table)} SET ${opts.setClause} WHERE ${opts.whereClause}`;
  return opts.returning === true ? sql`${base} RETURNING *` : base;
}

export function buildDeleteSql(opts: { table: string; whereClause: Sql }): Sql {
  return sql`DELETE FROM ${id(opts.table)} WHERE ${opts.whereClause}`;
}

export function buildUpsertSql(opts: {
  table: string;
  fields: readonly string[];
  values: unknown[];
  conflictColumns: readonly string[];
  onConflictAction: Sql;
  returning: readonly string[] | undefined;
}): Sql {
  const cols = opts.returning && opts.returning.length > 0 ? id(opts.returning) : raw("*");
  return sql`INSERT INTO ${id(opts.table)} (${id(opts.fields)}) VALUES (${placeholders(opts.values)}) ON CONFLICT (${id(opts.conflictColumns)}) ${opts.onConflictAction} RETURNING ${cols}`;
}

export function buildCountSql(opts: { table: string; whereClause: Sql }): Sql {
  return sql`SELECT COUNT(*) as count FROM ${id(opts.table)} WHERE ${opts.whereClause}`;
}
