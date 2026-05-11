import { type Sql, sql, raw, id, placeholders } from "./sql";

export function selectSql(opts: {
  table: string;
  select: readonly string[] | undefined;
  where: Sql;
  orderBy?: Sql;
  limit?: number;
  offset?: number;
}): Sql {
  const cols = opts.select && opts.select.length > 0 ? id(opts.select) : raw("*");
  let q = sql`SELECT ${cols} FROM ${id(opts.table)} WHERE ${opts.where}`;
  if (opts.orderBy !== undefined) q = sql`${q} ORDER BY ${opts.orderBy}`;
  if (opts.limit !== undefined) q = sql`${q} LIMIT ${opts.limit}`;
  if (opts.offset !== undefined) q = sql`${q} OFFSET ${opts.offset}`;
  return q;
}

export function insertSql(opts: {
  table: string;
  fields: readonly string[];
  values: unknown[];
  returning: readonly string[] | undefined;
}): Sql {
  const cols = opts.returning && opts.returning.length > 0 ? id(opts.returning) : raw("*");
  return sql`INSERT INTO ${id(opts.table)} (${id(opts.fields)}) VALUES (${placeholders(opts.values)}) RETURNING ${cols}`;
}

export function updateSql(opts: {
  table: string;
  set: Sql;
  where: Sql;
  returning?: boolean;
}): Sql {
  const base = sql`UPDATE ${id(opts.table)} SET ${opts.set} WHERE ${opts.where}`;
  return opts.returning === true ? sql`${base} RETURNING *` : base;
}

export function deleteSql(opts: { table: string; where: Sql }): Sql {
  return sql`DELETE FROM ${id(opts.table)} WHERE ${opts.where}`;
}

export function upsertSql(opts: {
  table: string;
  fields: readonly string[];
  values: unknown[];
  conflictColumns: readonly string[];
  onConflict: Sql;
  returning: readonly string[] | undefined;
}): Sql {
  const cols = opts.returning && opts.returning.length > 0 ? id(opts.returning) : raw("*");
  return sql`INSERT INTO ${id(opts.table)} (${id(opts.fields)}) VALUES (${placeholders(opts.values)}) ON CONFLICT (${id(opts.conflictColumns)}) ${opts.onConflict} RETURNING ${cols}`;
}

export function countSql(opts: { table: string; where: Sql }): Sql {
  return sql`SELECT COUNT(*) as count FROM ${id(opts.table)} WHERE ${opts.where}`;
}
