import type { Database as BunDatabase } from "bun:sqlite";

import type { Database as BetterSqlite3Database } from "better-sqlite3";
import type { Database as SqliteDatabase } from "sqlite";

import type {
  Adapter,
  Field,
  InferModel,
  Schema,
  Select,
  SortBy,
  Where,
  Cursor,
  Model,
} from "../types";
import {
  assertNoPrimaryKeyUpdates,
  buildPrimaryKeyFilter,
  getPrimaryKeyFields,
  getPrimaryKeyValues,
} from "./utils/common";
import {
  type QueryExecutor,
  isQueryExecutor,
  toRow,
  toDbRow,
  type Fragment,
  join,
  wrap,
  where,
  set,
  sort,
} from "./utils/sql";

export type SqliteDriver = SqliteDatabase | BunDatabase | BetterSqlite3Database;

/**
 * Limits the number of prepared statement objects kept in memory to prevent leaks
 * while allowing statement reuse for performance.
 */
const MAX_CACHED_STATEMENTS = 100;

// --- Internal SQLite Syntax Helpers ---

const quote = (s: string) => `"${s}"`;

const mapSqliteValue = (val: unknown, spec?: Field) => {
  if (spec?.type === "boolean" || (spec === undefined && typeof val === "boolean")) {
    return val === true ? 1 : 0;
  }
  return val;
};

function mapFieldType(field: Field): string {
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
      return "TEXT"; // SQLite stores JSON as plain text
    default:
      return "TEXT";
  }
}

function serializeJsonPath(path: string[]): string {
  let jsonPath = "$";
  for (let i = 0; i < path.length; i++) {
    const segment = path[i]!;
    let isIndex = true;
    if (segment.length === 0) isIndex = false;
    else {
      for (let j = 0; j < segment.length; j++) {
        const c = segment.codePointAt(j);
        if (c === undefined || c < 48 || c > 57) {
          isIndex = false;
          break;
        }
      }
    }
    if (isIndex) jsonPath += `[${segment}]`;
    else jsonPath += `.${segment}`;
  }
  return jsonPath;
}

function toColumnExpr(model: Model, fieldName: string, path?: string[]): Fragment {
  const quoted = quote(fieldName);
  if (!path || path.length === 0) return { strings: [quoted], params: [] };
  const field = model.fields[fieldName];
  if (field?.type !== "json" && field?.type !== "json[]") {
    throw new Error(`Cannot use JSON path on non-JSON field: ${fieldName}`);
  }
  return { strings: [`json_extract(${quoted}, ?)`], params: [serializeJsonPath(path)] };
}

// --- Driver detection and executors ---

function isSyncSqlite(driver: SqliteDriver): driver is BunDatabase | BetterSqlite3Database {
  return "prepare" in driver && !("all" in driver);
}

type SyncStatement = {
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): { changes: number };
};

interface SyncDriver {
  prepare(sql: string): SyncStatement;
}

function createSyncSqliteExecutor(driver: SyncDriver, inTransaction = false): QueryExecutor {
  const cache = new Map<string, SyncStatement>();

  function getStmt(sql: string): SyncStatement {
    let stmt = cache.get(sql);
    if (stmt === undefined) {
      if (cache.size >= MAX_CACHED_STATEMENTS) {
        const first = cache.keys().next();
        if (first.done !== true) cache.delete(first.value);
      }
      stmt = driver.prepare(sql);
      cache.set(sql, stmt);
    }
    return stmt;
  }

  return {
    all: (query) => {
      const { strings, params } = query;
      const sql = strings.join("?");
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver result row matches Record shape
      return Promise.resolve(getStmt(sql).all(...params) as Record<string, unknown>[]);
    },
    get: (query) => {
      const { strings, params } = query;
      const sql = strings.join("?");
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver returns either a row object or undefined
      return Promise.resolve(getStmt(sql).get(...params) as Record<string, unknown> | undefined);
    },
    run: (query) => {
      const { strings, params } = query;
      const sql = strings.join("?");
      const res = getStmt(sql).run(...params);
      return Promise.resolve({ changes: res.changes });
    },
    transaction: async (fn) => {
      getStmt("BEGIN").run();
      try {
        const res = await fn(createSyncSqliteExecutor(driver, true));
        getStmt("COMMIT").run();
        return res;
      } catch (e) {
        getStmt("ROLLBACK").run();
        throw e;
      }
    },
    inTransaction,
  };
}

function createAsyncSqliteExecutor(driver: SqliteDatabase, inTransaction = false): QueryExecutor {
  return {
    // eslint-disable-next-line typescript-eslint/no-unsafe-return -- async driver returns rows
    all: (query) => driver.all(query.strings.join("?"), query.params),
    // eslint-disable-next-line typescript-eslint/no-unsafe-return -- async driver returns row
    get: (query) => driver.get(query.strings.join("?"), query.params),
    run: async (query) => {
      const res = await driver.run(query.strings.join("?"), query.params);
      return { changes: res.changes ?? 0 };
    },
    transaction: async (fn) => {
      await driver.run("BEGIN");
      try {
        const res = await fn(createAsyncSqliteExecutor(driver, true));
        await driver.run("COMMIT");
        return res;
      } catch (e) {
        await driver.run("ROLLBACK");
        throw e;
      }
    },
    inTransaction,
  };
}

function createSqliteExecutor(driver: SqliteDriver): QueryExecutor {
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally checked in isSyncSqlite
  if (isSyncSqlite(driver)) return createSyncSqliteExecutor(driver as unknown as SyncDriver);
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- driver is structurally checked
  return createAsyncSqliteExecutor(driver as SqliteDatabase);
}

// --- Adapter ---

/**
 * SQLite Adapter for no-orm.
 */
export class SqliteAdapter<S extends Schema> implements Adapter<S> {
  private executor: QueryExecutor;

  constructor(
    private schema: S,
    driver: SqliteDriver | QueryExecutor,
  ) {
    this.executor = isQueryExecutor(driver) ? driver : createSqliteExecutor(driver);
  }

  private getOptions(model: Model) {
    return {
      model,
      columnExpr: (f: string, p?: string[]) => toColumnExpr(model, f, p),
      mapValue: mapSqliteValue,
    };
  }

  async migrate(): Promise<void> {
    const models = Object.entries(this.schema);

    // Create tables first, then indexes — indexes depend on tables existing.
    // DDL must be sequential: some drivers don't support concurrent DDL on one connection.
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      const fields = Object.entries(model.fields);
      const columns = fields.map(
        ([fname, f]) =>
          `${quote(fname)} ${mapFieldType(f)}${f.nullable === true ? "" : " NOT NULL"}`,
      );
      const primaryKeyFields = getPrimaryKeyFields(model);
      const pk = `PRIMARY KEY (${primaryKeyFields.map((f) => quote(f)).join(", ")})`;
      // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
      await this.executor.run({
        strings: [`CREATE TABLE IF NOT EXISTS ${quote(name)} (${columns.join(", ")}, ${pk})`],
        params: [],
      });
    }

    // Now create indexes
    for (let i = 0; i < models.length; i++) {
      const [name, model] = models[i]!;
      if (!model.indexes) continue;
      for (let j = 0; j < model.indexes.length; j++) {
        const idx = model.indexes[j]!;
        const fields = Array.isArray(idx.field) ? idx.field : [idx.field];
        const formatted = fields.map(
          (f) => `${quote(f)}${idx.order ? ` ${idx.order.toUpperCase()}` : ""}`,
        );
        // eslint-disable-next-line no-await-in-loop -- DDL is intentionally sequential
        await this.executor.run({
          strings: [
            `CREATE INDEX IF NOT EXISTS ${quote(`idx_${name}_${j}`)} ON ${quote(name)} (${formatted.join(", ")})`,
          ],
          params: [],
        });
      }
    }
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    if (this.executor.inTransaction) return fn(this);
    return this.executor.transaction((exec) => fn(new SqliteAdapter(this.schema, exec)));
  }

  async create<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: T; select?: Select<T> }): Promise<T> {
    const { model: modelName, data, select } = args;
    const model = this.schema[modelName]!;
    const input = toDbRow(model, data, mapSqliteValue);
    const fields = Object.keys(input);
    const sqlFields = fields.map((f) => quote(f)).join(", ");
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";

    const strings = [`INSERT INTO ${quote(modelName)} (${sqlFields}) VALUES (`];
    for (let i = 1; i < fields.length; i++) strings.push(", ");
    strings.push(`) RETURNING ${sqlSelect}`);
    const params = fields.map((f) => input[f]);

    const row = await this.executor.get({ strings, params });
    if (row === undefined || row === null) {
      const res = await this.find({
        model: modelName,
        where: buildPrimaryKeyFilter<T>(model, getPrimaryKeyValues(model, data)),
        select,
      });
      if (!res) throw new Error("Failed to insert record");
      return res;
    }
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row, select);
  }

  async find<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T>; select?: Select<T> }): Promise<T | null> {
    const { model: modelName, select } = args;
    const model = this.schema[modelName]!;
    const built = where(args.where, this.getOptions(model));
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";

    const query = wrap(built, `SELECT ${sqlSelect} FROM ${quote(modelName)} WHERE `, " LIMIT 1");

    const row = await this.executor.get(query);
    if (row === undefined || row === null) return null;
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
    return toRow<T>(model, row, select);
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
    const { model: modelName, select, sortBy, limit, offset, cursor } = args;
    const model = this.schema[modelName]!;
    const opts = this.getOptions(model);
    const built = where(args.where, Object.assign({}, opts, { cursor, sortBy }));
    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";

    let query = wrap(built, `SELECT ${sqlSelect} FROM ${quote(modelName)} WHERE `, "");

    if (sortBy && sortBy.length > 0) {
      query = join([query, sort(sortBy, opts.columnExpr)], " ORDER BY ");
    }
    if (limit !== undefined) {
      query.strings[query.strings.length - 1] += " LIMIT ";
      query.strings.push("");
      query.params.push(limit);
    }
    if (offset !== undefined) {
      query.strings[query.strings.length - 1] += " OFFSET ";
      query.strings.push("");
      query.params.push(offset);
    }
    const rows = await this.executor.all(query);

    const result: T[] = [];
    for (let i = 0; i < rows.length; i++) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
      result.push(toRow<T>(model, rows[i]!, select));
    }
    return result;
  }

  /**
   * Updates the first record matching the criteria. Primary key updates are rejected.
   */
  async update<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; data: Partial<T>; where: Where<T> }): Promise<T | null> {
    const { model: modelName, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const input = toDbRow(model, data, mapSqliteValue);
    const fields = Object.keys(input);

    if (fields.length === 0)
      return this.find({ model: modelName, where: args.where, select: undefined });

    const setFrag = set(input, quote);
    const whereFrag = where(args.where, this.getOptions(model));
    const query = join(
      [wrap(setFrag, `UPDATE ${quote(modelName)} SET `, ""), whereFrag],
      " WHERE ",
    );
    query.strings[query.strings.length - 1] += " RETURNING *";

    const row = await this.executor.get(query);
    if (row === undefined || row === null)
      return this.find({ model: modelName, where: args.where });
    // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
    return toRow<T>(model, row);
  }

  /**
   * Updates all records matching the criteria. Primary key updates are rejected.
   */
  async updateMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T>; data: Partial<T> }): Promise<number> {
    const { model: modelName, data } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, data);
    const input = toDbRow(model, data, mapSqliteValue);
    const fields = Object.keys(input);
    if (fields.length === 0) return 0;

    const setFrag = set(input, quote);
    const whereFrag = where(args.where, this.getOptions(model));
    const query = join(
      [wrap(setFrag, `UPDATE ${quote(modelName)} SET `, ""), whereFrag],
      " WHERE ",
    );

    const res = await this.executor.run(query);
    return res.changes;
  }

  /**
   * Performs an atomic insert-or-update.
   *
   * Conflicts are always handled on the Primary Key. If `where` is provided, the record
   * is only updated if the condition is met (acting as a predicate). Primary key
   * updates are rejected.
   */
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
    const { model: modelName, create: cData, update: uData, select } = args;
    const model = this.schema[modelName]!;
    assertNoPrimaryKeyUpdates(model, uData);

    const insertRow = toDbRow(model, cData, mapSqliteValue);
    const cFields = Object.keys(insertRow);
    const updateRow = toDbRow(model, uData, mapSqliteValue);
    const uFields = Object.keys(updateRow);
    const primaryKeyFields = getPrimaryKeyFields(model);

    const sqlFields = cFields.map((f) => quote(f)).join(", ");
    const sqlConflict = primaryKeyFields.map((f) => quote(f)).join(", ");

    const strings = [`INSERT INTO ${quote(modelName)} (${sqlFields}) VALUES (`];
    const params = [];

    for (let i = 0; i < cFields.length; i++) {
      if (i > 0) strings.push(", ");
      params.push(insertRow[cFields[i]!]);
    }
    strings.push(`) ON CONFLICT (${sqlConflict}) `);

    let query: Fragment = { strings, params };

    if (uFields.length > 0) {
      const setFrag = set(updateRow, quote);
      query.strings[query.strings.length - 1] += "DO UPDATE SET ";
      query = join([query, setFrag], "");

      if (args.where) {
        const built = where(args.where, this.getOptions(model));
        query.strings[query.strings.length - 1] += " WHERE ";
        query = join([query, built], "");
      }
    } else {
      query.strings[query.strings.length - 1] += "DO NOTHING";
    }

    const sqlSelect = select ? select.map((s) => quote(s)).join(", ") : "*";
    query.strings[query.strings.length - 1] += ` RETURNING ${sqlSelect}`;

    const row = await this.executor.get(query);
    if (row !== undefined && row !== null) {
      // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select matches model fields at runtime
      return toRow<T>(model, row, select);
    }

    const existing = await this.find({
      model: modelName,
      where: buildPrimaryKeyFilter<T>(model, getPrimaryKeyValues(model, cData)),
      select,
    });
    if (existing === null) throw new Error("Failed to refetch record after upsert");
    return existing;
  }

  async delete<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where: Where<T> }): Promise<void> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const built = where(args.where, this.getOptions(model));
    const query = wrap(built, `DELETE FROM ${quote(modelName)} WHERE `, "");
    await this.executor.run(query);
  }

  async deleteMany<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const built = where(args.where, this.getOptions(model));
    const query = wrap(built, `DELETE FROM ${quote(modelName)} WHERE `, "");
    const res = await this.executor.run(query);
    return res.changes;
  }

  async count<
    K extends keyof S & string,
    T extends Record<string, unknown> = InferModel<S[K]>,
  >(args: { model: K; where?: Where<T> }): Promise<number> {
    const { model: modelName } = args;
    const model = this.schema[modelName]!;
    const built = where(args.where, this.getOptions(model));
    const query = wrap(built, `SELECT COUNT(*) as count FROM ${quote(modelName)} WHERE `, "");
    const row = await this.executor.get(query);
    return Number(row?.["count"] ?? 0);
  }
}
