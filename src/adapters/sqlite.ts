import { SqliteDialect, createSqliteExecutor, type SqliteDriver } from "../dialects/sqlite";
import { isQueryExecutor, type QueryExecutor } from "../dialects/types";
import type { Adapter, Schema } from "../types";
import { SqlAdapter } from "./sql";

export class SqliteAdapter<S extends Schema = Schema> extends SqlAdapter<S> implements Adapter<S> {
  // Top-level SQLite transactions on one shared connection must be serialized.
  private transactionQueue = Promise.resolve();

  constructor(schema: S, driver: SqliteDriver | QueryExecutor) {
    super(schema, isQueryExecutor(driver) ? driver : createSqliteExecutor(driver), SqliteDialect);
  }

  async transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction(async (innerExecutor) => {
      return fn(new SqliteAdapter(this.schema, innerExecutor));
    });
  }
}
