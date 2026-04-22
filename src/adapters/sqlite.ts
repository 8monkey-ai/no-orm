import { SqliteDialect, createSqliteExecutor, type SqliteDriver } from "../dialects/sqlite";
import { isQueryExecutor, type QueryExecutor } from "../dialects/types";
import type { Adapter, Schema } from "../types";
import { SqlAdapter } from "./sql";

export class SqliteAdapter<S extends Schema = Schema> extends SqlAdapter<S> implements Adapter<S> {
  constructor(schema: S, driver: SqliteDriver | QueryExecutor) {
    super(schema, isQueryExecutor(driver) ? driver : createSqliteExecutor(driver), SqliteDialect);
  }

  transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction((innerExecutor) => {
      return fn(new SqliteAdapter(this.schema, innerExecutor));
    });
  }
}
