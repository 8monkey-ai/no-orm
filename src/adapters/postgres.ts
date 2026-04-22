import { PostgresDialect, createPostgresExecutor, type PostgresDriver } from "../dialects/postgres";
import { isQueryExecutor, type QueryExecutor } from "../dialects/types";
import type { Adapter, Schema } from "../types";
import { SqlAdapter } from "./sql";

export class PostgresAdapter<S extends Schema = Schema>
  extends SqlAdapter<S>
  implements Adapter<S>
{
  constructor(schema: S, driver: PostgresDriver | QueryExecutor) {
    super(
      schema,
      isQueryExecutor(driver) ? driver : createPostgresExecutor(driver),
      PostgresDialect,
    );
  }

  async transaction<T>(fn: (tx: Adapter<S>) => Promise<T>): Promise<T> {
    return this.executor.transaction(async (innerExecutor) => {
      return fn(new PostgresAdapter(this.schema, innerExecutor));
    });
  }
}
