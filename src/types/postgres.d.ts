declare module "postgres" {
  export interface Sql {
    begin<T>(fn: (tx: TransactionSql) => Promise<T>): Promise<T>;
    end(): Promise<void>;
    unsafe<R = Record<string, unknown>[]>(query: string, params?: unknown[]): Promise<R>;
    transaction<T>(fn: (tx: TransactionSql) => Promise<T>): Promise<T>;
  }

  export interface TransactionSql extends Sql {
    savepoint(name: string): TransactionSql;
    unsafe<R = Record<string, unknown>[]>(query: string, params?: unknown[]): Promise<R>;
    transaction<T>(fn: (tx: TransactionSql) => Promise<T>): Promise<T>;
  }

  export { Sql as default };
}
