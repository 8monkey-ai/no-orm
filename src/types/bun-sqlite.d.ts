declare module "bun:sqlite" {
  export interface Statement {
    all(...params: unknown[]): unknown[];
    get(...params: unknown[]): unknown;
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  }

  export interface Database {
    query<T>(sql: string): {
      all: (...params: unknown[]) => T[];
      get: (...params: unknown[]) => T | undefined;
      run: (...params: unknown[]) => { changes: number; lastInsertRowid: number | bigint };
    };
    exec(sql: string): void;
    prepare: (sql: string) => Statement;
    transaction<T>(fn: () => T): () => T;
  }

  export function Database(filename?: string): Database;
}
