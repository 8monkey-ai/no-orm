# @8monkey/no-orm

A tiny, composable ORM core for TypeScript libraries. No heavy abstractions, just the primitives to build cross-database tools with full type safety.

## Features

- **Tiny Core**: Minimal overhead, focuses on schema definition and basic CRUD.
- **Full Type Safety**: Inferred models from your schema definition.
- **Adapter-Based**: Switch between SQLite, PostgreSQL (coming soon), and more.
- **Nested JSON Support**: Query and filter nested JSON fields seamlessly.
- **Transactions**: Built-in support for stacked transactions with automatic rollbacks.

## Installation

```bash
bun add @8monkey/no-orm
```

## Quick Start

### 1. Define your Schema

```typescript
import { Schema } from "@8monkey/no-orm";

export const schema = {
  users: {
    fields: {
      id: { type: "string" },
      name: { type: "string" },
      age: { type: "number" },
      metadata: { type: "json", nullable: true },
      tags: { type: "json[]" },
    },
    primaryKey: "id",
    indexes: [
      { field: "age" },
      { field: ["name", "age"], order: "desc" },
    ],
  },
} as const satisfies Schema;
```

### 2. Initialize the Adapter

```typescript
import { SqliteAdapter } from "@8monkey/no-orm/adapters/sqlite";
import { Database } from "bun:sqlite";

const db = new Database(":memory:");
const adapter = new SqliteAdapter(schema, db);

await adapter.migrate();

// You can seamlessly query nested JSON!
const users = await adapter.findMany({
  model: "users",
  where: {
    field: "metadata",
    path: ["settings", "theme"],
    op: "eq",
    value: "dark",
  },
});
```

### 3. Transactions

```typescript
await adapter.transaction(async (tx) => {
  await tx.create({
    model: "users",
    data: { id: "1", name: "Alice", age: 30, tags: ["admin"] },
  });
  
  // Nested transactions use SAVEPOINTs automatically
  await tx.transaction(async (inner) => {
    await inner.update({
      model: "users",
      where: { field: "id", op: "eq", value: "1" },
      data: { age: 31 },
    });
  });
});
```

## Limitations

- **Concurrent Transactions**: If you share a single database connection globally (e.g., a single `bun:sqlite` instance) across concurrent web requests, their `adapter.transaction()` calls will interleave on the same connection. `no-orm` uses `AsyncLocalStorage` to ensure that operations within a transaction block use the correct savepoint state, but for true isolation in highly concurrent environments, consider a connection pool.
- **Upserts on JSON paths**: Upsert operations require the conflict target to be explicitly identifiable (like a `PRIMARY KEY`). `no-orm` does not support using `path` arguments in the `where` clause for `upsert` to prevent ambiguity.

## License

MIT
