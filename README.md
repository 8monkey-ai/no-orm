# @8monkey/no-orm

A tiny, schema-first persistence core for TypeScript libraries.

`no-orm` is intentionally small:

- one canonical schema shape
- inferred TypeScript model types
- adapter-based persistence
- minimal CRUD, filtering, ordering, pagination, and transactions

It is not a query builder, migration framework, or full ORM runtime.

## Installation

```bash
npm install @8monkey/no-orm
# or
bun add @8monkey/no-orm
```

## Define a Schema

```ts
import type { InferModel, Schema } from "@8monkey/no-orm";

export const schema = {
  users: {
    fields: {
      id: { type: "string" },
      name: { type: "string", max: 255 },
      age: { type: "number" },
      is_active: { type: "boolean" },
      metadata: { type: "json", nullable: true },
      tags: { type: "json[]", nullable: true },
      created_at: { type: "timestamp" },
    },
    primaryKey: "id",
    indexes: [{ field: "created_at", order: "desc" }],
  },
} as const satisfies Schema;

type User = InferModel<typeof schema.users>;
```

## Choose an Adapter

### SQLite

```ts
import { Database } from "bun:sqlite";
import { SqliteAdapter } from "@8monkey/no-orm/adapters/sqlite";

const db = new Database("data.db");
const adapter = new SqliteAdapter(schema, db);

await adapter.migrate();
```

### Postgres

```ts
import postgres from "postgres"; // or import { Pool } from "pg"
import { PostgresAdapter } from "@8monkey/no-orm/adapters/postgres";

const sql = postgres(process.env.POSTGRES_URL!);
const adapter = new PostgresAdapter(schema, sql);

await adapter.migrate();
```

### Memory

In-memory storage for testing or temporary data.

```ts
import { MemoryAdapter } from "@8monkey/no-orm/adapters/memory";

const adapter = new MemoryAdapter(schema, { maxItems: 100 });
await adapter.migrate();
```

## CRUD

```ts
// Create
const created = await adapter.create({
  model: "users",
  data: {
    id: "u1",
    name: "Alice",
    age: 30,
    is_active: true,
    metadata: { theme: "dark" },
    tags: ["admin"],
    created_at: Date.now(),
  },
});

// Find one
const found = await adapter.find({
  model: "users",
  where: { field: "id", op: "eq", value: "u1" },
});

// Find many
const users = await adapter.findMany({
  model: "users",
  where: { field: "is_active", op: "eq", value: true },
  sortBy: [{ field: "created_at", direction: "desc" }],
  limit: 20,
});

// Update one
const updated = await adapter.update({
  model: "users",
  where: { field: "id", op: "eq", value: "u1" },
  data: { age: 31 },
});

// Update many
const updatedCount = await adapter.updateMany({
  model: "users",
  where: { field: "is_active", op: "eq", value: true },
  data: { age: 99 },
});

// Delete one
await adapter.delete({
  model: "users",
  where: { field: "id", op: "eq", value: "u1" },
});

// Delete many
const deletedCount = await adapter.deleteMany({
  model: "users",
  where: { field: "is_active", op: "eq", value: false },
});

// Count
const total = await adapter.count({
  model: "users",
  where: { field: "is_active", op: "eq", value: true },
});

// Upsert - insert or update by primary key
const user = await adapter.upsert({
  model: "users",
  create: { id: "u1", name: "Alice", age: 30, is_active: true, created_at: Date.now() },
  update: { age: 31 },
  // Optional: only update if predicate is met
  where: { field: "is_active", op: "eq", value: true },
});
```

## Filtering

All operations accept a `where` clause:

```ts
// Operators
where: { field: "age", op: "eq", value: 30 }
where: { field: "age", op: "ne", value: null }
where: { field: "age", op: "gt", value: 18 }
where: { field: "age", op: "gte", value: 18 }
where: { field: "age", op: "lt", value: 65 }
where: { field: "age", op: "lte", value: 65 }
where: { field: "status", op: "in", value: ["active", "pending"] }
where: { field: "status", op: "not_in", value: ["banned"] }

// Combine with and/or
where: {
  and: [
    { field: "age", op: "gte", value: 18 },
    { field: "is_active", op: "eq", value: true },
  ],
}
```

## JSON Paths

Filter nested JSON fields using `path`:

```ts
const darkUsers = await adapter.findMany({
  model: "users",
  where: {
    field: "metadata",
    path: ["preferences", "theme"],
    op: "eq",
    value: "dark",
  },
});
```

## Pagination

```ts
// Offset pagination
const page = await adapter.findMany({
  model: "users",
  sortBy: [{ field: "created_at", direction: "desc" }],
  limit: 20,
  offset: 40,
});

// Cursor pagination (keyset)
const cursorPage = await adapter.findMany({
  model: "users",
  sortBy: [{ field: "created_at", direction: "desc" }],
  limit: 20,
  cursor: {
    after: { created_at: 1699900000000, id: "u20" },
  },
});
```

## Transactions

```ts
await adapter.transaction(async (tx) => {
  await tx.create({
    model: "users",
    data: { id: "u2", name: "Bob", age: 28, is_active: true, created_at: Date.now() },
  });

  await tx.update({
    model: "users",
    where: { field: "id", op: "eq", value: "u2" },
    data: { age: 29 },
  });
});
```

Nested calls to `transaction()` join the existing transaction.

## License

MIT
