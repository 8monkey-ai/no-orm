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

const db = new Database(":memory:");
const adapter = new SqliteAdapter(schema, db);

await adapter.migrate();
```

### Postgres

```ts
import { SQL } from "bun";
import { PostgresAdapter } from "@8monkey/no-orm/adapters/postgres";

const sql = new SQL(process.env.POSTGRES_URL!);
const adapter = new PostgresAdapter(schema, sql);

await adapter.migrate();
```

### Memory

```ts
import { MemoryAdapter } from "@8monkey/no-orm/adapters/memory";

const adapter = new MemoryAdapter(schema);
await adapter.migrate({ schema });
```

## Basic Operations

```ts
const created = await adapter.create<"users", User>({
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

const found = await adapter.find<"users", User>({
  model: "users",
  where: { field: "id", op: "eq", value: "u1" },
});

const recentUsers = await adapter.findMany<"users", User>({
  model: "users",
  where: { field: "is_active", op: "eq", value: true },
  sortBy: [{ field: "created_at", direction: "desc" }],
  limit: 20,
});
```

## JSON Path Filters

Nested JSON filters use the base field plus a `path` array:

```ts
const darkUsers = await adapter.findMany<"users", User>({
  model: "users",
  where: {
    field: "metadata",
    path: ["preferences", "theme"],
    op: "eq",
    value: "dark",
  },
});
```

Path segments are intentionally restricted to simple identifiers so adapters can
compile them safely for each backend.

## Transactions

```ts
await adapter.transaction(async (tx) => {
  await tx.create({
    model: "users",
    data: {
      id: "u2",
      name: "Bob",
      age: 28,
      is_active: true,
      metadata: null,
      tags: null,
      created_at: Date.now(),
    },
  });

  await tx.update({
    model: "users",
    where: { field: "id", op: "eq", value: "u2" },
    data: { age: 29 },
  });
});
```

SQLite and Postgres both support nested transactions through savepoints.

## Notes

- `upsert` is intentionally conservative in v1: the `where` clause must be equality conditions for every primary-key field.
- Primary-key updates are rejected to keep adapter behavior simple and consistent across backends.
- SQLite stores JSON as text; Postgres stores JSON as `jsonb`.
- **Numeric Precision**: `number` and `timestamp` fields use standard JavaScript `Number`. `bigint` is intentionally not supported in v1 to keep the core and adapters tiny.

## License

MIT
