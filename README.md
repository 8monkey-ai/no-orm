# no-orm

A tiny, database-independent persistence core for TypeScript libraries. No heavy abstractions, just the primitives.

## Features

- **Canonical Schema**: One portable schema representation for any database.
- **Type Inference**: Derive TypeScript types directly from your schema.
- **Adapter-Based**: Small, generic execution contract for multiple backends.

## Installation

```bash
bun add @8monkey/no-orm
```

## Usage

### 1. Define your Schema

```typescript
import { Schema } from "@8monkey/no-orm";

export const schema = {
  conversations: {
    fields: {
      id: { type: { type: "string", max: 255 } },
      created_at: { type: { type: "timestamp" } },
      metadata: { type: { type: "json" }, nullable: true },
    },
    primaryKey: {
      fields: ["id"],
    },
    indexes: [
      {
        fields: [
          { field: "created_at", order: "desc" },
          { field: "id", order: "desc" },
        ],
      },
    ],
  },
} as const satisfies Schema;
```

### 2. Infer Types

```typescript
import { InferModel } from "@8monkey/no-orm";

export type Conversation = InferModel<typeof schema.conversations>;
// Result: { id: string; created_at: number; metadata: Record<string, unknown> | null; }
```

### 3. Use an Adapter

```typescript
import { Adapter } from "@8monkey/no-orm";
// Import a concrete adapter (e.g., @8monkey/no-orm-sqlite)

const adapter: Adapter = new SqliteAdapter(schema, db);

// Minimal Schema Bootstrap
await adapter.migrate();

// You can seamlessly query nested JSON!
const darkUsers = await adapter.findMany({
  model: "conversations",
  where: { field: "metadata", path: ["theme"], op: "eq", value: "dark" },
});

// Create a record
const conv = await adapter.create({
  model: "conversations",
  data: {
    id: "conv_123",
    created_at: Date.now(),
    metadata: { theme: "dark" },
  },
});

// Find many with filters
const results = await adapter.findMany({
  model: "conversations",
  where: {
    field: "created_at",
    op: "gt",
    value: Date.now() - 86400000,
  },
  limit: 10,
});
```

## License

MIT
