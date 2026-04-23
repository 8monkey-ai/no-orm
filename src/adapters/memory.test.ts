import { describe, expect, it, beforeEach } from "bun:test";

import type { Schema, InferModel } from "../types";
import { MemoryAdapter } from "./memory";

describe("MemoryAdapter", () => {
  const schema = {
    users: {
      fields: {
        id: { type: "string" },
        name: { type: "string" },
        age: { type: "number" },
        is_active: { type: "boolean" },
        metadata: { type: "json", nullable: true },
        tags: { type: "json[]", nullable: true },
      },
      primaryKey: "id",
    },
    items: {
      fields: {
        group_id: { type: "string" },
        item_id: { type: "string" },
        value: { type: "number" },
        created_at: { type: "timestamp" },
      },
      primaryKey: ["group_id", "item_id"],
    },
  } as const satisfies Schema;

  type User = InferModel<typeof schema.users>;
  type Item = InferModel<typeof schema.items>;

  let adapter: MemoryAdapter<typeof schema>;

  beforeEach(async () => {
    adapter = new MemoryAdapter(schema);
    await adapter.migrate();
  });

  // --- Create & Find ---

  it("should create and find a record", async () => {
    const userData: User = {
      id: "u1",
      name: "Alice",
      age: 25,
      is_active: true,
      metadata: { theme: "dark" },
    };

    await adapter.create({ model: "users", data: userData });

    const found = await adapter.find({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });

    expect(found).toEqual(userData);
  });

  it("should reject duplicate primary keys", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });

    expect(() =>
      adapter.create({
        model: "users",
        data: { id: "u1", name: "Bob", age: 30, is_active: true, metadata: null },
      }),
    ).toThrow("already exists");
  });

  it("should return null for find with no match", async () => {
    const found = await adapter.find({
      model: "users",
      where: { field: "id", op: "eq", value: "nonexistent" },
    });
    expect(found).toBeNull();
  });

  // --- Composite primary keys ---

  it("should support composite primary keys", async () => {
    await adapter.create({
      model: "items",
      data: { group_id: "g1", item_id: "i1", value: 10, created_at: 1000 },
    });
    await adapter.create({
      model: "items",
      data: { group_id: "g1", item_id: "i2", value: 20, created_at: 2000 },
    });

    const found = await adapter.find<"items", Item>({
      model: "items",
      where: {
        and: [
          { field: "group_id", op: "eq", value: "g1" },
          { field: "item_id", op: "eq", value: "i2" },
        ],
      },
    });
    expect(found?.value).toBe(20);

    const all = await adapter.findMany({ model: "items" });
    expect(all).toHaveLength(2);
  });

  // --- Select projection ---

  it("should project fields with select", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });

    const found = await adapter.find<"users", User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
      select: ["id", "name"],
    });

    expect(found?.["id"]).toBe("u1");
    expect(found?.["name"]).toBe("Alice");
    expect(Object.keys(found!)).toEqual(["id", "name"]);
  });

  // --- FindMany ---

  it("should find multiple records with filters", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: false, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 35, is_active: true, metadata: null },
    });

    const actives = await adapter.findMany<"users", User>({
      model: "users",
      where: { field: "is_active", op: "eq", value: true },
      sortBy: [{ field: "age", direction: "asc" }],
    });

    expect(actives).toHaveLength(2);
    expect(actives[0]?.name).toBe("Alice");
    expect(actives[1]?.name).toBe("Charlie");
  });

  it("should return empty array when no records match", async () => {
    const results = await adapter.findMany({
      model: "users",
      where: { field: "age", op: "gt", value: 1000 },
    });
    expect(results).toHaveLength(0);
  });

  it("should support offset pagination", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "User1", age: 10, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "User2", age: 20, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "User3", age: 30, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u4", name: "User4", age: 40, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u5", name: "User5", age: 50, is_active: true, metadata: null },
    });

    const page = await adapter.findMany({
      model: "users",
      sortBy: [{ field: "age", direction: "asc" }],
      limit: 2,
      offset: 2,
    });
    expect(page).toHaveLength(2);
    expect(page[0]?.["age"]).toBe(30);
    expect(page[1]?.["age"]).toBe(40);
  });

  it("should support in/not_in operators", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 35, is_active: true, metadata: null },
    });

    const inResult = await adapter.findMany({
      model: "users",
      where: { field: "name", op: "in", value: ["Alice", "Charlie"] },
    });
    expect(inResult).toHaveLength(2);

    const notInResult = await adapter.findMany({
      model: "users",
      where: { field: "name", op: "not_in", value: ["Alice", "Charlie"] },
    });
    expect(notInResult).toHaveLength(1);
    expect(notInResult[0]?.["name"]).toBe("Bob");
  });

  // --- JSON path filters ---

  it("should support nested JSON path filters", async () => {
    await adapter.create({
      model: "users",
      data: {
        id: "u1",
        name: "Alice",
        age: 25,
        is_active: true,
        metadata: { settings: { theme: "dark" } },
      },
    });
    await adapter.create({
      model: "users",
      data: {
        id: "u2",
        name: "Bob",
        age: 30,
        is_active: true,
        metadata: { settings: { theme: "light" } },
      },
    });

    const darkThemeUsers = await adapter.findMany<"users", User>({
      model: "users",
      where: { field: "metadata", path: ["settings", "theme"], op: "eq", value: "dark" },
    });

    expect(darkThemeUsers).toHaveLength(1);
    expect(darkThemeUsers[0]?.name).toBe("Alice");
  });

  // --- Update ---

  it("should update a record", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });

    await adapter.update<"users", User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
      data: { age: 26 },
    });

    const updated = await adapter.find<"users", User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });

    expect(updated?.age).toBe(26);
  });

  it("should reject primary key updates", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });

    expect(() =>
      adapter.update<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
        data: { id: "u2" },
      }),
    ).toThrow("Primary key updates are not supported.");
  });

  it("should return null when updating non-existent record", async () => {
    const result = await adapter.update<"users", User>({
      model: "users",
      where: { field: "id", op: "eq", value: "nonexistent" },
      data: { age: 99 },
    });
    expect(result).toBeNull();
  });

  // --- UpdateMany ---

  it("should update multiple records", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 35, is_active: false, metadata: null },
    });

    const count = await adapter.updateMany<"users", User>({
      model: "users",
      where: { field: "is_active", op: "eq", value: true },
      data: { age: 99 },
    });
    expect(count).toBe(2);

    const alice = await adapter.find<"users", User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });
    expect(alice?.age).toBe(99);

    const charlie = await adapter.find<"users", User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u3" },
    });
    expect(charlie?.age).toBe(35); // unchanged
  });

  // --- Delete ---

  it("should delete a record", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });

    await adapter.delete({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });

    const found = await adapter.find({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });

    expect(found).toBeNull();
  });

  // --- DeleteMany ---

  it("should delete multiple records", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: false, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 35, is_active: true, metadata: null },
    });

    const count = await adapter.deleteMany({
      model: "users",
      where: { field: "is_active", op: "eq", value: true },
    });
    expect(count).toBe(2);

    const remaining = await adapter.findMany({ model: "users" });
    expect(remaining).toHaveLength(1);
    expect(remaining[0]?.["name"]).toBe("Bob");
  });

  // --- Count ---

  it("should count records", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: false, metadata: null },
    });

    const total = await adapter.count({ model: "users" });
    expect(total).toBe(2);

    const actives = await adapter.count({
      model: "users",
      where: { field: "is_active", op: "eq", value: true },
    });
    expect(actives).toBe(1);
  });

  // --- Transaction ---

  it("should support transaction passthrough", async () => {
    await adapter.transaction(async (tx) => {
      await tx.create({
        model: "users",
        data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
      });
    });

    const found = await adapter.find({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });
    expect(found?.["name"]).toBe("Alice");
  });

  // --- Logical operators ---

  it("should support complex logical operators", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: false, metadata: null },
    });

    const results = await adapter.findMany({
      model: "users",
      where: {
        or: [
          { field: "age", op: "gt", value: 28 },
          { field: "name", op: "eq", value: "Alice" },
        ],
      },
    });

    expect(results).toHaveLength(2);
  });

  // --- Null handling ---

  it("should filter by null equality (op: eq, value: null)", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u4", name: "NullUser", age: 40, is_active: true, metadata: null, tags: null },
    });
    const users = await adapter.findMany<"users", User>({
      model: "users",
      where: { field: "metadata", op: "eq", value: null },
    });
    expect(users.find((u) => u.id === "u4")).toBeDefined();
  });

  it("should filter by null inequality (op: ne, value: null)", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u4", name: "NullUser", age: 40, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: {
        id: "u5",
        name: "NotNullUser",
        age: 40,
        is_active: true,
        metadata: { has_data: true },
      },
    });
    const users = await adapter.findMany<"users", User>({
      model: "users",
      where: { field: "metadata", op: "ne", value: null },
    });
    expect(users.find((u) => u.id === "u5")).toBeDefined();
    expect(users.find((u) => u.id === "u4")).toBeUndefined();
  });

  // --- Upsert ---

  describe("Upsert", () => {
    it("should handle upsert correctly (insert and update)", async () => {
      const userData: User = {
        id: "u1",
        name: "Alice",
        age: 25,
        is_active: true as boolean,
        metadata: null,
      };

      // 1. Insert because it doesn't exist
      await adapter.upsert<"users", User>({
        model: "users",
        create: userData,
        update: { age: 30 },
      });

      let found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(25); // Should have used 'create' data

      // 2. Update because it exists
      await adapter.upsert<"users", User>({
        model: "users",
        create: userData,
        update: { age: 31 },
      });

      found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(31); // Should have used 'update' data
    });

    it("should support predicated upsert", async () => {
      const userData: User = {
        id: "u1",
        name: "Alice",
        age: 25,
        is_active: true as boolean,
        metadata: null,
      };

      await adapter.create({ model: "users", data: userData });

      // Condition fails, no update
      await adapter.upsert<"users", User>({
        model: "users",
        create: userData,
        update: { age: 30 },
        where: { field: "age", op: "gt", value: 40 },
      });

      let found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(25);

      // Condition passes, update happens
      await adapter.upsert<"users", User>({
        model: "users",
        create: userData,
        update: { age: 30 },
        where: { field: "age", op: "lt", value: 40 },
      });

      found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(30);
    });

    it("should throw error if primary key is missing in 'create' data", () => {
      // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
      const invalidData = {
        name: "Missing ID",
        age: 20,
      } as unknown as User;

      expect(() =>
        adapter.upsert({
          model: "users",
          create: invalidData,
          update: { age: 21 },
        }),
      ).toThrow("Missing primary key field: id");
    });
  });

  // --- Sorting ---

  it("should sort records with null values", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: { theme: "dark" } },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: true, metadata: null },
    });

    const results = await adapter.findMany({
      model: "users",
      sortBy: [{ field: "metadata", direction: "asc" }],
    });

    expect(results).toHaveLength(2);
    expect(results[0]?.["id"]).toBe("u2"); // null should come first in asc
    expect(results[1]?.["id"]).toBe("u1");
  });

  // --- Pagination ---

  it("should support keyset pagination", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 20, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 20, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 30, is_active: true, metadata: null },
    });

    // Page 1
    const p1 = await adapter.findMany({
      model: "users",
      sortBy: [
        { field: "age", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      limit: 2,
    });
    expect(p1).toHaveLength(2);
    expect(p1[0]?.["id"]).toBe("u1");
    expect(p1[1]?.["id"]).toBe("u2");

    // Page 2
    const p2 = await adapter.findMany({
      model: "users",
      sortBy: [
        { field: "age", direction: "asc" },
        { field: "id", direction: "asc" },
      ],
      cursor: { after: { age: 20, id: "u2" } },
    });
    expect(p2).toHaveLength(1);
    expect(p2[0]?.["id"]).toBe("u3");
  });

  // --- LRU eviction ---

  it("should evict oldest entries when maxSize is exceeded", async () => {
    const smallAdapter = new MemoryAdapter(schema, { maxSize: 2 });
    await smallAdapter.migrate();

    await smallAdapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });
    await smallAdapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: true, metadata: null },
    });
    await smallAdapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 35, is_active: true, metadata: null },
    });

    // u1 should have been evicted (maxSize=2)
    const u1 = await smallAdapter.find({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });
    expect(u1).toBeNull();

    // u2 and u3 should still exist
    const u3 = await smallAdapter.find({
      model: "users",
      where: { field: "id", op: "eq", value: "u3" },
    });
    expect(u3).not.toBeNull();
  });
});
