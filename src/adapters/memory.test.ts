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
      },
      primaryKey: "id",
    },
  } as const satisfies Schema;

  type User = InferModel<typeof schema.users>;

  let adapter: MemoryAdapter<typeof schema>;

  beforeEach(async () => {
    adapter = new MemoryAdapter(schema);
    await adapter.migrate({ schema });
  });

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

  it("should require primary-key equality in upsert filters", () => {
    const userData: User = {
      id: "u1",
      name: "Alice",
      age: 25,
      is_active: true,
      metadata: null,
    };

    expect(() =>
      adapter.upsert<"users", User>({
        model: "users",
        where: { field: "name", op: "eq", value: "Alice" },
        create: userData,
        update: { age: 26 },
      }),
    ).toThrow("Upsert requires equality filters for every primary key field.");
  });
});
