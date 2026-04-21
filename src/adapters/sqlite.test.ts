import { Database } from "bun:sqlite";
import { describe, expect, it, beforeEach } from "bun:test";

import type { Schema, InferModel } from "../types";
import { SqliteAdapter } from "./sqlite";

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
    indexes: [{ field: "name" }, { field: "age" }],
  },
} as const satisfies Schema;

type User = InferModel<typeof schema.users>;

describe("SqliteAdapter", () => {
  let db: Database;
  let adapter: SqliteAdapter<typeof schema>;

  beforeEach(async () => {
    db = new Database(":memory:");
    adapter = new SqliteAdapter(schema, db);
    await adapter.migrate();
  });

  describe("Basic CRUD", () => {
    it("should create and find a record", async () => {
      const user: User = {
        id: "u1",
        name: "Alice",
        age: 30,
        is_active: true,
        metadata: { theme: "dark" },
        tags: ["admin"],
      };
      await adapter.create({ model: "users", data: user });

      const found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found).toEqual(user);
    });

    it("should update a record and refetch correctly", async () => {
      await adapter.create({
        model: "users",
        data: { id: "u1", name: "Alice", age: 30, is_active: true, metadata: null, tags: null },
      });
      const updated = await adapter.update<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
        data: { age: 31 },
      });
      expect(updated?.age).toBe(31);
    });

    it("should reject primary key updates", async () => {
      await adapter.create({
        model: "users",
        data: { id: "u1", name: "Alice", age: 30, is_active: true, metadata: null, tags: null },
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
        data: { id: "u1", name: "Alice", age: 30, is_active: true, metadata: null, tags: null },
      });
      await adapter.delete({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      const found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found).toBeNull();
    });
  });

  describe("Filtering and Sorting", () => {
    beforeEach(async () => {
      await Promise.all([
        adapter.create({
          model: "users",
          data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null, tags: null },
        }),
        adapter.create({
          model: "users",
          data: { id: "u2", name: "Bob", age: 30, is_active: false, metadata: null, tags: null },
        }),
        adapter.create({
          model: "users",
          data: { id: "u3", name: "Charlie", age: 35, is_active: true, metadata: null, tags: null },
        }),
      ]);
    });

    it("should filter with 'in' operator", async () => {
      const users = await adapter.findMany<"users", User>({
        model: "users",
        where: { field: "age", op: "in", value: [25, 35] },
      });
      expect(users).toHaveLength(2);
    });

    it("should handle empty 'in' list gracefully", async () => {
      const users = await adapter.findMany<"users", User>({
        model: "users",
        where: { field: "age", op: "in", value: [] },
      });
      expect(users).toHaveLength(0);
    });

    it("should handle complex AND / OR where clauses", async () => {
      const found = await adapter.findMany<"users", User>({
        model: "users",
        where: {
          or: [
            {
              and: [
                { field: "age", op: "gte", value: 30 },
                { field: "is_active", op: "eq", value: true },
              ],
            },
            { field: "name", op: "eq", value: "Bob" },
          ],
        },
      });

      expect(found).toHaveLength(2);
      expect(found.map((f) => f.name)).toContain("Bob");
      expect(found.map((f) => f.name)).toContain("Charlie");
    });

    it("should sort records", async () => {
      const users = await adapter.findMany<"users", User>({
        model: "users",
        sortBy: [{ field: "age", direction: "desc" }],
      });
      expect(users[0]?.id).toBe("u3");
    });

    it("should filter by null equality (IS NULL)", async () => {
      await adapter.create({
        model: "users",
        data: { id: "u4", name: "NullUser", age: 40, is_active: true, metadata: null, tags: null },
      });
      const users = await adapter.findMany<"users", User>({
        model: "users",
        where: { field: "metadata", op: "eq", value: null },
      });
      // u1, u2, u3 in beforeEach also have metadata: null
      expect(users.length).toBeGreaterThanOrEqual(1);
      expect(users.find((u) => u.id === "u4")).toBeDefined();
    });

    it("should filter by null inequality (IS NOT NULL)", async () => {
      await adapter.create({
        model: "users",
        data: {
          id: "u5",
          name: "NotNullUser",
          age: 40,
          is_active: true,
          metadata: { has_data: true },
          tags: null,
        },
      });
      const users = await adapter.findMany<"users", User>({
        model: "users",
        where: { field: "metadata", op: "ne", value: null },
      });
      expect(users.find((u) => u.id === "u5")).toBeDefined();
      expect(users.find((u) => u.id === "u1")).toBeUndefined();
    });

    it("should sort records with null values", async () => {
      await adapter.create({
        model: "users",
        data: {
          id: "sn1",
          name: "Alice",
          age: 25,
          is_active: true,
          metadata: { theme: "dark" },
          tags: null,
        },
      });
      await adapter.create({
        model: "users",
        data: { id: "sn2", name: "Bob", age: 30, is_active: true, metadata: null, tags: null },
      });

      const results = await adapter.findMany({
        model: "users",
        where: { field: "id", op: "in", value: ["sn1", "sn2"] },
        sortBy: [{ field: "metadata", direction: "asc" }],
      });

      expect(results).toHaveLength(2);
      expect(results[0]?.["id"]).toBe("sn2"); // null should come first in SQLite ASC
      expect(results[1]?.["id"]).toBe("sn1");
    });
  });

  describe("JSON Path Filtering", () => {
    it("should handle nested JSON path filtering", async () => {
      await adapter.create({
        model: "users",
        data: {
          id: "j1",
          name: "User1",
          age: 20,
          is_active: true,
          metadata: { theme: "dark", window: { width: 800 } },
          tags: null,
        },
      });
      await adapter.create({
        model: "users",
        data: {
          id: "j2",
          name: "User2",
          age: 20,
          is_active: true,
          metadata: { theme: "light", window: { width: 1024 } },
          tags: null,
        },
      });
      await adapter.create({
        model: "users",
        data: {
          id: "j3",
          name: "User3",
          age: 20,
          is_active: true,
          metadata: { theme: "dark", window: { width: 1920 } },
          tags: null,
        },
      });

      // 1. Exact match on nested string (theme = 'dark')
      const darkUsers = await adapter.findMany<"users", User>({
        model: "users",
        where: { field: "metadata", path: ["theme"], op: "eq", value: "dark" },
      });
      expect(darkUsers).toHaveLength(2);

      // 2. Numeric operator on deeply nested number (window.width > 900)
      const wideUsers = await adapter.findMany<"users", User>({
        model: "users",
        where: { field: "metadata", path: ["window", "width"], op: "gt", value: 900 },
      });
      expect(wideUsers).toHaveLength(2);
    });
  });

  describe("Transactions", () => {
    it("should commit successful transactions", async () => {
      await adapter.transaction(async (tx) => {
        await tx.create({
          model: "users",
          data: { id: "t1", name: "TxUser1", age: 20, is_active: true, metadata: null, tags: null },
        });
      });
      const found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "t1" },
      });
      expect(found).not.toBeNull();
    });

    it("should rollback failed transactions", async () => {
      try {
        await adapter.transaction(async (tx) => {
          await tx.create({
            model: "users",
            data: {
              id: "t1",
              name: "TxUser1",
              age: 20,
              is_active: true,
              metadata: null,
              tags: null,
            },
          });
          throw new Error("Failure");
        });
      } catch {
        // expected
      }
      const found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "t1" },
      });
      expect(found).toBeNull();
    });

    it("should handle nested transactions with savepoints", async () => {
      await adapter.transaction(async (outer) => {
        if (!outer.transaction) throw new Error("Transactions not supported");
        await outer.create({
          model: "users",
          data: { id: "n1", name: "Outer1", age: 20, is_active: true, metadata: null, tags: null },
        });

        try {
          await outer.transaction(async (inner) => {
            await inner.update<"users", User>({
              model: "users",
              where: { field: "id", op: "eq", value: "n1" },
              data: { age: 40 },
            });
            throw new Error("Inner fail");
          });
        } catch {
          // expected
        }
      });

      const found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "n1" },
      });
      expect(found?.age).toBe(20);
    });
  });

  describe("Pagination", () => {
    it("should handle multi-field keyset pagination correctly", async () => {
      await adapter.create({
        model: "users",
        data: { id: "m1", name: "A", age: 30, is_active: true, metadata: null, tags: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m2", name: "B", age: 30, is_active: true, metadata: null, tags: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m3", name: "C", age: 30, is_active: true, metadata: null, tags: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m4", name: "A", age: 31, is_active: true, metadata: null, tags: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m5", name: "B", age: 31, is_active: true, metadata: null, tags: null },
      });

      const result = await adapter.findMany<"users", User>({
        model: "users",
        sortBy: [
          { field: "age", direction: "asc" },
          { field: "name", direction: "desc" },
        ],
        cursor: {
          after: { age: 30, name: "B" },
        },
        limit: 3,
      });

      expect(result).toHaveLength(3);
      expect(result[0]?.id).toBe("m1");
      expect(result[1]?.id).toBe("m5");
      expect(result[2]?.id).toBe("m4");
    });

    describe("Seeded Pagination", () => {
      beforeEach(async () => {
        const creations = [];
        for (let i = 1; i <= 5; i++) {
          creations.push(
            adapter.create({
              model: "users",
              data: {
                id: `p${i}`,
                name: `User ${i}`,
                age: 20 + i,
                is_active: true,
                metadata: null,
                tags: null,
              },
            }),
          );
        }
        await Promise.all(creations);
      });

      it("should respect limit and offset", async () => {
        const page1 = await adapter.findMany<"users", User>({
          model: "users",
          sortBy: [{ field: "age", direction: "asc" }],
          limit: 2,
          offset: 0,
        });
        expect(page1).toHaveLength(2);
        expect(page1[0]?.id).toBe("p1");

        const page2 = await adapter.findMany<"users", User>({
          model: "users",
          sortBy: [{ field: "age", direction: "asc" }],
          limit: 2,
          offset: 2,
        });
        expect(page2).toHaveLength(2);
        expect(page2[0]?.id).toBe("p3");
      });

      it("should handle cursor pagination ascending", async () => {
        const result = await adapter.findMany<"users", User>({
          model: "users",
          sortBy: [{ field: "age", direction: "asc" }],
          cursor: { after: { age: 22 } },
          limit: 2,
        });

        expect(result).toHaveLength(2);
        expect(result[0]?.id).toBe("p3");
      });

      it("should handle cursor pagination descending", async () => {
        const result = await adapter.findMany<"users", User>({
          model: "users",
          sortBy: [{ field: "age", direction: "desc" }],
          cursor: { after: { age: 24 } },
          limit: 2,
        });

        expect(result).toHaveLength(2);
        expect(result[0]?.id).toBe("p3");
      });
    });
  });

  describe("Upsert", () => {
    it("should handle upsert correctly", async () => {
      const data: User = {
        id: "u1",
        name: "Alice",
        age: 25,
        is_active: true,
        metadata: null,
        tags: null,
      };

      // Insert
      await adapter.upsert<"users", User>({
        model: "users",
        create: data,
        update: { age: 26 },
      });

      let found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(25);

      // Update
      await adapter.upsert<"users", User>({
        model: "users",
        create: data,
        update: { age: 26 },
      });

      found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(26);
    });

    it("should handle predicated upsert", async () => {
      const data: User = {
        id: "u1",
        name: "Alice",
        age: 25,
        is_active: true,
        metadata: null,
        tags: null,
      };

      await adapter.create({ model: "users", data });

      // Update should NOT happen if where condition is false
      await adapter.upsert<"users", User>({
        model: "users",
        create: data,
        update: { age: 30 },
        where: { field: "age", op: "gt", value: 50 },
      });

      let found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(25);

      // Update SHOULD happen if where condition is true
      await adapter.upsert<"users", User>({
        model: "users",
        create: data,
        update: { age: 30 },
        where: { field: "age", op: "lt", value: 50 },
      });

      found = await adapter.find<"users", User>({
        model: "users",
        where: { field: "id", op: "eq", value: "u1" },
      });
      expect(found?.age).toBe(30);
    });
  });
});
