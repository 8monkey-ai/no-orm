import { Database } from "bun:sqlite";
import { describe, expect, it, beforeEach } from "bun:test";

import type { Schema, InferModel } from "../core";
import { SqliteAdapter } from "./sqlite";

describe("SqliteAdapter", () => {
  const schema = {
    users: {
      fields: {
        id: { type: { type: "string" } },
        name: { type: { type: "string" } },
        age: { type: { type: "number" } },
        is_active: { type: { type: "boolean" } },
        metadata: { type: { type: "json" }, nullable: true },
      },
      primaryKey: { fields: ["id"] },
      indexes: [{ fields: [{ field: "name" }] }],
    },
  } as const satisfies Schema;

  type User = InferModel<typeof schema.users>;

  let adapter: SqliteAdapter;
  let db: Database;

  beforeEach(async () => {
    db = new Database(":memory:");
    adapter = new SqliteAdapter(schema, db);
    await adapter.migrate();
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

    const found = await adapter.find<User>({
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

    const actives = await adapter.findMany<User>({
      model: "users",
      where: { field: "is_active", op: "eq", value: true },
      sortBy: [{ field: "age", direction: "asc" }],
    });

    expect(actives).toHaveLength(2);
    expect(actives[0]?.name).toBe("Alice");
    expect(actives[1]?.name).toBe("Charlie");
  });

  it("should handle complex AND / OR where clauses", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 20, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u2", name: "Bob", age: 30, is_active: true, metadata: null },
    });
    await adapter.create({
      model: "users",
      data: { id: "u3", name: "Charlie", age: 40, is_active: false, metadata: null },
    });

    const found = await adapter.findMany<User>({
      model: "users",
      where: {
        or: [
          {
            and: [
              { field: "age", op: "gte", value: 30 },
              { field: "is_active", op: "eq", value: true },
            ],
          },
          { field: "name", op: "eq", value: "Charlie" },
        ],
      },
    });

    expect(found).toHaveLength(2);
    expect(found.map((f) => f.name)).toContain("Bob");
    expect(found.map((f) => f.name)).toContain("Charlie");
  });

  it("should handle nested JSON path filtering with `->>` syntax", async () => {
    await adapter.create({
      model: "users",
      data: {
        id: "j1",
        name: "User1",
        age: 20,
        is_active: true,
        metadata: { theme: "dark", window: { width: 800 } },
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
      },
    });

    // 1. Exact match on nested string (theme = 'dark')
    const darkUsers = await adapter.findMany<User>({
      model: "users",
      where: { field: "metadata", path: ["theme"], op: "eq", value: "dark" },
    });
    expect(darkUsers).toHaveLength(2);
    expect(darkUsers.map((u) => u.name)).toContain("User1");
    expect(darkUsers.map((u) => u.name)).toContain("User3");

    // 2. Numeric operator on deeply nested number (window.width > 900)
    const wideUsers = await adapter.findMany<User>({
      model: "users",
      where: { field: "metadata", path: ["window", "width"], op: "gt", value: 900 },
    });
    expect(wideUsers).toHaveLength(2);
    expect(wideUsers.map((u) => u.name)).toContain("User2");
    expect(wideUsers.map((u) => u.name)).toContain("User3");

    // 3. IN operator on nested string
    const specificUsers = await adapter.findMany<User>({
      model: "users",
      where: { field: "metadata", path: ["window", "width"], op: "in", value: [800, 1024] },
    });
    expect(specificUsers).toHaveLength(2);
    expect(specificUsers.map((u) => u.name)).toContain("User1");
    expect(specificUsers.map((u) => u.name)).toContain("User2");
  });

  it("should update a record", async () => {
    await adapter.create({
      model: "users",
      data: { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null },
    });

    await adapter.update<User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
      data: { age: 26 },
    });

    const updated = await adapter.find<User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });

    expect(updated?.age).toBe(26);
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

    const found = await adapter.find<User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });

    expect(found).toBeNull();
  });

  it("should upsert a record", async () => {
    const data = { id: "u1", name: "Alice", age: 25, is_active: true, metadata: null };

    // Insert
    await adapter.upsert({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
      create: data,
      update: { age: 26 },
    });

    let found = await adapter.find<User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });
    expect(found?.age).toBe(25);

    // Update
    await adapter.upsert({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
      create: data,
      update: { age: 26 },
    });

    found = await adapter.find<User>({
      model: "users",
      where: { field: "id", op: "eq", value: "u1" },
    });
    expect(found?.age).toBe(26);
  });

  describe("Transactions", () => {
    it("should successfully commit multiple operations in a transaction", async () => {
      await adapter.transaction(async (tx) => {
        await tx.create({
          model: "users",
          data: { id: "t1", name: "TxUser1", age: 20, is_active: true, metadata: null },
        });
        await tx.create({
          model: "users",
          data: { id: "t2", name: "TxUser2", age: 25, is_active: true, metadata: null },
        });
        await tx.update<User>({
          model: "users",
          where: { field: "id", op: "eq", value: "t1" },
          data: { age: 21 },
        });
      });

      const found1 = await adapter.find<User>({
        model: "users",
        where: { field: "id", op: "eq", value: "t1" },
      });
      const found2 = await adapter.find<User>({
        model: "users",
        where: { field: "id", op: "eq", value: "t2" },
      });

      expect(found1?.age).toBe(21);
      expect(found2?.name).toBe("TxUser2");
    });

    it("should rollback all operations in a failed transaction", async () => {
      // Pre-existing record
      await adapter.create({
        model: "users",
        data: { id: "t3", name: "Existing", age: 30, is_active: true, metadata: null },
      });

      try {
        await adapter.transaction(async (tx) => {
          // Operation 1: Create new
          await tx.create({
            model: "users",
            data: { id: "t4", name: "NewUser", age: 20, is_active: true, metadata: null },
          });
          // Operation 2: Update existing
          await tx.update<User>({
            model: "users",
            where: { field: "id", op: "eq", value: "t3" },
            data: { age: 31 },
          });
          throw new Error("Force rollback");
        });
      } catch (e) {
        if (e instanceof Error) {
          expect(e.message).toBe("Force rollback");
        }
      }

      // Verify new record was NOT created
      const foundNew = await adapter.find({
        model: "users",
        where: { field: "id", op: "eq", value: "t4" },
      });
      expect(foundNew).toBeNull();

      // Verify existing record was NOT updated
      const foundExisting = await adapter.find<User>({
        model: "users",
        where: { field: "id", op: "eq", value: "t3" },
      });
      expect(foundExisting?.age).toBe(30); // Still 30, not 31
    });

    it("should handle multiple operations in nested transactions via savepoints", async () => {
      await adapter.transaction(async (tx1) => {
        // Outer operations
        await tx1.create({
          model: "users",
          data: { id: "n1", name: "Outer1", age: 20, is_active: true, metadata: null },
        });
        await tx1.create({
          model: "users",
          data: { id: "n2", name: "Outer2", age: 20, is_active: true, metadata: null },
        });

        try {
          if (tx1.transaction) {
            await tx1.transaction(async (tx2) => {
              // Inner operations
              await tx2.create({
                model: "users",
                data: { id: "n3", name: "Inner1", age: 20, is_active: true, metadata: null },
              });
              await tx2.update<User>({
                model: "users",
                where: { field: "id", op: "eq", value: "n1" }, // Modifying outer record
                data: { name: "Outer1_Modified" },
              });
              throw new Error("Inner rollback");
            });
          }
        } catch {
          // Expected inner rollback
        }
      });

      // Outer operations should commit (n1 and n2 exist, but n1 is NOT modified by inner)
      const outer1 = await adapter.find<User>({
        model: "users",
        where: { field: "id", op: "eq", value: "n1" },
      });
      const outer2 = await adapter.find<User>({
        model: "users",
        where: { field: "id", op: "eq", value: "n2" },
      });

      expect(outer1?.name).toBe("Outer1"); // Inner tx rolled back; update to n1 was never applied
      expect(outer2).not.toBeNull();

      // Inner operations should rollback (n3 does not exist)
      const inner = await adapter.find({
        model: "users",
        where: { field: "id", op: "eq", value: "n3" },
      });
      expect(inner).toBeNull();
    });
  });

  describe("Pagination", () => {
    it("should handle multi-field keyset pagination correctly", async () => {
      // Seed data specifically for multi-field sort
      await adapter.create({
        model: "users",
        data: { id: "m1", name: "A", age: 30, is_active: true, metadata: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m2", name: "B", age: 30, is_active: true, metadata: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m3", name: "C", age: 30, is_active: true, metadata: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m4", name: "A", age: 31, is_active: true, metadata: null },
      });
      await adapter.create({
        model: "users",
        data: { id: "m5", name: "B", age: 31, is_active: true, metadata: null },
      });

      const result = await adapter.findMany<User>({
        model: "users",
        sortBy: [
          { field: "age", direction: "asc" },
          { field: "name", direction: "desc" },
        ],
        cursor: {
          after: { age: 30, name: "B" }, // Cursor points to m2
        },
        limit: 3,
      });

      // Sorted order:
      // 1. age: 30, name: "C" (m3) -> skipped (before cursor)
      // 2. age: 30, name: "B" (m2) -> cursor
      // 3. age: 30, name: "A" (m1) -> match 1
      // 4. age: 31, name: "B" (m5) -> match 2
      // 5. age: 31, name: "A" (m4) -> match 3

      expect(result).toHaveLength(3);
      expect(result[0]?.id).toBe("m1"); // age 30, name A (asc 30 == 30, desc A < B)
      expect(result[1]?.id).toBe("m5"); // age 31, name B (asc 31 > 30)
      expect(result[2]?.id).toBe("m4"); // age 31, name A
    });

    beforeEach(async () => {
      // Seed data for pagination
      const creations = [];
      for (let i = 1; i <= 5; i++) {
        creations.push(
          adapter.create({
            model: "users",
            data: { id: `p${i}`, name: `User ${i}`, age: 20 + i, is_active: true, metadata: null },
          }),
        );
      }
      await Promise.all(creations);
    });

    it("should respect limit and offset", async () => {
      const page1 = await adapter.findMany<User>({
        model: "users",
        sortBy: [{ field: "age", direction: "asc" }],
        limit: 2,
        offset: 0,
      });
      expect(page1).toHaveLength(2);
      expect(page1[0]?.name).toBe("User 1");
      expect(page1[1]?.name).toBe("User 2");

      const page2 = await adapter.findMany<User>({
        model: "users",
        sortBy: [{ field: "age", direction: "asc" }],
        limit: 2,
        offset: 2,
      });
      expect(page2).toHaveLength(2);
      expect(page2[0]?.name).toBe("User 3");
      expect(page2[1]?.name).toBe("User 4");
    });

    it("should handle cursor pagination ascending", async () => {
      const result = await adapter.findMany<User>({
        model: "users",
        sortBy: [{ field: "age", direction: "asc" }],
        cursor: { after: { age: 22 } },
        limit: 2,
      });

      expect(result).toHaveLength(2);
      expect(result[0]?.name).toBe("User 3"); // age 23 > 22
      expect(result[1]?.name).toBe("User 4"); // age 24 > 22
    });

    it("should handle cursor pagination descending", async () => {
      const result = await adapter.findMany<User>({
        model: "users",
        sortBy: [{ field: "age", direction: "desc" }],
        cursor: { after: { age: 24 } },
        limit: 2,
      });

      expect(result).toHaveLength(2);
      expect(result[0]?.name).toBe("User 3"); // age 23 < 24
      expect(result[1]?.name).toBe("User 2"); // age 22 < 24
    });
  });
});
