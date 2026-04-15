import { describe, expect, it } from "bun:test";
import type { InferModel, Schema } from "./core";

describe("no-orm core", () => {
  it("should infer correct types for a schema", () => {
    const schema = {
      users: {
        fields: {
          id: { type: { type: "string" } },
          age: { type: { type: "number" } },
          is_active: { type: { type: "boolean" } },
          created_at: { type: { type: "timestamp" } },
          metadata: { type: { type: "json" }, nullable: true },
        },
        primaryKey: {
          fields: ["id"],
        },
      },
    } as const satisfies Schema;

    type User = InferModel<typeof schema.users>;

    // Type checking verification (this is compile-time, but we can assert some basics)
    const user: User = {
      id: "1",
      age: 30,
      is_active: true,
      created_at: Date.now(),
      metadata: { role: "admin" },
    };

    expect(user.id).toBe("1");
    expect(user.age).toBe(30);
    expect(user.is_active).toBe(true);
    expect(typeof user.created_at).toBe("number");
    expect(typeof user.metadata).toBe("object");
    expect(user.metadata).not.toBeNull();

    const nullableUser: User = {
      id: "2",
      age: 25,
      is_active: false,
      created_at: Date.now(),
      metadata: null,
    };
    expect(nullableUser.metadata).toBeNull();
  });

  it("should handle complex schemas with multiple models", () => {
    const schema = {
      conversations: {
        fields: {
          id: { type: { type: "string", max: 255 } },
          created_at: { type: { type: "timestamp" } },
        },
        primaryKey: { fields: ["id"] },
      },
      messages: {
        fields: {
          id: { type: { type: "string", max: 255 } },
          conversation_id: { type: { type: "string", max: 255 } },
          content: { type: { type: "string" } },
        },
        primaryKey: { fields: ["id"] },
      },
    } as const satisfies Schema;

    type Conversation = InferModel<typeof schema.conversations>;
    type Message = InferModel<typeof schema.messages>;

    const conv: Conversation = { id: "c1", created_at: Date.now() };
    const msg: Message = { id: "m1", conversation_id: "c1", content: "hello" };

    expect(conv.id).toBe("c1");
    expect(msg.content).toBe("hello");
  });
});
