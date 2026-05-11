import { describe, expect, it } from "bun:test";

import type { Model } from "../../types";
import { Sql, id, join, placeholders, raw, set, sort, sql, stringifyJsonParam, where } from "./sql";

const model: Model = {
  fields: {
    id: { type: "string" },
    name: { type: "string" },
    age: { type: "number" },
    is_active: { type: "boolean" },
  },
  primaryKey: "id",
};

const columnExpr = (_model: Model, fieldName: string) => id(fieldName);
const boolToInt = (v: unknown) => (typeof v === "boolean" ? (v ? 1 : 0) : v);

function compile(s: Sql): { sql: string; params: unknown[] } {
  return { sql: s.compile("?"), params: s.params };
}

describe("Sql.compile", () => {
  it("returns the string when there are no params", () => {
    expect(new Sql(["SELECT 1"], []).compile("?")).toBe("SELECT 1");
  });

  it("joins strings with a placeholder string", () => {
    expect(new Sql(["id = ", ""], ["u1"]).compile("?")).toBe("id = ?");
  });

  it("uses a function placeholder with correct index", () => {
    expect(new Sql(["a = ", " AND b = ", ""], [1, 2]).compile((i) => `$${i + 1}`)).toBe(
      "a = $1 AND b = $2",
    );
  });
});

describe("Sql.toTaggedArgs", () => {
  it("returns strings with .raw and params spread", () => {
    const s = new Sql(["before", "after"], [42]);
    const [strings, ...params] = s.toTaggedArgs();
    expect(Array.from(strings)).toEqual(["before", "after"]);
    expect(strings.raw).toEqual(["before", "after"]);
    expect(params).toEqual([42]);
  });
});

describe("raw", () => {
  it("creates a Sql with no params", () => {
    const r = raw("SELECT *");
    expect(r.strings).toEqual(["SELECT *"]);
    expect(r.params).toEqual([]);
  });
});

describe("id", () => {
  it("quotes a single identifier", () => {
    expect(compile(id("users"))).toEqual({ sql: '"users"', params: [] });
  });

  it("joins an array of identifiers with commas", () => {
    expect(compile(id(["a", "b", "c"]))).toEqual({ sql: '"a", "b", "c"', params: [] });
  });

  it("returns empty for an empty string", () => {
    expect(compile(id(""))).toEqual({ sql: "", params: [] });
  });

  it("returns empty for an empty array", () => {
    expect(compile(id([]))).toEqual({ sql: "", params: [] });
  });

  it("accepts a custom quote character", () => {
    expect(compile(id("tbl", "`"))).toEqual({ sql: "`tbl`", params: [] });
  });
});

describe("sql tagged template", () => {
  it("handles a plain string with no interpolations", () => {
    expect(compile(sql`SELECT 1`)).toEqual({ sql: "SELECT 1", params: [] });
  });

  it("interpolates scalar values as params", () => {
    expect(compile(sql`WHERE age > ${25}`)).toEqual({ sql: "WHERE age > ?", params: [25] });
  });

  it("splices nested Sql instances inline", () => {
    const inner = sql`age > ${25}`;
    expect(compile(sql`WHERE ${inner} AND name = ${"Alice"}`)).toEqual({
      sql: "WHERE age > ? AND name = ?",
      params: [25, "Alice"],
    });
  });

  it("handles multiple nested Sql instances", () => {
    const a = sql`a = ${1}`;
    const b = sql`b = ${2}`;
    expect(compile(sql`SELECT * WHERE ${a} AND ${b}`)).toEqual({
      sql: "SELECT * WHERE a = ? AND b = ?",
      params: [1, 2],
    });
  });
});

describe("placeholders", () => {
  it("returns empty for no values", () => {
    expect(compile(placeholders([]))).toEqual({ sql: "", params: [] });
  });

  it("returns a single placeholder", () => {
    expect(compile(placeholders(["x"]))).toEqual({ sql: "?", params: ["x"] });
  });

  it("returns comma-separated placeholders", () => {
    expect(compile(placeholders([1, 2, 3]))).toEqual({ sql: "?, ?, ?", params: [1, 2, 3] });
  });
});

describe("join", () => {
  it("returns empty for no fragments", () => {
    expect(compile(join([], ", "))).toEqual({ sql: "", params: [] });
  });

  it("returns the single fragment unchanged", () => {
    expect(compile(join([sql`a = ${1}`], ", "))).toEqual({ sql: "a = ?", params: [1] });
  });

  it("joins multiple fragments with separator", () => {
    expect(compile(join([sql`a = ${1}`, sql`b = ${2}`, raw("c")], " AND "))).toEqual({
      sql: "a = ? AND b = ? AND c",
      params: [1, 2],
    });
  });
});

describe("set", () => {
  it("throws for empty data", () => {
    expect(() => set({})).toThrow();
  });

  it("produces a single assignment", () => {
    expect(compile(set({ name: "Alice" }))).toEqual({ sql: '"name" = ?', params: ["Alice"] });
  });

  it("produces multiple comma-separated assignments", () => {
    expect(compile(set({ name: "Alice", age: 30 }))).toEqual({
      sql: '"name" = ?, "age" = ?',
      params: ["Alice", 30],
    });
  });
});

describe("sort", () => {
  it("throws for empty sortBy", () => {
    expect(() => sort(model, [], columnExpr)).toThrow();
  });

  it("produces ASC", () => {
    expect(compile(sort(model, [{ field: "age", direction: "asc" }], columnExpr))).toEqual({
      sql: '"age" ASC',
      params: [],
    });
  });

  it("produces DESC", () => {
    expect(compile(sort(model, [{ field: "name", direction: "desc" }], columnExpr)).sql).toBe(
      '"name" DESC',
    );
  });

  it("handles multiple fields", () => {
    expect(
      compile(
        sort(
          model,
          [
            { field: "age", direction: "desc" },
            { field: "name", direction: "asc" },
          ],
          columnExpr,
        ),
      ).sql,
    ).toBe('"age" DESC, "name" ASC');
  });
});

describe("where", () => {
  it("returns 1=1 for undefined clause", () => {
    expect(compile(where(undefined, { model, columnExpr }))).toEqual({ sql: "1=1", params: [] });
  });

  it("eq operator with value", () => {
    expect(compile(where({ field: "id", op: "eq", value: "u1" }, { model, columnExpr }))).toEqual({
      sql: '("id" = ?)',
      params: ["u1"],
    });
  });

  it("eq operator with null produces IS NULL", () => {
    expect(compile(where({ field: "id", op: "eq", value: null }, { model, columnExpr }))).toEqual({
      sql: '("id" IS NULL)',
      params: [],
    });
  });

  it("ne operator with value", () => {
    expect(compile(where({ field: "age", op: "ne", value: 0 }, { model, columnExpr }))).toEqual({
      sql: '("age" != ?)',
      params: [0],
    });
  });

  it("ne operator with null produces IS NOT NULL", () => {
    expect(compile(where({ field: "age", op: "ne", value: null }, { model, columnExpr }))).toEqual({
      sql: '("age" IS NOT NULL)',
      params: [],
    });
  });

  it("gt operator", () => {
    expect(compile(where({ field: "age", op: "gt", value: 18 }, { model, columnExpr }))).toEqual({
      sql: '("age" > ?)',
      params: [18],
    });
  });

  it("gte operator", () => {
    expect(compile(where({ field: "age", op: "gte", value: 18 }, { model, columnExpr })).sql).toBe(
      '("age" >= ?)',
    );
  });

  it("lt operator", () => {
    expect(compile(where({ field: "age", op: "lt", value: 65 }, { model, columnExpr })).sql).toBe(
      '("age" < ?)',
    );
  });

  it("lte operator", () => {
    expect(compile(where({ field: "age", op: "lte", value: 65 }, { model, columnExpr })).sql).toBe(
      '("age" <= ?)',
    );
  });

  it("in operator with values", () => {
    expect(
      compile(where({ field: "age", op: "in", value: [25, 30] }, { model, columnExpr })),
    ).toEqual({ sql: '("age" IN (?, ?))', params: [25, 30] });
  });

  it("in operator with empty array produces 1=0", () => {
    expect(compile(where({ field: "age", op: "in", value: [] }, { model, columnExpr }))).toEqual({
      sql: "(1=0)",
      params: [],
    });
  });

  it("not_in operator with values", () => {
    expect(
      compile(where({ field: "age", op: "not_in", value: [25, 30] }, { model, columnExpr })),
    ).toEqual({ sql: '("age" NOT IN (?, ?))', params: [25, 30] });
  });

  it("not_in operator with empty array produces 1=1", () => {
    expect(
      compile(where({ field: "age", op: "not_in", value: [] }, { model, columnExpr })).sql,
    ).toBe("(1=1)");
  });

  it("and clause", () => {
    expect(
      compile(
        where(
          {
            and: [
              { field: "age", op: "gt", value: 18 },
              { field: "is_active", op: "eq", value: true },
            ],
          },
          { model, columnExpr },
        ),
      ),
    ).toEqual({ sql: '(("age" > ?) AND ("is_active" = ?))', params: [18, true] });
  });

  it("or clause", () => {
    expect(
      compile(
        where(
          {
            or: [
              { field: "age", op: "lt", value: 18 },
              { field: "age", op: "gt", value: 65 },
            ],
          },
          { model, columnExpr },
        ),
      ),
    ).toEqual({ sql: '(("age" < ?) OR ("age" > ?))', params: [18, 65] });
  });

  it("nested and inside or", () => {
    const result = compile(
      where(
        {
          or: [
            {
              and: [
                { field: "age", op: "gt", value: 18 },
                { field: "is_active", op: "eq", value: true },
              ],
            },
            { field: "name", op: "eq", value: "admin" },
          ],
        },
        { model, columnExpr },
      ),
    );
    expect(result.sql).toBe('((("age" > ?) AND ("is_active" = ?)) OR ("name" = ?))');
    expect(result.params).toEqual([18, true, "admin"]);
  });

  it("mapValue is applied to leaf values", () => {
    const result = compile(
      where(
        { field: "is_active", op: "eq", value: true },
        { model, columnExpr, mapValue: boolToInt },
      ),
    );
    expect(result.params).toEqual([1]);
  });
});

describe("stringifyJsonParam", () => {
  it("converts plain objects to JSON strings", () => {
    expect(stringifyJsonParam({ a: 1, b: 2 })).toBe('{"a":1,"b":2}');
  });

  it("converts arrays to JSON strings", () => {
    expect(stringifyJsonParam([1, 2, 3])).toBe("[1,2,3]");
  });

  it("passes null through unchanged", () => {
    expect(stringifyJsonParam(null)).toBeNull();
  });

  it("passes Date through unchanged", () => {
    const d = new Date("2025-01-15T10:30:00Z");
    expect(stringifyJsonParam(d)).toBe(d);
  });

  it("passes Uint8Array through unchanged", () => {
    const u = new Uint8Array([1, 2, 3]);
    expect(stringifyJsonParam(u)).toBe(u);
  });

  it("passes strings through unchanged", () => {
    expect(stringifyJsonParam("hello")).toBe("hello");
  });

  it("passes numbers through unchanged", () => {
    expect(stringifyJsonParam(42)).toBe(42);
  });

  it("passes booleans through unchanged", () => {
    expect(stringifyJsonParam(true)).toBe(true);
    expect(stringifyJsonParam(false)).toBe(false);
  });
});
