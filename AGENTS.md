# AGENTS.md

## Purpose

This file gives coding agents a fast, reliable workflow for contributing to `@8monkey/no-orm`.

## Project Snapshot

- Runtime: Bun + TypeScript (ESM).
- Library type: Tiny, schema-first persistence core for TypeScript libraries.
- Not a query builder, migration framework, or full ORM runtime.
- Designed to be embedded inside other libraries.

## Technical Design Priorities

1. Simple, clean, concise, and easy-to-read / maintain code.
2. Tiny footprint — fewer files, fewer lines, fewer abstractions.
3. Modular and tree-shakable with separate entrypoints per adapter.
4. Prefer clarity by default, but accept targeted complexity in hot paths when measurable.
5. Runtime-agnostic across Bun, Node.js, Deno, and edge runtimes.

If priorities conflict, apply this order:

1. Public API compatibility
2. Runtime portability
3. Readability and style consistency
4. Hot-path performance

## Repository Map

```
src/
  types.ts              Schema, Adapter interface, Where/SortBy/Cursor types
  index.ts              Public entrypoint (re-exports types.ts)
  adapters/
    memory.ts           MemoryAdapter (LRU-cache-backed)
    postgres.ts         PostgresAdapter (Autonomous SQL + Execution)
    sqlite.ts           SqliteAdapter (Autonomous SQL + Execution)
    utils/
      common.ts         Shared PK, pagination, and value helpers
      sql.ts            QueryExecutor interface and toRow helper (shared SQL logic)
```

Each adapter file is self-contained: SQL building, driver detection, executor factories, and adapter class all live together.

## Local Commands

- Install deps: `bun install`
- Build: `bun run build`
- Type check: `bun run typecheck` (runs oxlint with `--type-check`)
- Test: `bun test`
- Lint: `bun run lint`
- Format: `bun run format`
- Full check: `bun run check` (lint + typecheck)
- Do not run `bun run clean` unless explicitly requested (`git clean -fdx`).

## Change Workflow

1. Read the touched feature area first.
2. Keep edits minimal and localized; avoid broad refactors unless asked.
3. **No Rearrangement**: Do not move existing classes, methods, or functions to different positions within a file. Maintaining the original order is required to ensure clean git diffs and facilitate efficient code reviews.
4. Retain existing architectural and defensive comments that explain "why" (e.g. sequential DDL, driver detection order, V8 optimizations).
5. Update related tests when behavior changes.
6. Run `bun run check` and `bun test` before considering a change done.
7. If formatting/linting is impacted, run `bun run format` and `bun run lint`.

- Update this file with new "Lessons Learned" or "Mistakes to Avoid" if a significant architectural shift or subtle bug is encountered.

## TypeScript Rules

### Use `unknown` over `any`

All internal method signatures must use `unknown` or concrete types, never `any`. The `Where`, `Cursor`, and `SortBy` types default to `Record<string, unknown>` — internal helpers accept this default form. Public adapter methods use the generic `Where<T>` form.

### eslint-disable comments require justification

When a type assertion is unavoidable (e.g. `RowData -> T` at adapter boundaries), use `eslint-disable-next-line` with a short reason:

```ts
// eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
return res as T;
```

Never add blanket eslint-disable at the file level. Each suppression must be on the specific line and explain why it's safe.

### Prefer `unknown` narrowing over type assertions

Use `"key" in obj` checks and `typeof` guards to narrow before accessing. Only assert when the type system provably can't express the relationship (e.g. generic `T` at adapter boundaries, structurally-typed multi-driver factories).

### Do not modify `.oxlintrc.json`

The linter config is intentionally strict (pedantic + suspicious + correctness + perf as error). Do not add rule overrides. Fix the code to satisfy the rules, or add a targeted `eslint-disable-next-line` with justification.

## Code Style Rules

### No object spreads in hot paths

In the memory adapter, every CRUD operation is a hot path. Use `Object.assign({}, source)` or `Object.assign({}, a, b)` instead of `{ ...source }` or `{ ...a, ...b }`. Object spreads generate more code in transpiled output and can be slower in tight loops.

### Avoid `delete` on objects

Deleting properties deoptimizes V8/JSC hidden classes. Set to `undefined` or construct a new object.

### Avoid `await` in synchronous code

The memory adapter methods are synchronous. Return `Promise.resolve(value)` instead of marking them `async` and using `await Promise.resolve()`. This avoids unnecessary microtask scheduling overhead.

### Use `for` loops over iterators in performance-sensitive code

Prefer `for (let i = 0; i < arr.length; i++)` over `for...of` in adapter internals. The indexed form avoids iterator protocol overhead.

## Architecture Notes

### Adapter boundary is the one place where `as T` casts are acceptable

Storage holds `Record<string, unknown>` (RowData) but the adapter interface promises `T`. The cast from `RowData -> T` happens in `applySelect` (memory) and `toRow` (sql adapters). Keep this boundary thin and document it.

### SQL logic is autonomous

Each SQL adapter class (`PostgresAdapter`, `SqliteAdapter`) implements the `Adapter` interface by owning its SQL generation and execution flow. This significantly reduces abstraction leaks, improves readability, and allows for database-specific optimizations (like `RETURNING` clauses). Shared domain logic (PKs, pagination AST) lives in `common.ts`.

### QueryExecutor is the driver abstraction

Each database driver (pg Pool, postgres.js, Bun SQL, better-sqlite3, bun:sqlite, async sqlite) gets wrapped into a `QueryExecutor` (localized to each adapter) with uniform `all`/`get`/`run`/`transaction` methods. The executor factory lives in the adapter file next to its SQL syntax helpers.

## Dependency Rules

### All database drivers are optional peer dependencies

Users only install what they use. The separate entrypoints (`@8monkey/no-orm/adapters/sqlite`, etc.) mean unused driver imports are never evaluated.

| Peer dependency      | Required by                            |
| -------------------- | -------------------------------------- |
| `lru-cache`          | `MemoryAdapter`                        |
| `better-sqlite3`     | `SqliteAdapter`                        |
| `pg`                 | `PostgresAdapter` (pg driver)          |
| `postgres`           | `PostgresAdapter` (postgres.js driver) |
| `sqlite` / `sqlite3` | `SqliteAdapter` (async driver)         |

Bun SQL and bun:sqlite require no extra dependencies — types come from `@types/bun`.

### devDependencies include all peer deps for type-checking

Every optional peer dep that provides types must also be in `devDependencies` so that `bun run typecheck` resolves all imports. This includes `lru-cache`, `postgres`, and `sqlite`. The `@types/*` packages cover `pg`, `better-sqlite3`, and `sqlite3`.

## Schema Rules

### v1 schema is intentionally minimal

Supported field types: `string`, `number`, `boolean`, `timestamp`, `json`, `json[]`. No defaults, foreign key fields are just primitive types. No relations or automated joins.

### Validations are out of scope for v1

The code includes only defensive guards (missing PK fields, PK update rejection, JSON path SQL injection prevention). It does not validate schemas at construction time, enforce field types on insert, or check string max lengths. Do not add schema validation unless explicitly requested.

### All Adapter interface methods are non-optional

`migrate()`, `transaction()`, `upsert()`, `deleteMany()`, and `count()` are all required. All three adapters implement them. The `Adapter` interface reflects this — no `?` markers.

### migrate() takes no arguments

The schema is passed to the adapter constructor. `migrate()` uses `this.schema` to bootstrap storage. This differs from the original spec in issue #3 which had `migrate(args: { schema })`.

## Testing Expectations

- Prefer focused tests close to the changed code.
- Unit tests go in `src/**/*.test.ts`.
- Cover: CRUD lifecycle, composite primary keys, select projection, all operators (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`), logical composition (`and`, `or`), null handling, JSON path filters, pagination (offset + cursor), sorting with nulls, upsert (insert/update/predicated), updateMany, deleteMany, count, transactions, LRU eviction, duplicate key rejection.
- When adding a new adapter, add integration tests exercising the full operation set.

## Guardrails

- Do not remove or rename public exports without explicit request.
- Do not add new runtime dependencies. All database drivers must be optional peer deps.
- Do not modify `.oxlintrc.json` or `tsconfig.json` without explicit request.
- Keep comments concise and only where intent is non-obvious.

## PR/Commit Checklist

- [ ] Change is scoped to requested behavior.
- [ ] Types compile (`bun run typecheck`) with zero errors.
- [ ] Lint passes (`bun run lint`) with zero errors.
- [ ] Tests pass (`bun test`).
- [ ] No new `any` types introduced.
- [ ] No new `eslint-disable` without per-line justification comment.
- [ ] No object spreads introduced in adapter hot paths.
- [ ] No dead code (unused exports, unreachable branches).
- [ ] README updated if public API changed.

## Lessons Learned & Mistakes to Avoid

- **V8 hot paths**: Avoid object spreads and `delete` in adapter CRUD loops to maintain peak performance (hidden class stability).
- **Unified Logic**: Shared logic for keyset pagination (criteria building) and JSON path extraction should live in `common.ts` to ensure consistency between Memory and SQL adapters.
