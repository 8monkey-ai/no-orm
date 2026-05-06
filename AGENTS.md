# AGENTS.md

## Purpose

This file defines the architectural principles and implementation standards for contributing to `@8monkey/no-orm`.

## Core Principles

1.  **Tiny Persistence Core**: We prioritize a small footprint. Avoid adding new abstractions or runtime dependencies.
2.  **Schema-First & Static**: The schema is the source of truth. Type inference must be zero-cost.
3.  **Runtime-Agnostic**: All code must run across Bun, Node.js, Deno, and Edge environments.
4.  **Performance by Default**: We accept targeted complexity in internal hot paths (CRUD loops) to ensure the library adds minimal overhead to the underlying drivers.

## Architecture

- **The Adapter Pattern**: Every storage engine implements the `Adapter` interface. Adapters are stateful regarding their `schema` (passed at construction).
- **Autonomous Orchestration**: Each adapter owns its specific syntax orchestration (e.g., SQL template assembly, `RETURNING`, `ON CONFLICT`).
- **Shared Atomic Logic**: Shared logic for primary keys, pagination AST, and value helpers lives in `utils/common.ts`. SQL adapters utilize `utils/sql.ts` for atomic clause generation (`where`, `set`, `sort`).
- **QueryExecutor Abstraction**: Database drivers are wrapped in a `QueryExecutor` with a uniform interface (`all`/`get`/`run`/`transaction`). This allows the same adapter logic to support multiple drivers (e.g., `pg`, `postgres.js`, `bun:sqlite`).

## Implementation Standards

### TypeScript & Type Safety
- **Use `unknown` over `any`**: All internal signatures must use `unknown` or concrete types. Use narrowing (guards/typeof) before access.
- **Justified Assertions**: Use `eslint-disable-next-line` with a short, specific reason for unavoidable type assertions (e.g., at adapter boundaries where `RowData -> T`).
- **Strict Linting**: Do not modify `.oxlintrc.json`. Fix the code to satisfy the rules.

### High-Performance Internals
These rules apply to all internal adapter methods and shared utility loops:
- **No Object Spreads**: Use `Object.assign({}, ...)` instead of `{ ... }` to ensure hidden class stability and reduce transpilation overhead in hot paths.
- **Avoid `delete`**: Do not delete properties from objects (deoptimizes V8/JSC). Set to `undefined` or construct a new object.
- **Indexed Loops**: Prefer `for (let i = 0; i < arr.length; i++)` over `for...of` or `.forEach` to avoid iterator protocol overhead.
- **Sync Efficiency**: If an operation is naturally synchronous (like the `MemoryAdapter`), return `Promise.resolve(value)` instead of marking the method `async` to avoid unnecessary microtask scheduling.

## Dependency Strategy

- **Optional Peer Dependencies**: All database drivers (e.g., `pg`, `better-sqlite3`, `lru-cache`) are optional peer dependencies. Users only install what they use.
- **Driver Portability**: Ensure that unused driver imports are never evaluated (utilize separate entrypoints or dynamic checks).
- **Type Resolution**: All peer dependencies must have corresponding types in `devDependencies` to ensure `bun run typecheck` passes.

## Change Workflow

1.  **Minimal Edits**: Keep changes localized. Avoid broad refactors unless explicitly requested.
2.  **Order Preservation**: Do not rearrange existing classes or methods. Maintaining original order ensures clean git diffs and efficient review.
3.  **Explain the "Why"**: Retain or add comments explaining architectural choices (e.g., sequential DDL, driver detection order).
4.  **Verification**: 
    - Check `package.json` for current scripts.
    - Run linting, type-checking, and tests (`bun test`) before finishing.
    - Do not run `bun run clean` unless explicitly requested (`git clean -fdx`).

## PR/Commit Checklist

- [ ] Change is scoped to requested behavior.
- [ ] Types compile with zero errors.
- [ ] Lint/Format passes.
- [ ] Tests pass.
- [ ] No new `any` types or unjustified `eslint-disable`.
- [ ] No object spreads or `delete` in adapter hot paths.
- [ ] README updated if public API changed.
