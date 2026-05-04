import type { Field, Model, Select } from "../../types";
import { mapNumeric } from "./common";

/**
 * A Fragment keeps SQL logic and dynamic data separate to prevent injection.
 * It is structured to be compatible with TemplateStringsArray for safe driver calls.
 */
export interface Fragment {
  strings: string[];
  params: unknown[];
}

/** Shared contracts for SQL executors */
export interface QueryExecutor {
  all(query: Fragment): Promise<Record<string, unknown>[]>;
  get(query: Fragment): Promise<Record<string, unknown> | undefined | null>;
  run(query: Fragment): Promise<{ changes: number }>;
  transaction<T>(fn: (executor: QueryExecutor) => Promise<T>): Promise<T>;
  readonly inTransaction: boolean;
}

export function isQueryExecutor(obj: unknown): obj is QueryExecutor {
  if (typeof obj !== "object" || obj === null) return false;
  return (
    "all" in obj &&
    "run" in obj &&
    typeof (obj as Record<string, unknown>)["all"] === "function" &&
    typeof (obj as Record<string, unknown>)["run"] === "function"
  );
}

/**
 * Maps a raw database row to the inferred model type T.
 * Handles JSON parsing, boolean conversion, and numeric mapping.
 */
export function toRow<T extends Record<string, unknown>>(
  model: Model,
  row: Record<string, unknown>,
  select?: Select<T>,
): T {
  const fields = model.fields;
  const res: Record<string, unknown> = {};
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- select fields are strings
  const keys = (select as readonly string[]) ?? Object.keys(row);

  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = row[k];
    const spec = fields[k];
    if (spec === undefined || val === undefined || val === null) {
      res[k] = val;
      continue;
    }
    if (spec.type === "json" || spec.type === "json[]") {
      res[k] = typeof val === "string" ? JSON.parse(val) : val;
    } else if (spec.type === "boolean") {
      // Postgres returns boolean, SQLite returns 1/0
      res[k] = val === true || val === 1;
    } else if (spec.type === "number" || spec.type === "timestamp") {
      res[k] = mapNumeric(val);
    } else {
      res[k] = val;
    }
  }
  // eslint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- mapped fields match the shape of T
  return res as T;
}

/**
 * Prepares a data object for database insertion/update.
 * Handles JSON stringification and optional adapter-specific mapping.
 */
export function toDbRow(
  model: Model,
  data: Record<string, unknown>,
  mapValue?: (val: unknown, field: Field) => unknown,
): Record<string, unknown> {
  const fields = model.fields;
  const res: Record<string, unknown> = {};
  const keys = Object.keys(data);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const val = data[k];
    const spec = fields[k];
    if (val === undefined) continue;

    if (val === null) {
      res[k] = null;
      continue;
    }

    if (spec === undefined) {
      res[k] = val;
      continue;
    }

    let processed = val;
    if (spec.type === "json" || spec.type === "json[]") {
      processed = JSON.stringify(val);
    }

    res[k] = mapValue ? mapValue(processed, spec) : processed;
  }
  return res;
}

/**
 * Concatenates multiple fragments with a separator.
 */
export function join(fragments: Fragment[], separator: string): Fragment {
  if (fragments.length === 0) return { strings: [""], params: [] };

  const strings = [...fragments[0]!.strings];
  const params = [...fragments[0]!.params];

  for (let i = 1; i < fragments.length; i++) {
    const f = fragments[i]!;
    strings[strings.length - 1] += separator + f.strings[0];
    for (let j = 1; j < f.strings.length; j++) {
      strings.push(f.strings[j]!);
    }
    for (let j = 0; j < f.params.length; j++) {
      params.push(f.params[j]);
    }
  }

  return { strings, params };
}

/**
 * Wraps a fragment with a prefix and suffix.
 */
export function wrap(fragment: Fragment, prefix: string, suffix: string): Fragment {
  const strings = [...fragment.strings];
  strings[0] = prefix + strings[0]!;
  strings[strings.length - 1] += suffix;
  return { strings, params: [...fragment.params] };
}

