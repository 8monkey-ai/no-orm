import type { FieldName } from "../types";

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function isValidField<T>(field: unknown): field is FieldName<T> {
  return typeof field === "string" && field !== "";
}

export function isStringKey(key: unknown): key is string {
  return typeof key === "string" && key !== "";
}

export function isModelType<T>(obj: unknown): obj is T {
  return typeof obj === "object" && obj !== null;
}
