export function isQueryExecutor(obj: any): obj is QueryExecutor {
  return (
    obj !== null &&
    typeof obj === "object" &&
    typeof obj.all === "function" &&
    typeof obj.run === "function"
  );
}
