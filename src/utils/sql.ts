export function quote(name: string): string {
  return `"${name}"`;
}

export function escapeLiteral(val: string): string {
  return val.replaceAll("'", "''");
}
