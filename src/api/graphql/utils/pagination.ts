export const MAX_FIRST = 250;
export const MAX_SKIP = 5000;
export const DEFAULT_FIRST = 100;

export function clampFirst(first: number | undefined | null): number {
  if (first === undefined || first === null || first < 1) return DEFAULT_FIRST;
  return Math.min(first, MAX_FIRST);
}

export function clampSkip(skip: number | undefined | null): number {
  if (skip === undefined || skip === null || skip < 0) return 0;
  return Math.min(skip, MAX_SKIP);
}

export interface CursorData {
  created_at: string;
  asset: string;
}

export function encodeCursor(data: CursorData): string {
  const json = JSON.stringify(data);
  return Buffer.from(json, 'utf-8').toString('base64');
}

export function decodeCursor(cursor: string): CursorData | null {
  try {
    const json = Buffer.from(cursor, 'base64').toString('utf-8');
    const parsed: unknown = JSON.parse(json);
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      !('created_at' in parsed) ||
      !('asset' in parsed)
    ) {
      return null;
    }
    const obj = parsed as Record<string, unknown>;
    if (typeof obj.created_at !== 'string' || typeof obj.asset !== 'string') {
      return null;
    }
    return { created_at: obj.created_at, asset: obj.asset };
  } catch {
    return null;
  }
}

export function buildCursorClause(
  cursor: string | undefined | null,
  orderDirection: 'asc' | 'desc',
  startParamIndex: number,
): { sql: string; params: unknown[]; paramIndex: number } | null {
  if (!cursor) return null;

  const data = decodeCursor(cursor);
  if (!data) return null;

  const op = orderDirection === 'desc' ? '<' : '>';
  const sql = `AND (created_at, asset) ${op} ($${startParamIndex}, $${startParamIndex + 1})`;

  return {
    sql,
    params: [data.created_at, data.asset],
    paramIndex: startParamIndex + 2,
  };
}
