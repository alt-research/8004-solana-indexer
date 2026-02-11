/**
 * Sanitize binary data for PostgreSQL TEXT storage
 * Only strips NUL (0x00) bytes which cause "invalid byte sequence for encoding UTF8: 0x00"
 * All other bytes are preserved to avoid corrupting binary or non-ASCII data
 */

/**
 * Strip NUL bytes (0x00) from a Uint8Array
 * Returns a clean buffer safe for PostgreSQL TEXT storage
 */
export function stripNullBytes(data: Uint8Array): Buffer {
  const result: number[] = [];
  for (let i = 0; i < data.length; i++) {
    if (data[i] !== 0x00) {
      result.push(data[i]);
    }
  }
  return Buffer.from(result);
}

/**
 * Check if a Uint8Array contains NUL bytes (0x00)
 */
export function hasNullBytes(data: Uint8Array): boolean {
  for (let i = 0; i < data.length; i++) {
    if (data[i] === 0x00) {
      return true;
    }
  }
  return false;
}
