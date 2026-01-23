/**
 * ZSTD Compression utilities for metadata storage
 *
 * Format:
 * - First byte: 0x00 = uncompressed, 0x01 = ZSTD compressed
 * - Rest: actual data (raw or compressed)
 *
 * Threshold: 256 bytes (compress only if larger)
 */

import { compress as zstdCompress, decompress as zstdDecompress } from '@mongodb-js/zstd';
import { createChildLogger } from '../logger.js';

const logger = createChildLogger('compression');

// Compression settings
const COMPRESS_THRESHOLD = 256; // bytes
const ZSTD_LEVEL = 3; // Good balance of speed and ratio

// Prefix bytes
const PREFIX_RAW = 0x00;
const PREFIX_ZSTD = 0x01;

/**
 * Compress data for storage (with threshold check)
 * Returns: [prefix byte] + [data or compressed data]
 */
export async function compressForStorage(data: Buffer): Promise<Buffer> {
  // Small data: store raw with prefix
  if (data.length <= COMPRESS_THRESHOLD) {
    return Buffer.concat([Buffer.from([PREFIX_RAW]), data]);
  }

  try {
    const compressed = await zstdCompress(data, ZSTD_LEVEL);

    // Only use compression if it actually saves space
    if (compressed.length < data.length) {
      logger.debug({
        original: data.length,
        compressed: compressed.length,
        ratio: ((data.length - compressed.length) / data.length * 100).toFixed(1) + '%'
      }, 'Data compressed');
      return Buffer.concat([Buffer.from([PREFIX_ZSTD]), compressed]);
    }

    // Compression didn't help, store raw
    return Buffer.concat([Buffer.from([PREFIX_RAW]), data]);
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Compression failed, storing raw');
    return Buffer.concat([Buffer.from([PREFIX_RAW]), data]);
  }
}

/**
 * Decompress data from storage
 * Handles: prefixed data (new format) and legacy unprefixed data
 */
export async function decompressFromStorage(data: Buffer): Promise<Buffer> {
  if (data.length === 0) {
    return data;
  }

  const prefix = data[0];

  // New format: prefixed data
  if (prefix === PREFIX_RAW) {
    return data.slice(1);
  }

  if (prefix === PREFIX_ZSTD) {
    try {
      const decompressed = await zstdDecompress(data.slice(1));
      return decompressed;
    } catch (error: any) {
      logger.error({ error: error.message }, 'Decompression failed');
      throw new Error(`Decompression failed: ${error.message}`);
    }
  }

  // Legacy format: no prefix, return as-is
  // This handles data stored before compression was added
  return data;
}

/**
 * Check if data is compressed (has ZSTD prefix)
 */
export function isCompressed(data: Buffer): boolean {
  return data.length > 0 && data[0] === PREFIX_ZSTD;
}

/**
 * Synchronous compression (for use in sync contexts)
 * Note: Slightly slower than async version
 */
export function compressForStorageSync(data: Buffer): Buffer {
  // Small data: store raw with prefix
  if (data.length <= COMPRESS_THRESHOLD) {
    return Buffer.concat([Buffer.from([PREFIX_RAW]), data]);
  }

  // For sync context, just store raw (async compression preferred)
  return Buffer.concat([Buffer.from([PREFIX_RAW]), data]);
}

// Export constants for testing
export const COMPRESSION_THRESHOLD = COMPRESS_THRESHOLD;
export const COMPRESSION_LEVEL = ZSTD_LEVEL;
