/**
 * Unit tests for compression utilities
 */

import { describe, it, expect } from "vitest";
import {
  compressForStorage,
  decompressFromStorage,
  isCompressed,
  COMPRESSION_THRESHOLD,
} from "../../src/utils/compression.js";

describe("Compression Utilities", () => {
  describe("compressForStorage", () => {
    it("should add PREFIX_RAW for small data", async () => {
      const smallData = Buffer.from("small");
      const result = await compressForStorage(smallData);

      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.slice(1).toString()).toBe("small");
    });

    it("should respect COMPRESSION_THRESHOLD", async () => {
      // Data exactly at threshold should not be compressed
      const atThreshold = Buffer.alloc(COMPRESSION_THRESHOLD, "x");
      const result = await compressForStorage(atThreshold);

      expect(result[0]).toBe(0x00); // PREFIX_RAW (not compressed)
    });

    it("should compress large data with ZSTD", async () => {
      // Repetitive data compresses well
      const largeData = Buffer.alloc(1024, "x");
      const result = await compressForStorage(largeData);

      expect(result[0]).toBe(0x01); // PREFIX_ZSTD
      expect(result.length).toBeLessThan(largeData.length);
    });

    it("should not compress if compression makes it larger", async () => {
      // Random data doesn't compress well
      const randomData = Buffer.alloc(300);
      for (let i = 0; i < randomData.length; i++) {
        randomData[i] = Math.floor(Math.random() * 256);
      }

      const result = await compressForStorage(randomData);

      // Should fall back to raw storage if compression doesn't help
      expect(result[0]).toBe(0x00); // PREFIX_RAW
      expect(result.length).toBe(randomData.length + 1);
    });
  });

  describe("decompressFromStorage", () => {
    it("should handle empty buffer", async () => {
      const result = await decompressFromStorage(Buffer.alloc(0));
      expect(result.length).toBe(0);
    });

    it("should strip PREFIX_RAW and return data", async () => {
      const original = Buffer.from("hello world");
      const stored = Buffer.concat([Buffer.from([0x00]), original]);

      const result = await decompressFromStorage(stored);
      expect(result.toString()).toBe("hello world");
    });

    it("should decompress PREFIX_ZSTD data", async () => {
      // First compress some data
      const original = Buffer.alloc(1024, "y");
      const compressed = await compressForStorage(original);

      expect(compressed[0]).toBe(0x01); // Should be compressed

      // Then decompress
      const decompressed = await decompressFromStorage(compressed);
      expect(decompressed.toString()).toBe(original.toString());
    });

    it("should handle legacy data without prefix", async () => {
      // Legacy data: no prefix
      const legacy = Buffer.from("legacy data");

      const result = await decompressFromStorage(legacy);
      expect(result.toString()).toBe("legacy data");
    });

    it("should round-trip correctly", async () => {
      const testCases = [
        Buffer.from("small"),
        Buffer.from("medium data " + "x".repeat(100)),
        Buffer.alloc(1024, "large"),
        Buffer.from(JSON.stringify({ name: "test", value: 123 })),
      ];

      for (const original of testCases) {
        const compressed = await compressForStorage(original);
        const decompressed = await decompressFromStorage(compressed);
        expect(decompressed.toString()).toBe(original.toString());
      }
    });
  });

  describe("isCompressed", () => {
    it("should return false for empty buffer", () => {
      expect(isCompressed(Buffer.alloc(0))).toBe(false);
    });

    it("should return true for PREFIX_ZSTD", () => {
      const data = Buffer.concat([Buffer.from([0x01]), Buffer.from("data")]);
      expect(isCompressed(data)).toBe(true);
    });

    it("should return false for PREFIX_RAW", () => {
      const data = Buffer.concat([Buffer.from([0x00]), Buffer.from("data")]);
      expect(isCompressed(data)).toBe(false);
    });
  });

  describe("JSON metadata compression", () => {
    it("should efficiently compress JSON metadata", async () => {
      const jsonMetadata = JSON.stringify({
        name: "Test Agent",
        description: "A test agent with a longer description that should compress well",
        skills: ["web-search", "code-generation", "data-analysis"],
        domains: ["finance", "tech", "healthcare"],
        endpoints: [
          { type: "mcp", url: "https://api.example.com/mcp" },
          { type: "a2a", url: "https://api.example.com/a2a" },
        ],
        customFields: {
          field1: "value1",
          field2: "value2 with some longer text to help compression",
        },
      });

      const buffer = Buffer.from(jsonMetadata);
      const compressed = await compressForStorage(buffer);

      // JSON should compress well (>50% for this test data)
      if (buffer.length > COMPRESSION_THRESHOLD) {
        expect(compressed.length).toBeLessThan(buffer.length * 0.7);
      }

      // Round-trip should preserve data
      const decompressed = await decompressFromStorage(compressed);
      expect(decompressed.toString()).toBe(jsonMetadata);
    });
  });
});
