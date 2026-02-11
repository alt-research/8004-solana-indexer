import { describe, it, expect } from "vitest";
import { stripNullBytes, hasNullBytes } from "../../src/utils/sanitize.js";

describe("Sanitize Utilities", () => {
  describe("stripNullBytes", () => {
    it("should strip NUL bytes from data", () => {
      const data = new Uint8Array([0x48, 0x00, 0x65, 0x00, 0x6c]);
      const result = stripNullBytes(data);
      expect(result).toEqual(Buffer.from([0x48, 0x65, 0x6c]));
    });

    it("should return empty buffer for all-null data", () => {
      const data = new Uint8Array([0x00, 0x00, 0x00]);
      const result = stripNullBytes(data);
      expect(result.length).toBe(0);
    });

    it("should return same content for data without nulls", () => {
      const data = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]);
      const result = stripNullBytes(data);
      expect(result).toEqual(Buffer.from("Hello"));
    });

    it("should handle empty input", () => {
      const data = new Uint8Array([]);
      const result = stripNullBytes(data);
      expect(result.length).toBe(0);
    });

    it("should handle single null byte", () => {
      const data = new Uint8Array([0x00]);
      const result = stripNullBytes(data);
      expect(result.length).toBe(0);
    });

    it("should handle null bytes at start and end", () => {
      const data = new Uint8Array([0x00, 0x41, 0x42, 0x00]);
      const result = stripNullBytes(data);
      expect(result).toEqual(Buffer.from("AB"));
    });
  });

  describe("hasNullBytes", () => {
    it("should return true when data contains NUL bytes", () => {
      const data = new Uint8Array([0x48, 0x00, 0x65]);
      expect(hasNullBytes(data)).toBe(true);
    });

    it("should return false when data has no NUL bytes", () => {
      const data = new Uint8Array([0x48, 0x65, 0x6c]);
      expect(hasNullBytes(data)).toBe(false);
    });

    it("should return false for empty data", () => {
      const data = new Uint8Array([]);
      expect(hasNullBytes(data)).toBe(false);
    });

    it("should detect null byte at start", () => {
      const data = new Uint8Array([0x00, 0x41]);
      expect(hasNullBytes(data)).toBe(true);
    });

    it("should detect null byte at end", () => {
      const data = new Uint8Array([0x41, 0x00]);
      expect(hasNullBytes(data)).toBe(true);
    });

    it("should return true for all-null data", () => {
      const data = new Uint8Array([0x00, 0x00, 0x00]);
      expect(hasNullBytes(data)).toBe(true);
    });
  });
});
