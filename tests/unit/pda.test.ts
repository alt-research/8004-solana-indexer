import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey, Connection } from "@solana/web3.js";

// Mock 8004-solana SDK exports
vi.mock("8004-solana", () => ({
  PROGRAM_ID: new PublicKey("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C"),
  ATOM_ENGINE_PROGRAM_ID: new PublicKey("AToMufS4QD6hEXvcvBDg9m1AHeCLpmZQsyfYa5h9MwAF"),
  MPL_CORE_PROGRAM_ID: new PublicKey("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d"),
}));

import {
  getRootConfigPda,
  getRegistryConfigPda,
  getValidationConfigPda,
  getAgentPda,
  getValidationRequestPda,
  getMetadataEntryPda,
  computeKeyHash,
  getAtomConfigPda,
  getAtomStatsPda,
  parseAssetPubkey,
  isValidPubkey,
  fetchRootConfig,
  fetchRegistryConfig,
  fetchBaseCollection,
  AGENT_REGISTRY_PROGRAM_ID,
  ATOM_ENGINE_PROGRAM_ID,
  MPL_CORE_PROGRAM_ID,
} from "../../src/utils/pda.js";

describe("PDA Utilities", () => {
  const testAsset = new PublicKey(new Uint8Array(32).fill(1));
  const testCollection = new PublicKey(new Uint8Array(32).fill(2));
  const testValidator = new PublicKey(new Uint8Array(32).fill(3));

  describe("Program ID exports", () => {
    it("should export AGENT_REGISTRY_PROGRAM_ID from SDK", () => {
      expect(AGENT_REGISTRY_PROGRAM_ID).toBeDefined();
      expect(AGENT_REGISTRY_PROGRAM_ID.toBase58()).toBe("8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C");
    });

    it("should export ATOM_ENGINE_PROGRAM_ID from SDK", () => {
      expect(ATOM_ENGINE_PROGRAM_ID).toBeDefined();
      expect(ATOM_ENGINE_PROGRAM_ID.toBase58()).toBe("AToMufS4QD6hEXvcvBDg9m1AHeCLpmZQsyfYa5h9MwAF");
    });

    it("should export MPL_CORE_PROGRAM_ID from SDK", () => {
      expect(MPL_CORE_PROGRAM_ID).toBeDefined();
    });
  });

  describe("getRootConfigPda", () => {
    it("should derive root config PDA with default program ID", () => {
      const [pda, bump] = getRootConfigPda();
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
      expect(bump).toBeLessThanOrEqual(255);
    });

    it("should derive root config PDA with custom program ID", () => {
      const customProgramId = new PublicKey(new Uint8Array(32).fill(9));
      const [pda1] = getRootConfigPda();
      const [pda2] = getRootConfigPda(customProgramId);
      expect(pda1.toBase58()).not.toBe(pda2.toBase58());
    });

    it("should be deterministic", () => {
      const [pda1] = getRootConfigPda();
      const [pda2] = getRootConfigPda();
      expect(pda1.toBase58()).toBe(pda2.toBase58());
    });
  });

  describe("getRegistryConfigPda", () => {
    it("should derive registry config PDA from collection", () => {
      const [pda, bump] = getRegistryConfigPda(testCollection);
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });

    it("should produce different PDAs for different collections", () => {
      const otherCollection = new PublicKey(new Uint8Array(32).fill(5));
      const [pda1] = getRegistryConfigPda(testCollection);
      const [pda2] = getRegistryConfigPda(otherCollection);
      expect(pda1.toBase58()).not.toBe(pda2.toBase58());
    });
  });

  describe("getValidationConfigPda", () => {
    it("should derive validation config PDA", () => {
      const [pda, bump] = getValidationConfigPda();
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });
  });

  describe("getAgentPda", () => {
    it("should derive agent PDA from asset pubkey", () => {
      const [pda, bump] = getAgentPda(testAsset);
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });

    it("should produce different PDAs for different assets", () => {
      const otherAsset = new PublicKey(new Uint8Array(32).fill(7));
      const [pda1] = getAgentPda(testAsset);
      const [pda2] = getAgentPda(otherAsset);
      expect(pda1.toBase58()).not.toBe(pda2.toBase58());
    });
  });

  describe("getValidationRequestPda", () => {
    it("should derive validation request PDA", () => {
      const [pda, bump] = getValidationRequestPda(testAsset, testValidator, 1);
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });

    it("should produce different PDAs for different nonces", () => {
      const [pda1] = getValidationRequestPda(testAsset, testValidator, 1);
      const [pda2] = getValidationRequestPda(testAsset, testValidator, 2);
      expect(pda1.toBase58()).not.toBe(pda2.toBase58());
    });

    it("should handle bigint nonces", () => {
      const [pda1] = getValidationRequestPda(testAsset, testValidator, 1n);
      const [pda2] = getValidationRequestPda(testAsset, testValidator, 1);
      expect(pda1.toBase58()).toBe(pda2.toBase58());
    });
  });

  describe("getMetadataEntryPda", () => {
    it("should derive metadata PDA from string key", () => {
      const [pda, bump] = getMetadataEntryPda(testAsset, "description");
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });

    it("should derive metadata PDA from Uint8Array key hash", () => {
      const keyHash = computeKeyHash("description");
      const [pda1] = getMetadataEntryPda(testAsset, "description");
      const [pda2] = getMetadataEntryPda(testAsset, keyHash);
      expect(pda1.toBase58()).toBe(pda2.toBase58());
    });

    it("should produce different PDAs for different keys", () => {
      const [pda1] = getMetadataEntryPda(testAsset, "name");
      const [pda2] = getMetadataEntryPda(testAsset, "description");
      expect(pda1.toBase58()).not.toBe(pda2.toBase58());
    });

    it("should handle Uint8Array longer than 16 bytes", () => {
      const longHash = new Uint8Array(32).fill(0xab);
      const [pda] = getMetadataEntryPda(testAsset, longHash);
      expect(pda).toBeInstanceOf(PublicKey);
    });
  });

  describe("computeKeyHash", () => {
    it("should return 16 bytes of SHA256", () => {
      const hash = computeKeyHash("test");
      expect(hash).toBeInstanceOf(Uint8Array);
      expect(hash.length).toBe(16);
    });

    it("should be deterministic", () => {
      const hash1 = computeKeyHash("key1");
      const hash2 = computeKeyHash("key1");
      expect(hash1).toEqual(hash2);
    });

    it("should produce different hashes for different keys", () => {
      const hash1 = computeKeyHash("key1");
      const hash2 = computeKeyHash("key2");
      expect(hash1).not.toEqual(hash2);
    });
  });

  describe("getAtomConfigPda", () => {
    it("should derive ATOM config PDA", () => {
      const [pda, bump] = getAtomConfigPda();
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });
  });

  describe("getAtomStatsPda", () => {
    it("should derive ATOM stats PDA from asset", () => {
      const [pda, bump] = getAtomStatsPda(testAsset);
      expect(pda).toBeInstanceOf(PublicKey);
      expect(bump).toBeGreaterThanOrEqual(0);
    });

    it("should produce different PDAs for different assets", () => {
      const otherAsset = new PublicKey(new Uint8Array(32).fill(8));
      const [pda1] = getAtomStatsPda(testAsset);
      const [pda2] = getAtomStatsPda(otherAsset);
      expect(pda1.toBase58()).not.toBe(pda2.toBase58());
    });
  });

  describe("parseAssetPubkey", () => {
    it("should parse valid base58 pubkey", () => {
      const pubkey = testAsset.toBase58();
      const result = parseAssetPubkey(pubkey);
      expect(result.toBase58()).toBe(pubkey);
    });

    it("should throw for invalid pubkey", () => {
      expect(() => parseAssetPubkey("invalid")).toThrow();
    });
  });

  describe("isValidPubkey", () => {
    it("should return true for valid pubkey", () => {
      expect(isValidPubkey(testAsset.toBase58())).toBe(true);
    });

    it("should return false for invalid pubkey", () => {
      expect(isValidPubkey("not-a-pubkey")).toBe(false);
    });

    it("should return false for empty string", () => {
      expect(isValidPubkey("")).toBe(false);
    });

    it("should return true for system program", () => {
      expect(isValidPubkey("11111111111111111111111111111111")).toBe(true);
    });
  });

  describe("fetchRootConfig", () => {
    it("should return null when account does not exist", async () => {
      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const result = await fetchRootConfig(mockConnection);
      expect(result).toBeNull();
    });

    it("should throw for account data too small", async () => {
      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(50), // Too small
        }),
      } as unknown as Connection;

      await expect(fetchRootConfig(mockConnection)).rejects.toThrow("Invalid RootConfig account size");
    });

    it("should parse valid RootConfig data", async () => {
      const data = Buffer.alloc(73);
      const baseCollection = new PublicKey(new Uint8Array(32).fill(0xaa));
      const authority = new PublicKey(new Uint8Array(32).fill(0xbb));
      // discriminator (8 bytes) + base_collection (32) + authority (32) + bump (1)
      baseCollection.toBuffer().copy(data, 8);
      authority.toBuffer().copy(data, 40);
      data[72] = 255; // bump

      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue({ data }),
      } as unknown as Connection;

      const result = await fetchRootConfig(mockConnection);
      expect(result).not.toBeNull();
      expect(result!.baseCollection.toBase58()).toBe(baseCollection.toBase58());
      expect(result!.authority.toBase58()).toBe(authority.toBase58());
      expect(result!.bump).toBe(255);
    });
  });

  describe("fetchRegistryConfig", () => {
    it("should return null when account does not exist", async () => {
      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const registryPda = new PublicKey(new Uint8Array(32).fill(1));
      const result = await fetchRegistryConfig(mockConnection, registryPda);
      expect(result).toBeNull();
    });

    it("should return null for data too small", async () => {
      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue({
          data: Buffer.alloc(50),
        }),
      } as unknown as Connection;

      const registryPda = new PublicKey(new Uint8Array(32).fill(1));
      const result = await fetchRegistryConfig(mockConnection, registryPda);
      expect(result).toBeNull();
    });

    it("should parse valid RegistryConfig data", async () => {
      const data = Buffer.alloc(73);
      const collection = new PublicKey(new Uint8Array(32).fill(0xcc));
      const authority = new PublicKey(new Uint8Array(32).fill(0xdd));
      collection.toBuffer().copy(data, 8);
      authority.toBuffer().copy(data, 40);
      data[72] = 128;

      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue({ data }),
      } as unknown as Connection;

      const registryPda = new PublicKey(new Uint8Array(32).fill(1));
      const result = await fetchRegistryConfig(mockConnection, registryPda);
      expect(result).not.toBeNull();
      expect(result!.collection.toBase58()).toBe(collection.toBase58());
      expect(result!.authority.toBase58()).toBe(authority.toBase58());
      expect(result!.bump).toBe(128);
    });
  });

  describe("fetchBaseCollection", () => {
    it("should return null when root config does not exist", async () => {
      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue(null),
      } as unknown as Connection;

      const result = await fetchBaseCollection(mockConnection);
      expect(result).toBeNull();
    });

    it("should return baseCollection from RootConfig", async () => {
      const data = Buffer.alloc(73);
      const baseCollection = new PublicKey(new Uint8Array(32).fill(0xee));
      baseCollection.toBuffer().copy(data, 8);
      // authority
      new Uint8Array(32).fill(0xff).forEach((b, i) => { data[40 + i] = b; });
      data[72] = 1;

      const mockConnection = {
        getAccountInfo: vi.fn().mockResolvedValue({ data }),
      } as unknown as Connection;

      const result = await fetchBaseCollection(mockConnection);
      expect(result).not.toBeNull();
      expect(result!.toBase58()).toBe(baseCollection.toBase58());
    });
  });
});
