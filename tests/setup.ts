import { vi, beforeEach, afterEach } from "vitest";

// Mock environment variables
process.env.DATABASE_URL = "POSTGRES_DSN_REDACTED";
process.env.RPC_URL = "https://api.devnet.solana.com";
process.env.WS_URL = "wss://api.devnet.solana.com";
process.env.PROGRAM_ID = "8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C";
process.env.LOG_LEVEL = "silent";

// Mock pino logger to be silent during tests
vi.mock("pino", () => {
  const mockLogger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    fatal: vi.fn(),
    child: vi.fn(() => mockLogger),
  };
  return { default: vi.fn(() => mockLogger) };
});

// Reset mocks between tests
beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});
