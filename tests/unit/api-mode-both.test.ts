import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AddressInfo } from "net";
import type { Server } from "http";
import type { Express } from "express";

vi.mock("../../src/api/graphql/index.js", () => ({
  createGraphQLHandler: vi.fn(() => ({
    handle: (
      _req: unknown,
      res: { status: (code: number) => { json: (body: unknown) => void } }
    ) => {
      res.status(200).json({ data: { ok: true } });
    },
  })),
}));

const originalEnv = process.env;

function makePrismaStub() {
  return {
    agent: {
      findMany: vi.fn().mockResolvedValue([]),
      count: vi.fn().mockResolvedValue(0),
    },
  };
}

async function startServer(options: { prisma: any; pool: any }) {
  const { createApiServer } = await import("../../src/api/server.js");
  const app: Express = createApiServer(options);

  const server = await new Promise<Server>((resolve, reject) => {
    const started = app.listen(0, "127.0.0.1", () => resolve(started));
    started.on("error", reject);
  });

  const addr = server.address() as AddressInfo;
  return {
    server,
    baseUrl: `http://127.0.0.1:${addr.port}`,
  };
}

async function stopServer(server: Server) {
  await new Promise<void>((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

async function waitForGraphqlMount(baseUrl: string): Promise<void> {
  for (let i = 0; i < 20; i += 1) {
    const res = await fetch(`${baseUrl}/v2/graphql`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: "{ __typename }" }),
    });
    if (res.status === 200) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("GraphQL endpoint was not mounted in time");
}

describe("API_MODE=both behavior", () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = {
      ...originalEnv,
      API_MODE: "both",
      ENABLE_GRAPHQL: "true",
    };
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it("serves REST and leaves GraphQL unmounted when only Prisma is available", async () => {
    const { server, baseUrl } = await startServer({
      prisma: makePrismaStub() as any,
      pool: null as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents`);
      expect(restRes.status).toBe(200);

      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(404);
    } finally {
      await stopServer(server);
    }
  });

  it("serves GraphQL and disables REST when only Supabase pool is available", async () => {
    const { server, baseUrl } = await startServer({
      prisma: null as any,
      pool: { query: vi.fn() } as any,
    });

    try {
      const restRes = await fetch(`${baseUrl}/rest/v1/agents`);
      expect(restRes.status).toBe(410);

      await waitForGraphqlMount(baseUrl);
      const gqlRes = await fetch(`${baseUrl}/v2/graphql`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ query: "{ stats { totalAgents } }" }),
      });
      expect(gqlRes.status).toBe(200);
      const body = await gqlRes.json();
      expect(body).toEqual({ data: { ok: true } });
    } finally {
      await stopServer(server);
    }
  });
});
