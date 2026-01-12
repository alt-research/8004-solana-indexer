import { PrismaClient } from "@prisma/client";
import { config, validateConfig } from "./config.js";
import { logger } from "./logger.js";
import { Processor } from "./indexer/processor.js";
import { createGraphQLServer } from "./api/server.js";

async function main() {
  try {
    validateConfig();
  } catch (error) {
    logger.fatal({ error }, "Configuration validation failed");
    process.exit(1);
  }

  logger.info(
    {
      programId: config.programId,
      rpcUrl: config.rpcUrl,
      indexerMode: config.indexerMode,
    },
    "Starting 8004 Solana Indexer"
  );

  const prisma = new PrismaClient();

  try {
    await prisma.$connect();
    logger.info("Database connected");
  } catch (error) {
    logger.fatal({ error }, "Failed to connect to database");
    process.exit(1);
  }

  const processor = new Processor(prisma);
  const graphqlServer = await createGraphQLServer({ prisma, processor });

  await graphqlServer.start();
  await processor.start();

  const shutdown = async (signal: string) => {
    logger.info({ signal }, "Shutdown signal received");

    try {
      await processor.stop();
      await graphqlServer.stop();
      await prisma.$disconnect();
      logger.info("Shutdown complete");
      process.exit(0);
    } catch (error) {
      logger.error({ error }, "Error during shutdown");
      process.exit(1);
    }
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  logger.info("8004 Solana Indexer is running");
}

main().catch((error) => {
  logger.fatal({ error }, "Unhandled error");
  process.exit(1);
});
