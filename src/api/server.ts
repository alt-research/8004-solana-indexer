import { createServer } from "http";
import { createYoga, createSchema } from "graphql-yoga";
import { PrismaClient } from "@prisma/client";
import { typeDefs } from "./schema.js";
import { resolvers, ResolverContext } from "./resolvers.js";
import { Processor } from "../indexer/processor.js";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("graphql-server");

export interface GraphQLServerOptions {
  prisma: PrismaClient;
  processor: Processor;
  port?: number;
}

export async function createGraphQLServer(options: GraphQLServerOptions) {
  const { prisma, processor, port = config.graphqlPort } = options;

  const schema = createSchema({
    typeDefs,
    resolvers,
  });

  const yoga = createYoga({
    schema,
    context: (): ResolverContext => ({
      prisma,
      processor,
    }),
    graphiql: {
      title: "8004 Indexer GraphQL",
      defaultQuery: `# Welcome to 8004 Indexer GraphQL API
#
# Example queries:

# Get all agents
query GetAgents {
  agents(limit: 10) {
    id
    owner
    nftName
    uri
    feedbackCount
    averageScore
  }
}

# Get agent with feedbacks
query GetAgentWithFeedbacks($id: ID!) {
  agent(id: $id) {
    id
    owner
    nftName
    feedbacks(limit: 5) {
      score
      tag1
      tag2
      client
    }
  }
}

# Get indexer stats
query GetStats {
  stats {
    totalAgents
    totalFeedbacks
    totalValidations
    lastProcessedSignature
  }
  indexerStatus {
    running
    mode
    pollerActive
    wsActive
  }
}
`,
    },
    logging: {
      debug: (...args) => logger.debug(args),
      info: (...args) => logger.info(args),
      warn: (...args) => logger.warn(args),
      error: (...args) => logger.error(args),
    },
    cors: {
      origin: "*",
      methods: ["GET", "POST", "OPTIONS"],
    },
  });

  const server = createServer(yoga);

  return {
    server,
    start: () =>
      new Promise<void>((resolve) => {
        server.listen(port, () => {
          logger.info(
            { port, graphiql: `http://localhost:${port}/graphql` },
            "GraphQL server started"
          );
          resolve();
        });
      }),
    stop: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => {
          if (err) reject(err);
          else resolve();
        });
      }),
  };
}
