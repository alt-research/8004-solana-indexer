import { createYoga } from 'graphql-yoga';
import { makeExecutableSchema } from '@graphql-tools/schema';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import type { Pool } from 'pg';
import type { PrismaClient } from '@prisma/client';

import { resolvers } from './resolvers/index.js';
import { createContext } from './context.js';
import { analyzeQuery } from './plugins/complexity.js';
import { analyzeDepth } from './plugins/depth-limit.js';
import { createChildLogger } from '../../logger.js';
import type { DocumentNode } from 'graphql';

const logger = createChildLogger('graphql');

const __dirname = dirname(fileURLToPath(import.meta.url));

function loadTypeDefs(): string {
  return readFileSync(join(__dirname, 'schema.graphql'), 'utf-8');
}

export interface GraphQLHandlerOptions {
  pool: Pool;
  prisma?: PrismaClient | null;
}

export function createGraphQLHandler(options: GraphQLHandlerOptions) {
  const typeDefs = loadTypeDefs();
  const schema = makeExecutableSchema({ typeDefs, resolvers });

  const contextFactory = createContext({
    pool: options.pool,
    prisma: options.prisma,
  });

  const isProduction = process.env.NODE_ENV === 'production';

  const yoga = createYoga({
    schema,
    context: contextFactory,
    graphiql: !isProduction,
    landingPage: false,
    maskedErrors: isProduction,
    logging: {
      debug: (...args: unknown[]) => logger.debug(args[0], 'GraphQL debug'),
      info: (...args: unknown[]) => logger.info(args[0], 'GraphQL info'),
      warn: (...args: unknown[]) => logger.warn(args[0], 'GraphQL warn'),
      error: (...args: unknown[]) => logger.error(args[0], 'GraphQL error'),
    },
    plugins: [
      {
        onParse() {
          return ({ result }: { result: DocumentNode | Error | null }) => {
            if (!result || result instanceof Error) return;

            const complexityResult = analyzeQuery(result);
            if (!complexityResult.allowed) {
              logger.warn({
                cost: complexityResult.cost,
                reason: complexityResult.reason,
              }, 'Query rejected: complexity');
              throw new Error(complexityResult.reason);
            }

            const depthResult = analyzeDepth(result);
            if (!depthResult.allowed) {
              logger.warn({
                depth: depthResult.depth,
                reason: depthResult.reason,
              }, 'Query rejected: depth');
              throw new Error(depthResult.reason);
            }

            logger.debug({
              cost: complexityResult.cost,
              depth: depthResult.depth,
            }, 'Query analyzed');
          };
        },
      },
    ],
  });

  logger.info({ graphiql: !isProduction }, 'GraphQL handler created');
  return yoga;
}
