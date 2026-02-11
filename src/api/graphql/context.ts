import type { Pool } from 'pg';
import type { PrismaClient } from '@prisma/client';
import { createDataLoaders, type DataLoaders } from './dataloaders.js';

export interface GraphQLContext {
  pool: Pool;
  prisma: PrismaClient | null;
  loaders: DataLoaders;
  networkMode: string;
}

export interface GraphQLServerConfig {
  pool: Pool;
  prisma?: PrismaClient | null;
}

export function createContext(config: GraphQLServerConfig) {
  const networkMode = process.env.SOLANA_NETWORK === 'mainnet-beta' ? 'mainnet' : 'devnet';

  return (): GraphQLContext => {
    const loaders = createDataLoaders(config.pool);
    return {
      pool: config.pool,
      prisma: config.prisma ?? null,
      loaders,
      networkMode,
    };
  };
}
