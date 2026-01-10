import { PrismaClient, Prisma } from "@prisma/client";
import { Processor } from "../indexer/processor.js";

export interface ResolverContext {
  prisma: PrismaClient;
  processor: Processor;
}

type AgentOrderBy = "CREATED_AT_ASC" | "CREATED_AT_DESC" | "UPDATED_AT_ASC" | "UPDATED_AT_DESC";
type FeedbackOrderBy = "CREATED_AT_ASC" | "CREATED_AT_DESC" | "SCORE_ASC" | "SCORE_DESC";

function getAgentOrderBy(orderBy?: AgentOrderBy): Prisma.AgentOrderByWithRelationInput {
  switch (orderBy) {
    case "CREATED_AT_ASC":
      return { createdAt: "asc" };
    case "UPDATED_AT_ASC":
      return { updatedAt: "asc" };
    case "UPDATED_AT_DESC":
      return { updatedAt: "desc" };
    case "CREATED_AT_DESC":
    default:
      return { createdAt: "desc" };
  }
}

function getFeedbackOrderBy(orderBy?: FeedbackOrderBy): Prisma.FeedbackOrderByWithRelationInput {
  switch (orderBy) {
    case "CREATED_AT_ASC":
      return { createdAt: "asc" };
    case "SCORE_ASC":
      return { score: "asc" };
    case "SCORE_DESC":
      return { score: "desc" };
    case "CREATED_AT_DESC":
    default:
      return { createdAt: "desc" };
  }
}

export const resolvers = {
  Query: {
    // Single entity queries
    agent: async (
      _: unknown,
      { id }: { id: string },
      { prisma }: ResolverContext
    ) => {
      return prisma.agent.findUnique({ where: { id } });
    },

    feedback: async (
      _: unknown,
      { id }: { id: string },
      { prisma }: ResolverContext
    ) => {
      return prisma.feedback.findUnique({ where: { id } });
    },

    validation: async (
      _: unknown,
      { id }: { id: string },
      { prisma }: ResolverContext
    ) => {
      return prisma.validation.findUnique({ where: { id } });
    },

    registry: async (
      _: unknown,
      { id }: { id: string },
      { prisma }: ResolverContext
    ) => {
      return prisma.registry.findUnique({ where: { id } });
    },

    // List queries
    agents: async (
      _: unknown,
      args: {
        owner?: string;
        collection?: string;
        registry?: string;
        limit?: number;
        offset?: number;
        orderBy?: AgentOrderBy;
      },
      { prisma }: ResolverContext
    ) => {
      const where: Prisma.AgentWhereInput = {};
      if (args.owner) where.owner = args.owner;
      if (args.collection) where.collection = args.collection;
      if (args.registry) where.registry = args.registry;

      return prisma.agent.findMany({
        where,
        take: args.limit || 50,
        skip: args.offset || 0,
        orderBy: getAgentOrderBy(args.orderBy),
      });
    },

    feedbacks: async (
      _: unknown,
      args: {
        agentId?: string;
        client?: string;
        minScore?: number;
        maxScore?: number;
        tag?: string;
        revoked?: boolean;
        limit?: number;
        offset?: number;
        orderBy?: FeedbackOrderBy;
      },
      { prisma }: ResolverContext
    ) => {
      const where: Prisma.FeedbackWhereInput = {};
      if (args.agentId) where.agentId = args.agentId;
      if (args.client) where.client = args.client;
      if (args.minScore !== undefined) where.score = { gte: args.minScore };
      if (args.maxScore !== undefined) {
        where.score = { ...where.score as object, lte: args.maxScore };
      }
      if (args.tag) {
        where.OR = [{ tag1: args.tag }, { tag2: args.tag }];
      }
      if (args.revoked !== undefined) where.revoked = args.revoked;

      return prisma.feedback.findMany({
        where,
        take: args.limit || 50,
        skip: args.offset || 0,
        orderBy: getFeedbackOrderBy(args.orderBy),
      });
    },

    validations: async (
      _: unknown,
      args: {
        agentId?: string;
        validator?: string;
        requester?: string;
        pending?: boolean;
        limit?: number;
        offset?: number;
      },
      { prisma }: ResolverContext
    ) => {
      const where: Prisma.ValidationWhereInput = {};
      if (args.agentId) where.agentId = args.agentId;
      if (args.validator) where.validator = args.validator;
      if (args.requester) where.requester = args.requester;
      if (args.pending !== undefined) {
        where.response = args.pending ? null : { not: null };
      }

      return prisma.validation.findMany({
        where,
        take: args.limit || 50,
        skip: args.offset || 0,
        orderBy: { createdAt: "desc" },
      });
    },

    registries: async (
      _: unknown,
      args: {
        registryType?: string;
        authority?: string;
        limit?: number;
        offset?: number;
      },
      { prisma }: ResolverContext
    ) => {
      const where: Prisma.RegistryWhereInput = {};
      if (args.registryType) where.registryType = args.registryType;
      if (args.authority) where.authority = args.authority;

      return prisma.registry.findMany({
        where,
        take: args.limit || 50,
        skip: args.offset || 0,
        orderBy: { createdAt: "desc" },
      });
    },

    // Stats
    stats: async (_: unknown, __: unknown, { prisma }: ResolverContext) => {
      const [
        totalAgents,
        totalFeedbacks,
        totalValidations,
        totalRegistries,
        indexerState,
      ] = await Promise.all([
        prisma.agent.count(),
        prisma.feedback.count(),
        prisma.validation.count(),
        prisma.registry.count(),
        prisma.indexerState.findUnique({ where: { id: "main" } }),
      ]);

      return {
        totalAgents,
        totalFeedbacks,
        totalValidations,
        totalRegistries,
        lastProcessedSignature: indexerState?.lastSignature,
        lastProcessedSlot: indexerState?.lastSlot,
        updatedAt: indexerState?.updatedAt,
      };
    },

    indexerStatus: (_: unknown, __: unknown, { processor }: ResolverContext) => {
      return processor.getStatus();
    },

    // Search
    searchAgents: async (
      _: unknown,
      { query, limit }: { query: string; limit?: number },
      { prisma }: ResolverContext
    ) => {
      return prisma.agent.findMany({
        where: {
          OR: [
            { id: { contains: query } },
            { owner: { contains: query } },
            { nftName: { contains: query, mode: "insensitive" } },
          ],
        },
        take: limit || 10,
        orderBy: { createdAt: "desc" },
      });
    },
  },

  // Field resolvers
  Agent: {
    metadata: async (
      agent: { id: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.agentMetadata.findMany({
        where: { agentId: agent.id },
      });
    },

    feedbacks: async (
      agent: { id: string },
      args: { limit?: number; offset?: number; revoked?: boolean },
      { prisma }: ResolverContext
    ) => {
      const where: Prisma.FeedbackWhereInput = { agentId: agent.id };
      if (args.revoked !== undefined) where.revoked = args.revoked;

      return prisma.feedback.findMany({
        where,
        take: args.limit || 50,
        skip: args.offset || 0,
        orderBy: { createdAt: "desc" },
      });
    },

    validations: async (
      agent: { id: string },
      args: { limit?: number; offset?: number; pending?: boolean },
      { prisma }: ResolverContext
    ) => {
      const where: Prisma.ValidationWhereInput = { agentId: agent.id };
      if (args.pending !== undefined) {
        where.response = args.pending ? null : { not: null };
      }

      return prisma.validation.findMany({
        where,
        take: args.limit || 50,
        skip: args.offset || 0,
        orderBy: { createdAt: "desc" },
      });
    },

    feedbackCount: async (
      agent: { id: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.feedback.count({
        where: { agentId: agent.id, revoked: false },
      });
    },

    averageScore: async (
      agent: { id: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      const result = await prisma.feedback.aggregate({
        where: { agentId: agent.id, revoked: false },
        _avg: { score: true },
      });
      return result._avg.score;
    },

    validationCount: async (
      agent: { id: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.validation.count({
        where: { agentId: agent.id },
      });
    },
  },

  Feedback: {
    agent: async (
      feedback: { agentId: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.agent.findUnique({
        where: { id: feedback.agentId },
      });
    },

    responses: async (
      feedback: { id: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.feedbackResponse.findMany({
        where: { feedbackId: feedback.id },
        orderBy: { createdAt: "asc" },
      });
    },
  },

  FeedbackResponse: {
    feedback: async (
      response: { feedbackId: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.feedback.findUnique({
        where: { id: response.feedbackId },
      });
    },
  },

  Validation: {
    agent: async (
      validation: { agentId: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.agent.findUnique({
        where: { id: validation.agentId },
      });
    },

    isPending: (validation: { response: number | null }) => {
      return validation.response === null;
    },
  },

  Registry: {
    agentCount: async (
      registry: { collection: string },
      _: unknown,
      { prisma }: ResolverContext
    ) => {
      return prisma.agent.count({
        where: { collection: registry.collection },
      });
    },
  },

  // Scalar resolvers
  DateTime: {
    serialize: (value: Date) => value.toISOString(),
    parseValue: (value: string) => new Date(value),
  },

  BigInt: {
    serialize: (value: bigint) => value.toString(),
    parseValue: (value: string) => BigInt(value),
  },

  Bytes: {
    serialize: (value: Buffer) => value.toString("hex"),
    parseValue: (value: string) => Buffer.from(value, "hex"),
  },
};
