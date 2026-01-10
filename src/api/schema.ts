export const typeDefs = /* GraphQL */ `
  scalar DateTime
  scalar BigInt
  scalar Bytes

  type Agent {
    id: ID!
    owner: String!
    wallet: String
    uri: String!
    nftName: String!
    collection: String!
    registry: String!
    createdAt: DateTime!
    updatedAt: DateTime!
    createdTxSignature: String
    createdSlot: BigInt

    # Relations
    metadata: [AgentMetadata!]!
    feedbacks(limit: Int, offset: Int, revoked: Boolean): [Feedback!]!
    validations(limit: Int, offset: Int, pending: Boolean): [Validation!]!

    # Computed fields
    feedbackCount: Int!
    averageScore: Float
    validationCount: Int!
  }

  type AgentMetadata {
    id: ID!
    agentId: String!
    key: String!
    value: Bytes!
    immutable: Boolean!
    txSignature: String
    slot: BigInt
  }

  type Feedback {
    id: ID!
    agentId: String!
    client: String!
    feedbackIndex: BigInt!
    score: Int!
    tag1: String!
    tag2: String!
    endpoint: String!
    feedbackUri: String!
    feedbackHash: Bytes!
    revoked: Boolean!
    createdAt: DateTime!
    createdTxSignature: String
    createdSlot: BigInt

    # Relations
    agent: Agent!
    responses: [FeedbackResponse!]!
  }

  type FeedbackResponse {
    id: ID!
    feedbackId: String!
    responder: String!
    responseUri: String!
    responseHash: Bytes!
    createdAt: DateTime!
    txSignature: String
    slot: BigInt

    # Relations
    feedback: Feedback!
  }

  type Validation {
    id: ID!
    agentId: String!
    validator: String!
    requester: String!
    nonce: Int!
    requestUri: String!
    requestHash: Bytes!
    response: Int
    responseUri: String
    responseHash: Bytes
    tag: String
    createdAt: DateTime!
    respondedAt: DateTime
    requestTxSignature: String
    requestSlot: BigInt
    responseTxSignature: String
    responseSlot: BigInt

    # Relations
    agent: Agent!

    # Computed
    isPending: Boolean!
  }

  type Registry {
    id: ID!
    collection: String!
    registryType: String!
    authority: String!
    baseIndex: Int
    createdAt: DateTime!
    txSignature: String
    slot: BigInt

    # Computed
    agentCount: Int!
  }

  type IndexerStats {
    totalAgents: Int!
    totalFeedbacks: Int!
    totalValidations: Int!
    totalRegistries: Int!
    lastProcessedSignature: String
    lastProcessedSlot: BigInt
    updatedAt: DateTime
  }

  type IndexerStatus {
    running: Boolean!
    mode: String!
    pollerActive: Boolean!
    wsActive: Boolean!
  }

  type Query {
    # Single entity queries
    agent(id: ID!): Agent
    feedback(id: ID!): Feedback
    validation(id: ID!): Validation
    registry(id: ID!): Registry

    # List queries
    agents(
      owner: String
      collection: String
      registry: String
      limit: Int
      offset: Int
      orderBy: AgentOrderBy
    ): [Agent!]!

    feedbacks(
      agentId: ID
      client: String
      minScore: Int
      maxScore: Int
      tag: String
      revoked: Boolean
      limit: Int
      offset: Int
      orderBy: FeedbackOrderBy
    ): [Feedback!]!

    validations(
      agentId: ID
      validator: String
      requester: String
      pending: Boolean
      limit: Int
      offset: Int
    ): [Validation!]!

    registries(
      registryType: String
      authority: String
      limit: Int
      offset: Int
    ): [Registry!]!

    # Stats
    stats: IndexerStats!
    indexerStatus: IndexerStatus!

    # Search
    searchAgents(query: String!, limit: Int): [Agent!]!
  }

  enum AgentOrderBy {
    CREATED_AT_ASC
    CREATED_AT_DESC
    UPDATED_AT_ASC
    UPDATED_AT_DESC
  }

  enum FeedbackOrderBy {
    CREATED_AT_ASC
    CREATED_AT_DESC
    SCORE_ASC
    SCORE_DESC
  }
`;
