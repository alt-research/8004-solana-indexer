# 8004 Solana Indexer

Solana indexer for the 8004 Agent Registry with GraphQL v2 and transitional REST v1.

## Features

- WebSocket + polling ingestion modes
- Reorg resilience with verification worker
- GraphQL API (`/v2/graphql`) with query depth/complexity guards
- Legacy REST v1 (`/rest/v1/*`) available when `API_MODE=rest|both`
- Supabase/PostgreSQL data backend support
- Strict events-only integrity policy:
  - revoke/response without parent feedback => `ORPHANED`
  - revoke/response `seal_hash` mismatch => `ORPHANED`
  - GraphQL filters exclude `ORPHANED` records by default

## Quick Start

```bash
npm install
cp .env.example .env
npm run db:generate
npm run db:push
npm run dev
```

GraphQL v2 endpoint:

```text
http://localhost:3001/v2/graphql
```

## Remote API Docs

Canonical API docs are maintained in:

- https://github.com/QuantuLabs/8004-solana-api
- https://github.com/QuantuLabs/8004-solana-api/blob/main/docs/rest-v1.md
- https://github.com/QuantuLabs/8004-solana-api/blob/main/docs/collections.md
- https://github.com/QuantuLabs/8004-solana-api/tree/main/docs/examples

## Required Environment

```bash
DB_MODE=supabase
SUPABASE_DSN=POSTGRES_DSN_REDACTED
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_KEY=<service_role_key>
RPC_URL=https://api.devnet.solana.com
WS_URL=wss://api.devnet.solana.com
```

Notes:

- `API_MODE=both` is default.
- GraphQL requires `DB_MODE=supabase`.
- REST v1 requires `DB_MODE=local` (Prisma).
- `GRAPHQL_STATS_CACHE_TTL_MS` controls `globalStats`/`protocol` aggregate cache TTL (default `60000` ms).

## Commands

```bash
npm run dev
npm run build
npm test
npm run test:e2e
npm run check:graphql:coherence
npm run bench:graphql:sql
npm run bench:hashchain
```

## Docker

Build local image:

```bash
docker build \
  --build-arg INDEXER_VERSION=$(node -p "require('./package.json').version") \
  -t 8004-indexer-classic:local .
```

Run local smoke container:

```bash
docker run --rm -p 3001:3001 --env-file .env 8004-indexer-classic:local
```

CI test target in Docker:

```bash
docker compose -f docker/ci/ci-classic.yml up --build --abort-on-container-exit
```

Runtime stack (digest-pin friendly):

```bash
docker compose -f docker/stack/classic-stack.yml up -d
```

Integrity helpers:

```bash
scripts/docker/record-digest.sh ghcr.io/quantulabs/8004-indexer-classic v1.6.0 docker/digests.yml
scripts/docker/verify-image-integrity.sh ghcr.io/quantulabs/8004-indexer-classic v1.6.0
```

## GraphQL Example

```graphql
query Dashboard {
  stats {
    totalAgents
    totalFeedback
    totalValidations
  }
  agents(first: 10, orderBy: createdAt, orderDirection: desc) {
    id
    asset
    owner
    feedbackCount
    trustScore
  }
}
```

## E2E Scope

`npm run test:e2e` runs the maintained major end-to-end suites:

- `tests/e2e/reorg-resilience.test.ts`
- `tests/e2e/devnet-verification.test.ts`

## Project Structure

```text
src/
├── api/        # GraphQL server + resolvers
├── db/         # Database handlers
├── indexer/    # Poller, websocket, processor, verifier
├── parser/     # Program event decoder
├── config.ts   # Runtime config
└── index.ts    # Entry point
```
