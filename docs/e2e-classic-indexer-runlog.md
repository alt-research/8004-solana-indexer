# Classic Indexer E2E Run Log

- Date (UTC): 2026-02-25T19:28:51Z
- Repo: `/Users/true/Documents/Pipeline/CasterCorp/8004-solana-indexer`
- Raw logs directory: `.tmp/e2e-classic-run-20260225-202851`
- Scope: classic indexer normal mode, docker mode, env variants, localnet compatibility

## Tooling

- `node`: `v23.5.0`
- `npm`: `10.9.2`
- `bun`: `1.2.12`
- `docker`: `Docker version 28.5.0`
- `docker compose`: `5.0.2`
- `anchor`: `0.31.1`
- `solana`: `2.3.10`

## Command Outcomes

| # | Command | Exit | Outcome |
|---|---|---:|---|
| 1 | `npm run db:generate` + `DATABASE_URL=file:./data/localnet.db npm run db:push` | 0 | Prisma client generated and local SQLite schema pushed. |
| 2 | Static env validation (`.env.example`, `.env.localnet`) | 0 | Key/mode checks passed; `.env.localnet` and `.env.example` have expected key sets. |
| 3 | `npm run localnet:start` (standalone) | 0 | Validator started on `8899/8900`; registry program deployed (`8oo4...`). |
| 4 | `npm run localnet:init` (standalone) | 1 | Failed with `fetch failed` during `tests/init-localnet.ts` account check. |
| 5 | Normal REST smoke (`.env.localnet`, API `3101`) | 0 | `GET /health` returned `{"status":"ok"}`; `GET /rest/v1/stats` returned data. |
| 6 | Normal GraphQL smoke (local Postgres init + API `3102`) | 0 | `GET /health` ok; GraphQL `{"query":"{ __typename }"}` returned `Query`. |
| 7 | `.env.example` equivalent normal run (API `3103`) | 0 | Service reached healthy state; startup log still shows Supabase DNS resolution warning (`ENOTFOUND base`). |
| 8 | `npm run test:localnet` | 1 | Failed: `Initialize ATOM Engine` -> `Attempt to load a program that does not exist`. |
| 9 | `LOCALNET_CLONE_ATOM_PROGRAM=1 npm run test:localnet` | 2 | Failed: `Initialize Agent Registry` -> `Program is not deployed` / `Unsupported program id`. |
| 10 | `npm run test:docker:ci` | 1 | Docker test container ran Vitest but reported `No test files found`. |
| 11 | `docker build ... -t 8004-indexer-classic:local .` | 0 | Local classic image built successfully. |
| 12 | Docker REST matrix attempt (`DATABASE_URL=file:./data/localnet-docker.db`, API `3201`) | 1 | Failed: Prisma `P2021` missing table `main.OrphanResponse` (fresh DB file lacked schema). |
| 13 | Docker REST matrix retry (`DATABASE_URL=file:./data/indexer.db`, API `3201`) | 0 | Healthy; `GET /rest/v1/stats` returned zeroed counters. |
| 14 | Docker GraphQL matrix (`postgres:16-alpine` on `55433`, API `3202`) | 0 | Healthy; GraphQL `__typename` query returned `Query`. |
| 15 | Localnet compatibility check (env vs `8004-solana/Anchor.toml`) | 0 | Program ID + RPC/WS ports matched CasterCorp localnet config. |

## Ports Used

- Local validator RPC: `8899`
- Local validator WS: `8900`
- Normal REST smoke API: `3101`
- Normal GraphQL smoke API: `3102`
- `.env.example` validation API: `3103`
- Docker REST matrix API: `3201`
- Docker GraphQL matrix API: `3202`
- Temporary Postgres for normal GraphQL smoke: `55432`
- Temporary Postgres for Docker GraphQL matrix: `55433`

## Localnet Compatibility (CasterCorp)

Compared:
- Indexer: `.env.localnet`
- Program repo: `/Users/true/Documents/Pipeline/CasterCorp/8004-solana/Anchor.toml`

Results:
- `PROGRAM_ID` match: `8oo4J9tBB3Hna1jRQ3rWvJjojqM5DYTDJo5cejUuJy3C`
- RPC port match: `.env.localnet` uses `http://localhost:8899`, Anchor localnet uses `rpc_port = 8899`
- WS port match: `.env.localnet` uses `ws://localhost:8900`
- ATOM note: `scripts/localnet-start.sh` only clones ATOM when `LOCALNET_CLONE_ATOM_PROGRAM=1`; default is `0`.

## Failure Details

### A) `npm run test:localnet` failed

- Failure: `Initialize ATOM Engine (if needed)`
- Error: `Transaction simulation failed: Attempt to load a program that does not exist.`
- Context: Registry init succeeded first; ATOM init failed.

### B) `LOCALNET_CLONE_ATOM_PROGRAM=1 npm run test:localnet` failed

- Failure: `Initialize Agent Registry (if needed)`
- Error logs include:
  - `Program is not deployed`
  - `Unsupported program id`
- Context: ATOM init succeeded in this run, but registry init failed and root config fetch failed afterward.

### C) `npm run test:docker:ci` failed

- Vitest output: `No test files found, exiting with code 1`
- Filter passed into container:
  - `tests/unit/config.test.ts`
  - `tests/unit/parser/types.test.ts`
  - `tests/unit/parser/decoder.test.ts`
- Likely driver from image context: `.dockerignore` currently excludes `tests`, `scripts`, and `docker`.

### D) First Docker REST matrix attempt failed

- Error: Prisma `P2021` (`table main.OrphanResponse does not exist`)
- Cause: using a new SQLite path `file:./data/localnet-docker.db` without schema push.
- Retry using `file:./data/indexer.db` passed.

## Important Log Snippets

- REST smoke (`14-rest-localnet-smoke.log`):
  - `{"status":"ok"}`
  - `[{"total_agents":32,"total_feedbacks":123,"total_collections":1,"total_validations":0}]`

- GraphQL smoke (`15-graphql-smoke.log`):
  - `{"status":"ok"}`
  - `{"data":{"__typename":"Query"}}`

- `.env.example` equivalent run (`16-env-example-indexer.out`):
  - `Failed to load indexer state, using deployment fallback`
  - `error: "getaddrinfo ENOTFOUND base"`

- Docker REST retry (`34-docker-matrix-rest-localnet-retry.log`):
  - `{"status":"ok"}`
  - `[{"total_agents":0,"total_feedbacks":0,"total_collections":0,"total_validations":0}]`

- Docker GraphQL matrix (`35-docker-matrix-graphql.log`):
  - `{"status":"ok"}`
  - `{"data":{"__typename":"Query"}}`

## Notes

- A disk-space incident occurred during execution (`no space left on device`); resolved by Docker prune before continuing.
- All raw command transcripts are preserved under `.tmp/e2e-classic-run-20260225-202851`.
