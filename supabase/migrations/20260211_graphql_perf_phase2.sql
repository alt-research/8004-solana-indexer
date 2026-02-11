-- Additional GraphQL performance optimizations:
-- 1) Fast ILIKE search for agentSearch
-- 2) Faster registration listing scans on metadata _uri keys

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_agents_graphql_nft_name_trgm
ON agents USING gin (nft_name gin_trgm_ops)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_agents_graphql_asset_trgm
ON agents USING gin (asset gin_trgm_ops)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_agents_graphql_owner_trgm
ON agents USING gin (owner gin_trgm_ops)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_metadata_graphql_uri_asset_updated
ON metadata(asset, updated_at DESC)
WHERE status != 'ORPHANED' AND key LIKE '\_uri:%' ESCAPE '\';
