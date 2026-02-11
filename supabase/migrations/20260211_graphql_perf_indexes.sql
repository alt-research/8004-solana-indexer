-- GraphQL query performance indexes
-- Added to support cursor pagination and common GraphQL order/filter patterns.

CREATE INDEX IF NOT EXISTS idx_agents_graphql_created_asset
ON agents(created_at DESC, asset DESC)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_feedbacks_graphql_created_asset
ON feedbacks(created_at DESC, asset DESC)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_feedbacks_graphql_value_asset
ON feedbacks(value DESC, asset DESC)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_responses_graphql_created_id
ON feedback_responses(created_at DESC, id DESC)
WHERE status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_validations_graphql_created_id
ON validations(created_at DESC, id DESC)
WHERE chain_status != 'ORPHANED';

CREATE INDEX IF NOT EXISTS idx_metadata_graphql_asset_key
ON metadata(asset, key)
WHERE status != 'ORPHANED';
