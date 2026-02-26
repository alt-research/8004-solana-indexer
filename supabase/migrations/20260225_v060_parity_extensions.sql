-- v0.6.0 parity migration
-- - widen feedback value storage to i128-safe numeric
-- - allow value_decimals up to 18
-- - add identity extension fields on agents

-- i128-safe value storage
ALTER TABLE feedbacks
  ALTER COLUMN value TYPE NUMERIC(39,0) USING value::numeric;

ALTER TABLE feedbacks DROP CONSTRAINT IF EXISTS feedbacks_value_decimals_check;
ALTER TABLE feedbacks ADD CONSTRAINT feedbacks_value_decimals_check
  CHECK (value_decimals >= 0 AND value_decimals <= 18);

COMMENT ON COLUMN feedbacks.value IS 'Raw metric value as signed integer in i128 range (v0.6.0)';
COMMENT ON COLUMN feedbacks.value_decimals IS 'Decimal precision 0-18 for value interpretation (v0.6.0)';

-- identity extension fields
ALTER TABLE agents ADD COLUMN IF NOT EXISTS creator TEXT;
ALTER TABLE agents ADD COLUMN IF NOT EXISTS canonical_col TEXT DEFAULT '';
ALTER TABLE agents ADD COLUMN IF NOT EXISTS col_locked BOOLEAN DEFAULT FALSE;
ALTER TABLE agents ADD COLUMN IF NOT EXISTS parent_asset TEXT;
ALTER TABLE agents ADD COLUMN IF NOT EXISTS parent_creator TEXT;
ALTER TABLE agents ADD COLUMN IF NOT EXISTS parent_locked BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_agents_creator ON agents(creator);
CREATE INDEX IF NOT EXISTS idx_agents_parent_asset ON agents(parent_asset);

UPDATE agents
SET creator = owner
WHERE creator IS NULL;
