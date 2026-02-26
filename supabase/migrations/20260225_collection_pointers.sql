-- Canonical collection pointer registry (c1:<cid>)

CREATE TABLE IF NOT EXISTS collection_pointers (
  col TEXT NOT NULL,
  creator TEXT NOT NULL,
  first_seen_asset TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  first_seen_slot BIGINT NOT NULL,
  first_seen_tx_signature TEXT,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_slot BIGINT NOT NULL,
  last_seen_tx_signature TEXT,
  asset_count BIGINT NOT NULL DEFAULT 0 CHECK (asset_count >= 0),
  PRIMARY KEY (col, creator)
);

CREATE INDEX IF NOT EXISTS idx_collection_pointers_creator
  ON collection_pointers(creator);
CREATE INDEX IF NOT EXISTS idx_collection_pointers_first_seen_at
  ON collection_pointers(first_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_collection_pointers_last_seen_at
  ON collection_pointers(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_agents_canonical_col
  ON agents(canonical_col)
  WHERE canonical_col <> '';
CREATE INDEX IF NOT EXISTS idx_agents_col_creator_active
  ON agents(canonical_col, creator, created_at DESC, asset DESC)
  WHERE status != 'ORPHANED' AND canonical_col <> '';
CREATE INDEX IF NOT EXISTS idx_agents_parent_active
  ON agents(parent_asset, created_at DESC, asset DESC)
  WHERE status != 'ORPHANED' AND parent_asset IS NOT NULL;

-- Best-effort backfill from current agent snapshots.
WITH ranked AS (
  SELECT
    a.canonical_col,
    COALESCE(a.creator, a.owner) AS creator,
    a.asset,
    a.updated_at,
    COALESCE(a.block_slot, 0) AS block_slot,
    a.tx_signature,
    ROW_NUMBER() OVER (
      PARTITION BY a.canonical_col, COALESCE(a.creator, a.owner)
      ORDER BY a.updated_at ASC, COALESCE(a.block_slot, 0) ASC, a.asset ASC
    ) AS rn
  FROM agents a
  WHERE a.canonical_col <> ''
),
agg AS (
  SELECT
    a.canonical_col,
    COALESCE(a.creator, a.owner) AS creator,
    COUNT(*)::bigint AS asset_count,
    MAX(a.updated_at) AS last_seen_at,
    MAX(COALESCE(a.block_slot, 0))::bigint AS last_seen_slot
  FROM agents a
  WHERE a.canonical_col <> ''
  GROUP BY a.canonical_col, COALESCE(a.creator, a.owner)
)
INSERT INTO collection_pointers (
  col,
  creator,
  first_seen_asset,
  first_seen_at,
  first_seen_slot,
  first_seen_tx_signature,
  last_seen_at,
  last_seen_slot,
  last_seen_tx_signature,
  asset_count
)
SELECT
  r.canonical_col,
  r.creator,
  r.asset,
  r.updated_at,
  r.block_slot,
  r.tx_signature,
  a.last_seen_at,
  a.last_seen_slot,
  r.tx_signature,
  a.asset_count
FROM ranked r
JOIN agg a
  ON a.canonical_col = r.canonical_col
 AND a.creator = r.creator
WHERE r.rn = 1
ON CONFLICT (col, creator) DO NOTHING;

UPDATE collection_pointers cp
SET asset_count = sub.asset_count
FROM (
  SELECT
    canonical_col AS col,
    COALESCE(creator, owner) AS creator,
    COUNT(*)::bigint AS asset_count
  FROM agents
  WHERE canonical_col <> ''
  GROUP BY canonical_col, COALESCE(creator, owner)
) sub
WHERE cp.col = sub.col
  AND cp.creator = sub.creator;
