-- =============================================
-- Fix tx_index ordering in feedback indexes
-- Migration: 2026-02-26
-- =============================================
-- Previous indexes used COALESCE(tx_index, 0) which incorrectly sorts
-- NULL tx_index as position 0 (first) instead of last.
-- PostgreSQL expression indexes don't support NULLS LAST directly,
-- so we drop the COALESCE-based indexes and recreate with proper columns.
-- The query planner will use these for ORDER BY ... tx_index NULLS LAST.

-- Fix feedback deterministic ordering index
DROP INDEX IF EXISTS idx_feedbacks_deterministic_order;
CREATE INDEX idx_feedbacks_deterministic_order
ON feedbacks(asset, client_address, block_slot, tx_index NULLS LAST, tx_signature);

-- Fix feedback global ordering index
DROP INDEX IF EXISTS idx_feedbacks_global_order;
CREATE INDEX idx_feedbacks_global_order
ON feedbacks(asset, block_slot, tx_index NULLS LAST, tx_signature);
