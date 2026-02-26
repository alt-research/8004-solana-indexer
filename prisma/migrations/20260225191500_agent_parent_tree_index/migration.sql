-- Improve parent/children tree traversal performance
CREATE INDEX IF NOT EXISTS "Agent_parentAsset_status_idx"
ON "Agent"("parentAsset", "status");
