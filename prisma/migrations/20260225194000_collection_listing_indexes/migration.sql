-- Improve creator+collection listing and count queries
CREATE INDEX IF NOT EXISTS "Agent_collectionPointer_status_idx"
ON "Agent"("collectionPointer", "status");

CREATE INDEX IF NOT EXISTS "Agent_collectionPointer_creator_createdAt_idx"
ON "Agent"("collectionPointer", "creator", "createdAt");
