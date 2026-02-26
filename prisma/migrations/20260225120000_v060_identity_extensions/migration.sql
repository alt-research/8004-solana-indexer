-- v0.6.0 identity extension fields for Agent
ALTER TABLE "Agent" ADD COLUMN "creator" TEXT;
ALTER TABLE "Agent" ADD COLUMN "collectionPointer" TEXT NOT NULL DEFAULT '';
ALTER TABLE "Agent" ADD COLUMN "colLocked" BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE "Agent" ADD COLUMN "parentAsset" TEXT;
ALTER TABLE "Agent" ADD COLUMN "parentCreator" TEXT;
ALTER TABLE "Agent" ADD COLUMN "parentLocked" BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS "Agent_creator_idx" ON "Agent"("creator");
CREATE INDEX IF NOT EXISTS "Agent_parentAsset_idx" ON "Agent"("parentAsset");

UPDATE "Agent"
SET "creator" = "owner"
WHERE "creator" IS NULL;
