/*
  Warnings:

  - You are about to drop the column `baseIndex` on the `Registry` table. All the data in the column will be lost.
  - You are about to alter the column `nonce` on the `Validation` table. The data in that column could be lost. The data in that column will be cast from `Int` to `BigInt`.

*/
-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_Registry" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "collection" TEXT NOT NULL,
    "registryType" TEXT NOT NULL,
    "authority" TEXT NOT NULL,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "txSignature" TEXT,
    "slot" BIGINT
);
INSERT INTO "new_Registry" ("authority", "collection", "createdAt", "id", "registryType", "slot", "txSignature") SELECT "authority", "collection", "createdAt", "id", "registryType", "slot", "txSignature" FROM "Registry";
DROP TABLE "Registry";
ALTER TABLE "new_Registry" RENAME TO "Registry";
CREATE UNIQUE INDEX "Registry_collection_key" ON "Registry"("collection");
CREATE INDEX "Registry_registryType_idx" ON "Registry"("registryType");
CREATE INDEX "Registry_authority_idx" ON "Registry"("authority");
CREATE TABLE "new_Validation" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "agentId" TEXT NOT NULL,
    "validator" TEXT NOT NULL,
    "requester" TEXT NOT NULL,
    "nonce" BIGINT NOT NULL,
    "requestUri" TEXT,
    "requestHash" BLOB,
    "response" INTEGER,
    "responseUri" TEXT,
    "responseHash" BLOB,
    "tag" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "respondedAt" DATETIME,
    "requestTxSignature" TEXT,
    "requestSlot" BIGINT,
    "responseTxSignature" TEXT,
    "responseSlot" BIGINT,
    CONSTRAINT "Validation_agentId_fkey" FOREIGN KEY ("agentId") REFERENCES "Agent" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "new_Validation" ("agentId", "createdAt", "id", "nonce", "requestHash", "requestSlot", "requestTxSignature", "requestUri", "requester", "respondedAt", "response", "responseHash", "responseSlot", "responseTxSignature", "responseUri", "tag", "validator") SELECT "agentId", "createdAt", "id", "nonce", "requestHash", "requestSlot", "requestTxSignature", "requestUri", "requester", "respondedAt", "response", "responseHash", "responseSlot", "responseTxSignature", "responseUri", "tag", "validator" FROM "Validation";
DROP TABLE "Validation";
ALTER TABLE "new_Validation" RENAME TO "Validation";
CREATE INDEX "Validation_agentId_idx" ON "Validation"("agentId");
CREATE INDEX "Validation_validator_idx" ON "Validation"("validator");
CREATE INDEX "Validation_requester_idx" ON "Validation"("requester");
CREATE UNIQUE INDEX "Validation_agentId_validator_nonce_key" ON "Validation"("agentId", "validator", "nonce");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;
