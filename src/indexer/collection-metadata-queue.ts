import PQueue from "p-queue";
import { Pool } from "pg";
import { config } from "../config.js";
import { createChildLogger } from "../logger.js";
import { digestCollectionPointerDoc } from "./collectionDigest.js";

const logger = createChildLogger("collection-metadata-queue");

const CONCURRENCY = 5;
const INTERVAL = 100;
const TIMEOUT_MS = 30000;
const MAX_QUEUE_SIZE = 5000;

export interface CollectionMetadataTask {
  assetId: string;
  col: string;
  addedAt: number;
}

class CollectionMetadataQueue {
  private queue: PQueue;
  private pool: Pool | null = null;
  private pending = new Map<string, CollectionMetadataTask>();
  private statsInterval: NodeJS.Timeout | null = null;
  private stats = {
    queued: 0,
    processed: 0,
    skippedStale: 0,
    skippedDuplicate: 0,
    errors: 0,
  };

  constructor() {
    this.queue = new PQueue({
      concurrency: CONCURRENCY,
      interval: INTERVAL,
      intervalCap: CONCURRENCY,
      timeout: TIMEOUT_MS,
    });

    this.statsInterval = setInterval(() => this.logStats(), 60000);
  }

  setPool(pool: Pool): void {
    this.pool = pool;
  }

  add(assetId: string, col: string): void {
    if (!config.collectionMetadataIndexEnabled || !assetId || !col) {
      return;
    }

    const existing = this.pending.get(assetId);
    if (existing && existing.col === col) {
      this.stats.skippedDuplicate++;
      return;
    }

    if (this.queue.size + this.queue.pending >= MAX_QUEUE_SIZE) {
      logger.warn({ assetId, queueSize: this.queue.size }, "Collection metadata queue full, rejecting task");
      return;
    }

    const task: CollectionMetadataTask = {
      assetId,
      col,
      addedAt: Date.now(),
    };

    this.pending.set(assetId, task);
    this.stats.queued++;

    this.queue.add(() => this.processTask(task)).catch((err) => {
      this.stats.errors++;
      logger.error({ assetId, col, error: err.message }, "Collection metadata queue task failed");
    });
  }

  addBatch(tasks: Array<{ assetId: string; col: string }>): void {
    for (const task of tasks) {
      this.add(task.assetId, task.col);
    }
  }

  private async processTask(task: CollectionMetadataTask): Promise<void> {
    const { assetId, col } = task;
    if (!this.pool) return;

    try {
      const currentPending = this.pending.get(assetId);
      if (currentPending === task) {
        this.pending.delete(assetId);
      }

      const currentAgent = await this.pool.query<{
        canonical_col: string;
        creator: string | null;
        owner: string;
      }>(
        `SELECT canonical_col, creator, owner FROM agents WHERE asset = $1`,
        [assetId]
      );

      if (currentAgent.rows.length === 0) {
        this.stats.skippedStale++;
        return;
      }

      const row = currentAgent.rows[0];
      if (row.canonical_col !== col) {
        this.stats.skippedStale++;
        return;
      }

      const creator = row.creator || row.owner;
      const result = await digestCollectionPointerDoc(col);

      if (result.status !== "ok" || !result.fields) {
        await this.pool.query(
          `UPDATE collection_pointers
           SET metadata_status = $1,
               metadata_hash = $2,
               metadata_bytes = $3,
               metadata_updated_at = NOW()
           WHERE col = $4
             AND creator = $5`,
          [result.status, result.hash || null, result.bytes ?? null, col, creator]
        );
        this.stats.processed++;
        return;
      }

      await this.pool.query(
        `UPDATE collection_pointers
         SET version = $1,
             name = $2,
             symbol = $3,
             description = $4,
             image = $5,
             banner_image = $6,
             social_website = $7,
             social_x = $8,
             social_discord = $9,
             metadata_status = $10,
             metadata_hash = $11,
             metadata_bytes = $12,
             metadata_updated_at = NOW()
         WHERE col = $13
           AND creator = $14`,
        [
          result.fields.version,
          result.fields.name,
          result.fields.symbol,
          result.fields.description,
          result.fields.image,
          result.fields.bannerImage,
          result.fields.socialWebsite,
          result.fields.socialX,
          result.fields.socialDiscord,
          "ok",
          result.hash || null,
          result.bytes ?? null,
          col,
          creator,
        ]
      );

      this.stats.processed++;
      logger.info({ assetId, col, creator }, "Collection metadata extracted");
    } catch (error: any) {
      this.stats.errors++;
      logger.error({ assetId, col, error: error.message }, "Collection metadata extraction failed");
    }
  }

  getStats() {
    return {
      ...this.stats,
      queueSize: this.queue.size,
      pendingCount: this.pending.size,
    };
  }

  shutdown(): void {
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }
  }

  private logStats(): void {
    if (this.stats.queued === 0) return;
    logger.info(this.getStats(), "Collection metadata queue stats (60s)");
  }
}

export const collectionMetadataQueue = new CollectionMetadataQueue();
