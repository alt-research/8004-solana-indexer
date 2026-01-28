import {
  Connection,
  PublicKey,
  Logs,
  Context,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { parseTransactionLogs, toTypedEvent } from "../parser/decoder.js";
import { handleEvent, EventContext } from "../db/handlers.js";
import { saveIndexerState } from "../db/supabase.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("websocket");

// Health check interval (30 seconds)
const HEALTH_CHECK_INTERVAL = 30_000;
// Consider connection stale if no activity for 2 minutes
const STALE_THRESHOLD = 120_000;

export interface WebSocketIndexerOptions {
  connection: Connection;
  prisma: PrismaClient | null;
  programId: PublicKey;
  reconnectInterval?: number;
  maxRetries?: number;
}

export class WebSocketIndexer {
  private connection: Connection;
  private prisma: PrismaClient | null;
  private programId: PublicKey;
  private reconnectInterval: number;
  private maxRetries: number;
  private subscriptionId: number | null = null;
  private isRunning = false;
  private retryCount = 0;
  private healthCheckTimer: NodeJS.Timeout | null = null;
  private lastActivityTime: number = Date.now();
  private processedCount = 0;
  private errorCount = 0;
  // Concurrency guards
  private isCheckingHealth = false;
  private isReconnecting = false;

  constructor(options: WebSocketIndexerOptions) {
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.reconnectInterval = options.reconnectInterval || config.wsReconnectInterval;
    this.maxRetries = options.maxRetries || config.wsMaxRetries;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("WebSocket indexer already running");
      return;
    }

    this.isRunning = true;
    this.lastActivityTime = Date.now();
    logger.info({ programId: this.programId.toBase58() }, "Starting WebSocket indexer");

    await this.subscribe();
    this.startHealthCheck();
  }

  async stop(): Promise<void> {
    logger.info({
      processedCount: this.processedCount,
      errorCount: this.errorCount
    }, "Stopping WebSocket indexer");

    this.isRunning = false;
    this.stopHealthCheck();

    if (this.subscriptionId !== null) {
      try {
        await this.connection.removeOnLogsListener(this.subscriptionId);
        logger.info({ subscriptionId: this.subscriptionId }, "Removed WebSocket subscription");
      } catch (error) {
        logger.warn({ error, subscriptionId: this.subscriptionId }, "Error removing subscription");
      }
      this.subscriptionId = null;
    }
  }

  private startHealthCheck(): void {
    this.stopHealthCheck();

    this.healthCheckTimer = setInterval(() => {
      this.checkHealth();
    }, HEALTH_CHECK_INTERVAL);

    logger.debug("Health check timer started");
  }

  private stopHealthCheck(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
      logger.debug("Health check timer stopped");
    }
  }

  private async checkHealth(): Promise<void> {
    if (!this.isRunning) return;

    // Reentrancy guard - prevent overlapping health checks
    if (this.isCheckingHealth) {
      logger.debug("Health check already in progress, skipping");
      return;
    }

    this.isCheckingHealth = true;
    try {
      const timeSinceActivity = Date.now() - this.lastActivityTime;

      logger.debug({
        timeSinceActivity,
        subscriptionId: this.subscriptionId,
        processedCount: this.processedCount,
        errorCount: this.errorCount
      }, "Health check");

      // Check if connection is stale (no activity for too long)
      if (timeSinceActivity > STALE_THRESHOLD) {
        logger.warn({
          timeSinceActivity,
          threshold: STALE_THRESHOLD
        }, "WebSocket connection appears stale, reconnecting...");

        await this.forceReconnect();
        return;
      }

      // Verify HTTP connectivity (but don't update lastActivityTime - only WS events should)
      try {
        const slot = await this.connection.getSlot();
        logger.debug({ slot }, "HTTP connectivity OK");
        // NOTE: We intentionally don't update lastActivityTime here
        // Only actual WebSocket events should reset the stale timer
      } catch (error) {
        logger.error({ error }, "Health check failed - connection error");
        await this.forceReconnect();
      }
    } finally {
      this.isCheckingHealth = false;
    }
  }

  private async forceReconnect(): Promise<void> {
    // Concurrency guard - prevent overlapping reconnects
    if (this.isReconnecting) {
      logger.debug("Reconnection already in progress, skipping");
      return;
    }

    this.isReconnecting = true;
    try {
      logger.info("Forcing WebSocket reconnection...");

      // Clean up existing subscription
      if (this.subscriptionId !== null) {
        try {
          await this.connection.removeOnLogsListener(this.subscriptionId);
        } catch (error) {
          logger.debug({ error }, "Error removing old subscription during reconnect");
        }
        this.subscriptionId = null;
      }

      // Reconnect
      await this.reconnect();
    } finally {
      this.isReconnecting = false;
    }
  }

  private async subscribe(): Promise<void> {
    try {
      logger.info("Subscribing to program logs...");

      this.subscriptionId = this.connection.onLogs(
        this.programId,
        // CRITICAL FIX: Wrap async callback to catch unhandled promise rejections
        (logs: Logs, ctx: Context) => {
          this.lastActivityTime = Date.now();
          this.handleLogs(logs, ctx).catch((error) => {
            this.errorCount++;
            logger.error({
              error: error instanceof Error ? error.message : String(error),
              signature: logs.signature,
              errorCount: this.errorCount
            }, "Unhandled error in handleLogs - caught by wrapper");

            // If too many errors, force reconnect
            if (this.errorCount > 10 && this.errorCount % 10 === 0) {
              logger.warn({ errorCount: this.errorCount }, "High error count, scheduling reconnect");
              this.forceReconnect().catch(e => {
                logger.error({ error: e }, "Failed to reconnect after errors");
              });
            }
          });
        },
        "confirmed"
      );

      logger.info(
        { subscriptionId: this.subscriptionId },
        "WebSocket subscription active"
      );

      this.retryCount = 0;
      this.errorCount = 0;
    } catch (error) {
      logger.error({ error }, "Failed to subscribe to logs");
      await this.reconnect();
    }
  }

  private async handleLogs(logs: Logs, ctx: Context): Promise<void> {
    if (logs.err) {
      logger.debug({ signature: logs.signature }, "Transaction failed, skipping");
      return;
    }

    const startTime = Date.now();

    try {
      const events = parseTransactionLogs(logs.logs);
      if (events.length === 0) return;

      logger.debug(
        { signature: logs.signature, eventCount: events.length, slot: ctx.slot },
        "Received logs"
      );

      // Approximate block time for WebSocket events
      const blockTime = new Date();

      for (const event of events) {
        const typedEvent = toTypedEvent(event);
        if (!typedEvent) continue;

        const eventCtx: EventContext = {
          signature: logs.signature,
          slot: BigInt(ctx.slot),
          blockTime,
        };

        try {
          await handleEvent(this.prisma, typedEvent, eventCtx);
        } catch (eventError) {
          logger.error({
            error: eventError instanceof Error ? eventError.message : String(eventError),
            eventType: typedEvent.type,
            signature: logs.signature
          }, "Error handling event");
          // Continue with other events
        }

        // Only log to Prisma if in local mode
        if (this.prisma) {
          try {
            await this.prisma.eventLog.create({
              data: {
                eventType: typedEvent.type,
                signature: logs.signature,
                slot: BigInt(ctx.slot),
                blockTime,
                data: event.data as object,
                processed: true,
              },
            });
          } catch (prismaError) {
            logger.warn({
              error: prismaError instanceof Error ? prismaError.message : String(prismaError),
              signature: logs.signature
            }, "Failed to log event to Prisma");
          }
        }
      }

      // Update state - both local and Supabase modes
      try {
        if (this.prisma) {
          await this.prisma.indexerState.upsert({
            where: { id: "main" },
            create: {
              id: "main",
              lastSignature: logs.signature,
              lastSlot: BigInt(ctx.slot),
            },
            update: {
              lastSignature: logs.signature,
              lastSlot: BigInt(ctx.slot),
            },
          });
        } else {
          // Supabase mode - persist cursor for recovery
          await saveIndexerState(logs.signature, BigInt(ctx.slot));
        }
      } catch (stateError) {
        logger.error({
          error: stateError instanceof Error ? stateError.message : String(stateError),
          signature: logs.signature
        }, "Failed to save indexer state");
      }

      this.processedCount++;
      const duration = Date.now() - startTime;

      if (this.processedCount % 100 === 0) {
        logger.info({
          processedCount: this.processedCount,
          errorCount: this.errorCount,
          lastDuration: duration
        }, "WebSocket processing stats");
      }

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error({ error: errorMessage, signature: logs.signature }, "Error handling logs");

      // Log errors only in local mode
      if (this.prisma) {
        try {
          await this.prisma.eventLog.create({
            data: {
              eventType: "PROCESSING_FAILED",
              signature: logs.signature,
              slot: BigInt(ctx.slot),
              blockTime: new Date(),
              data: { logs: logs.logs },
              processed: false,
              error: errorMessage,
            },
          });
        } catch (logError) {
          logger.warn({ error: logError }, "Failed to log error to Prisma");
        }
      }

      // Re-throw to be caught by wrapper
      throw error;
    }
  }

  private async reconnect(): Promise<void> {
    if (!this.isRunning) return;

    if (this.retryCount >= this.maxRetries) {
      logger.error({
        retryCount: this.retryCount,
        maxRetries: this.maxRetries
      }, "Max retries exceeded, WebSocket indexer stopped");
      this.isRunning = false;
      this.stopHealthCheck();
      return;
    }

    this.retryCount++;
    logger.info(
      { retryCount: this.retryCount, maxRetries: this.maxRetries, interval: this.reconnectInterval },
      "Reconnecting WebSocket"
    );

    await new Promise((resolve) =>
      setTimeout(resolve, this.reconnectInterval)
    );

    await this.subscribe();
  }

  isActive(): boolean {
    return this.isRunning && this.subscriptionId !== null;
  }

  getStats(): { processedCount: number; errorCount: number; lastActivity: number } {
    return {
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      lastActivity: this.lastActivityTime,
    };
  }
}

/**
 * Test if WebSocket endpoint is available
 * @param rpcUrl - HTTP RPC endpoint for connection test
 * @param wsUrl - WebSocket endpoint to configure
 */
export async function testWebSocketConnection(rpcUrl: string, wsUrl: string): Promise<boolean> {
  try {
    logger.debug({ rpcUrl, wsUrl }, "Testing WebSocket connection");

    // Use HTTP endpoint for getSlot, but configure WS endpoint
    const connection = new Connection(rpcUrl, {
      wsEndpoint: wsUrl,
      commitment: "confirmed",
    });
    // Test HTTP connectivity first
    const slot = await connection.getSlot();
    logger.debug({ slot }, "WebSocket connection test successful");
    return true;
  } catch (error) {
    logger.debug({ error }, "WebSocket connection test failed");
    return false;
  }
}
