import {
  Connection,
  PublicKey,
  ConfirmedSignatureInfo,
} from "@solana/web3.js";
import { PrismaClient } from "@prisma/client";
import { config } from "../config.js";
import { parseTransaction, toTypedEvent } from "../parser/decoder.js";
import { handleEvent, EventContext } from "../db/handlers.js";
import { createChildLogger } from "../logger.js";

const logger = createChildLogger("poller");

export interface PollerOptions {
  connection: Connection;
  prisma: PrismaClient;
  programId: PublicKey;
  pollingInterval?: number;
  batchSize?: number;
}

export class Poller {
  private connection: Connection;
  private prisma: PrismaClient;
  private programId: PublicKey;
  private pollingInterval: number;
  private batchSize: number;
  private isRunning = false;
  private lastSignature: string | null = null;

  constructor(options: PollerOptions) {
    this.connection = options.connection;
    this.prisma = options.prisma;
    this.programId = options.programId;
    this.pollingInterval = options.pollingInterval || config.pollingInterval;
    this.batchSize = options.batchSize || config.batchSize;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn("Poller already running");
      return;
    }

    this.isRunning = true;
    logger.info({ programId: this.programId.toBase58() }, "Starting poller");

    // Load last signature from DB
    await this.loadState();

    // Start polling loop
    this.poll();
  }

  async stop(): Promise<void> {
    logger.info("Stopping poller");
    this.isRunning = false;
  }

  private async loadState(): Promise<void> {
    const state = await this.prisma.indexerState.findUnique({
      where: { id: "main" },
    });

    if (state?.lastSignature) {
      this.lastSignature = state.lastSignature;
      logger.info({ lastSignature: this.lastSignature }, "Resuming from signature");
    } else {
      logger.info("Starting from latest transactions");
    }
  }

  private async saveState(signature: string, slot: bigint): Promise<void> {
    await this.prisma.indexerState.upsert({
      where: { id: "main" },
      create: {
        id: "main",
        lastSignature: signature,
        lastSlot: slot,
      },
      update: {
        lastSignature: signature,
        lastSlot: slot,
      },
    });
  }

  private async poll(): Promise<void> {
    while (this.isRunning) {
      try {
        await this.processNewTransactions();
      } catch (error) {
        logger.error({ error }, "Error in polling loop");
      }

      // Wait before next poll
      await new Promise((resolve) =>
        setTimeout(resolve, this.pollingInterval)
      );
    }
  }

  private async processNewTransactions(): Promise<void> {
    // Fetch new signatures
    const signatures = await this.fetchSignatures();

    if (signatures.length === 0) {
      logger.debug("No new transactions");
      return;
    }

    logger.info({ count: signatures.length }, "Processing transactions");

    // Process in reverse order (oldest first)
    for (const sig of signatures.reverse()) {
      try {
        await this.processTransaction(sig);
        this.lastSignature = sig.signature;
        await this.saveState(sig.signature, BigInt(sig.slot));
      } catch (error) {
        logger.error(
          { error, signature: sig.signature },
          "Error processing transaction"
        );
        // Log to event log for retry
        await this.logFailedTransaction(sig, error);
      }
    }
  }

  private async fetchSignatures(): Promise<ConfirmedSignatureInfo[]> {
    const options: {
      limit: number;
      before?: string;
      until?: string;
    } = {
      limit: this.batchSize,
    };

    // If we have a last signature, only fetch newer ones
    if (this.lastSignature) {
      options.until = this.lastSignature;
    }

    const signatures = await this.connection.getSignaturesForAddress(
      this.programId,
      options
    );

    // Filter out failed transactions
    return signatures.filter((sig) => sig.err === null);
  }

  private async processTransaction(sig: ConfirmedSignatureInfo): Promise<void> {
    // Fetch full transaction
    const tx = await this.connection.getParsedTransaction(sig.signature, {
      maxSupportedTransactionVersion: 0,
    });

    if (!tx) {
      logger.warn({ signature: sig.signature }, "Transaction not found");
      return;
    }

    // Parse events
    const parsed = parseTransaction(tx);
    if (!parsed || parsed.events.length === 0) {
      return;
    }

    logger.debug(
      { signature: sig.signature, eventCount: parsed.events.length },
      "Parsed transaction"
    );

    // Process each event
    for (const event of parsed.events) {
      const typedEvent = toTypedEvent(event);
      if (!typedEvent) continue;

      const ctx: EventContext = {
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: sig.blockTime
          ? new Date(sig.blockTime * 1000)
          : new Date(),
      };

      await handleEvent(this.prisma, typedEvent, ctx);

      // Log event for debugging/replay
      await this.prisma.eventLog.create({
        data: {
          eventType: typedEvent.type,
          signature: sig.signature,
          slot: BigInt(sig.slot),
          blockTime: ctx.blockTime,
          data: event.data as object,
          processed: true,
        },
      });
    }
  }

  private async logFailedTransaction(
    sig: ConfirmedSignatureInfo,
    error: unknown
  ): Promise<void> {
    await this.prisma.eventLog.create({
      data: {
        eventType: "PROCESSING_FAILED",
        signature: sig.signature,
        slot: BigInt(sig.slot),
        blockTime: sig.blockTime
          ? new Date(sig.blockTime * 1000)
          : new Date(),
        data: {},
        processed: false,
        error: error instanceof Error ? error.message : String(error),
      },
    });
  }
}
