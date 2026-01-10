import { BorshCoder, EventParser, Idl } from "@coral-xyz/anchor";
import { PublicKey, ParsedTransactionWithMeta } from "@solana/web3.js";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { ProgramEvent } from "./types.js";
import { createChildLogger } from "../logger.js";
import { config } from "../config.js";

const logger = createChildLogger("decoder");

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load IDL
const idlPath = join(__dirname, "../../idl/agent_registry_8004.json");
const idl: Idl = JSON.parse(readFileSync(idlPath, "utf-8"));

// Create coder and event parser
const coder = new BorshCoder(idl);
const eventParser = new EventParser(new PublicKey(config.programId), coder);

export interface ParsedEvent {
  name: string;
  data: Record<string, unknown>;
}

export interface TransactionEvents {
  signature: string;
  slot: number;
  blockTime: number | null;
  events: ParsedEvent[];
}

/**
 * Parse events from a transaction's logs
 */
export function parseTransactionLogs(logs: string[]): ParsedEvent[] {
  const events: ParsedEvent[] = [];

  try {
    const generator = eventParser.parseLogs(logs);
    for (const event of generator) {
      events.push({
        name: event.name,
        data: event.data as Record<string, unknown>,
      });
    }
  } catch (error) {
    logger.debug({ error }, "Failed to parse some logs");
  }

  return events;
}

/**
 * Parse events from a parsed transaction
 */
export function parseTransaction(
  tx: ParsedTransactionWithMeta
): TransactionEvents | null {
  if (!tx.meta?.logMessages) {
    return null;
  }

  const events = parseTransactionLogs(tx.meta.logMessages);

  if (events.length === 0) {
    return null;
  }

  return {
    signature: tx.transaction.signatures[0],
    slot: tx.slot,
    blockTime: tx.blockTime ?? null,
    events,
  };
}

/**
 * Convert raw event data to typed event
 */
export function toTypedEvent(event: ParsedEvent): ProgramEvent | null {
  const { name, data } = event;

  try {
    switch (name) {
      case "AgentRegisteredInRegistry":
        return {
          type: "AgentRegisteredInRegistry",
          data: {
            asset: new PublicKey(data.asset as string),
            registry: new PublicKey(data.registry as string),
            collection: new PublicKey(data.collection as string),
            owner: new PublicKey(data.owner as string),
          },
        };

      case "AgentOwnerSynced":
        return {
          type: "AgentOwnerSynced",
          data: {
            asset: new PublicKey(data.asset as string),
            oldOwner: new PublicKey(data.oldOwner as string),
            newOwner: new PublicKey(data.newOwner as string),
          },
        };

      case "UriUpdated":
        return {
          type: "UriUpdated",
          data: {
            asset: new PublicKey(data.asset as string),
            newUri: data.newUri as string,
            updatedBy: new PublicKey(data.updatedBy as string),
          },
        };

      case "WalletUpdated":
        return {
          type: "WalletUpdated",
          data: {
            asset: new PublicKey(data.asset as string),
            oldWallet: data.oldWallet
              ? new PublicKey(data.oldWallet as string)
              : null,
            newWallet: new PublicKey(data.newWallet as string),
            updatedBy: new PublicKey(data.updatedBy as string),
          },
        };

      case "MetadataSet":
        return {
          type: "MetadataSet",
          data: {
            asset: new PublicKey(data.asset as string),
            key: data.key as string,
            value: new Uint8Array(data.value as number[]),
            immutable: data.immutable as boolean,
          },
        };

      case "MetadataDeleted":
        return {
          type: "MetadataDeleted",
          data: {
            asset: new PublicKey(data.asset as string),
            key: data.key as string,
          },
        };

      case "BaseRegistryCreated":
        return {
          type: "BaseRegistryCreated",
          data: {
            registry: new PublicKey(data.registry as string),
            collection: new PublicKey(data.collection as string),
            baseIndex: data.baseIndex as number,
            createdBy: new PublicKey(data.createdBy as string),
          },
        };

      case "UserRegistryCreated":
        return {
          type: "UserRegistryCreated",
          data: {
            registry: new PublicKey(data.registry as string),
            collection: new PublicKey(data.collection as string),
            owner: new PublicKey(data.owner as string),
          },
        };

      case "BaseRegistryRotated":
        return {
          type: "BaseRegistryRotated",
          data: {
            oldRegistry: new PublicKey(data.oldRegistry as string),
            newRegistry: new PublicKey(data.newRegistry as string),
            rotatedBy: new PublicKey(data.rotatedBy as string),
          },
        };

      case "NewFeedback":
        return {
          type: "NewFeedback",
          data: {
            asset: new PublicKey(data.asset as string),
            clientAddress: new PublicKey(data.clientAddress as string),
            feedbackIndex: BigInt(data.feedbackIndex as string),
            score: data.score as number,
            tag1: data.tag1 as string,
            tag2: data.tag2 as string,
            endpoint: data.endpoint as string,
            feedbackUri: data.feedbackUri as string,
            feedbackHash: new Uint8Array(data.feedbackHash as number[]),
          },
        };

      case "FeedbackRevoked":
        return {
          type: "FeedbackRevoked",
          data: {
            asset: new PublicKey(data.asset as string),
            clientAddress: new PublicKey(data.clientAddress as string),
            feedbackIndex: BigInt(data.feedbackIndex as string),
          },
        };

      case "ResponseAppended":
        return {
          type: "ResponseAppended",
          data: {
            asset: new PublicKey(data.asset as string),
            feedbackIndex: BigInt(data.feedbackIndex as string),
            responder: new PublicKey(data.responder as string),
            responseUri: data.responseUri as string,
            responseHash: new Uint8Array(data.responseHash as number[]),
          },
        };

      case "ValidationRequested":
        return {
          type: "ValidationRequested",
          data: {
            asset: new PublicKey(data.asset as string),
            validatorAddress: new PublicKey(data.validatorAddress as string),
            nonce: data.nonce as number,
            requestUri: data.requestUri as string,
            requestHash: new Uint8Array(data.requestHash as number[]),
            requester: new PublicKey(data.requester as string),
          },
        };

      case "ValidationResponded":
        return {
          type: "ValidationResponded",
          data: {
            asset: new PublicKey(data.asset as string),
            validatorAddress: new PublicKey(data.validatorAddress as string),
            nonce: data.nonce as number,
            response: data.response as number,
            responseUri: data.responseUri as string,
            responseHash: new Uint8Array(data.responseHash as number[]),
            tag: data.tag as string,
          },
        };

      default:
        logger.warn({ eventName: name }, "Unknown event type");
        return null;
    }
  } catch (error) {
    logger.error({ error, event }, "Failed to convert event to typed event");
    return null;
  }
}

export { idl, coder, eventParser };
