import type { GraphQLContext } from '../context.js';
import type { MetadataRow } from '../dataloaders.js';
import { encodeMetadataId } from '../utils/ids.js';
import { decompressFromStorage } from '../../../utils/compression.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

export const metadataResolvers = {
  AgentMetadata: {
    id(parent: MetadataRow) {
      return encodeMetadataId(parent.asset, parent.key);
    },
    async agent(parent: MetadataRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.agentById.load(parent.asset);
    },
    key(parent: MetadataRow) {
      return parent.key;
    },
    async value(parent: MetadataRow) {
      const bytes = parent.value instanceof Buffer
        ? parent.value
        : (parent.value instanceof Uint8Array ? Buffer.from(parent.value) : Buffer.from(String(parent.value)));
      try {
        const decompressed = await decompressFromStorage(bytes);
        return decompressed.toString('utf-8');
      } catch {
        return bytes.toString('base64');
      }
    },
    updatedAt(parent: MetadataRow) {
      return toUnixTimestamp(parent.updated_at);
    },
  },
};
