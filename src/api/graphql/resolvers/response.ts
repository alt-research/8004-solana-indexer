import type { GraphQLContext } from '../context.js';
import type { ResponseRow } from '../dataloaders.js';
import { encodeResponseId } from '../utils/ids.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

export const responseResolvers = {
  FeedbackResponse: {
    id(parent: ResponseRow) {
      return encodeResponseId(
        parent.asset,
        parent.client_address,
        parent.feedback_index,
        parent.responder,
        parent.tx_signature ?? ''
      );
    },
    async feedback(parent: ResponseRow, _args: unknown, ctx: GraphQLContext) {
      return ctx.loaders.feedbackByLookup.load(`${parent.asset}:${parent.client_address}:${parent.feedback_index}`);
    },
    responder(parent: ResponseRow) {
      return parent.responder;
    },
    responseUri(parent: ResponseRow) {
      return parent.response_uri;
    },
    responseHash(parent: ResponseRow) {
      return parent.response_hash;
    },
    createdAt(parent: ResponseRow) {
      return toUnixTimestamp(parent.created_at);
    },
    solana(parent: ResponseRow) {
      return parent;
    },
  },
};
