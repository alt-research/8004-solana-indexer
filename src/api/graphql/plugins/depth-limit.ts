import { type DocumentNode, visit } from 'graphql';

const MAX_DEPTH = 5;

export function calculateDepth(document: DocumentNode): number {
  let maxDepth = 0;
  let currentDepth = 0;

  visit(document, {
    Field: {
      enter() {
        currentDepth++;
        if (currentDepth > maxDepth) maxDepth = currentDepth;
      },
      leave() {
        currentDepth--;
      },
    },
    InlineFragment: {
      enter() {
        currentDepth++;
        if (currentDepth > maxDepth) maxDepth = currentDepth;
      },
      leave() {
        currentDepth--;
      },
    },
  });

  return maxDepth;
}

export interface DepthResult {
  allowed: boolean;
  depth: number;
  maxDepth: number;
  reason?: string;
}

export function analyzeDepth(document: DocumentNode): DepthResult {
  const depth = calculateDepth(document);
  if (depth > MAX_DEPTH) {
    return {
      allowed: false,
      depth,
      maxDepth: MAX_DEPTH,
      reason: `Query depth ${depth} exceeds maximum ${MAX_DEPTH}`,
    };
  }
  return { allowed: true, depth, maxDepth: MAX_DEPTH };
}

export { MAX_DEPTH };
