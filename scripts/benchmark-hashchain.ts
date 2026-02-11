import { performance } from 'node:perf_hooks';
import { PublicKey } from '@solana/web3.js';
import { ReplayVerifier } from '../src/services/replay-verifier.js';

type ChainType = 'feedback' | 'response' | 'revoke';

type Timing = {
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
  minMs: number;
  maxMs: number;
  avgEventsPerSec: number;
};

const EVENT_COUNT = Number.parseInt(process.env.HASHCHAIN_BENCH_EVENTS || '10000', 10);
const WARMUP = Number.parseInt(process.env.HASHCHAIN_BENCH_WARMUP || '2', 10);
const ITERATIONS = Number.parseInt(process.env.HASHCHAIN_BENCH_ITERATIONS || '8', 10);
const CHECKPOINT_INTERVAL = Number.parseInt(process.env.HASHCHAIN_BENCH_CHECKPOINT_INTERVAL || '1000', 10);

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.max(0, Math.min(sorted.length - 1, Math.floor(sorted.length * p)));
  return sorted[idx];
}

function computeTiming(valuesMs: number[], eventsPerRun: number): Timing {
  const sorted = [...valuesMs].sort((a, b) => a - b);
  const avgMs = sorted.reduce((acc, v) => acc + v, 0) / sorted.length;
  const avgEventsPerSec = eventsPerRun / (avgMs / 1000);
  return {
    avgMs,
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    avgEventsPerSec,
  };
}

function keyFromSeed(seed: number): string {
  return new PublicKey(new Uint8Array(32).fill(seed)).toBase58();
}

function sealHash(seed: number): Uint8Array {
  const buf = Buffer.alloc(32);
  buf.writeUInt32LE(seed >>> 0, 0);
  return new Uint8Array(buf);
}

function buildData(eventCount: number) {
  const agentId = keyFromSeed(1);
  const clientId = keyFromSeed(6);
  const responder = keyFromSeed(2);

  const feedbackRows = Array.from({ length: eventCount }, (_, i) => ({
    agentId,
    client: clientId,
    feedbackIndex: BigInt(i),
    feedbackHash: sealHash(i + 1),
    createdSlot: 100000n + BigInt(i),
    runningDigest: null,
    status: 'FINALIZED',
  }));

  const responseRows = Array.from({ length: eventCount }, (_, i) => ({
    responder,
    responseHash: sealHash(100000 + i + 1),
    responseCount: BigInt(i),
    slot: 200000n + BigInt(i),
    runningDigest: null,
    status: 'FINALIZED',
    feedback: {
      agentId,
      client: clientId,
      feedbackIndex: BigInt(i),
      feedbackHash: sealHash(i + 1),
    },
  }));

  const revokeRows = Array.from({ length: eventCount }, (_, i) => ({
    agentId,
    client: clientId,
    feedbackIndex: BigInt(i),
    feedbackHash: sealHash(i + 1),
    revokeCount: BigInt(i),
    slot: 300000n + BigInt(i),
    runningDigest: null,
    status: 'FINALIZED',
  }));

  return { agentId, feedbackRows, responseRows, revokeRows };
}

function createMockPrisma(eventCount: number) {
  const { agentId, feedbackRows, responseRows, revokeRows } = buildData(eventCount);
  let checkpointWrites = 0;

  const prisma = {
    feedback: {
      findMany: async (args: any) => {
        const gt: bigint = args?.where?.feedbackIndex?.gt ?? -1n;
        const take: number = args?.take ?? feedbackRows.length;
        return feedbackRows.filter((r) => r.feedbackIndex > gt).slice(0, take);
      },
    },
    feedbackResponse: {
      findMany: async (args: any) => {
        const gt: bigint = args?.where?.responseCount?.gt ?? -1n;
        const take: number = args?.take ?? responseRows.length;
        return responseRows.filter((r) => (r.responseCount ?? -1n) > gt).slice(0, take);
      },
    },
    revocation: {
      findMany: async (args: any) => {
        const gt: bigint = args?.where?.revokeCount?.gt ?? -1n;
        const take: number = args?.take ?? revokeRows.length;
        return revokeRows.filter((r) => r.revokeCount > gt).slice(0, take);
      },
    },
    hashChainCheckpoint: {
      findFirst: async () => null,
      upsert: async () => {
        checkpointWrites++;
      },
    },
  };

  return {
    agentId,
    prisma,
    getCheckpointWrites: () => checkpointWrites,
  };
}

async function runChainBench(chainType: ChainType): Promise<{ timing: Timing; finalDigest: string; checkpointsStored: number; checkpointWrites: number }> {
  const { agentId, prisma, getCheckpointWrites } = createMockPrisma(EVENT_COUNT);
  const verifier = new ReplayVerifier(prisma as any);
  verifier.CHECKPOINT_INTERVAL = CHECKPOINT_INTERVAL;

  const replay = (verifier as any).replayChainFromDB.bind(verifier) as (
    agentId: string,
    chainType: ChainType,
    startDigest: Buffer,
    startCount: number,
  ) => Promise<{ finalDigest: string; checkpointsStored: number }>;

  for (let i = 0; i < WARMUP; i++) {
    await replay(agentId, chainType, Buffer.alloc(32), 0);
  }

  const durations: number[] = [];
  let finalDigest = '';
  let checkpointsStored = 0;

  for (let i = 0; i < ITERATIONS; i++) {
    const t0 = performance.now();
    const result = await replay(agentId, chainType, Buffer.alloc(32), 0);
    durations.push(performance.now() - t0);
    finalDigest = result.finalDigest;
    checkpointsStored = result.checkpointsStored;
  }

  return {
    timing: computeTiming(durations, EVENT_COUNT),
    finalDigest,
    checkpointsStored,
    checkpointWrites: getCheckpointWrites(),
  };
}

function round2(value: number): number {
  return Number(value.toFixed(2));
}

async function main() {
  console.log(`HASHCHAIN_BENCH events=${EVENT_COUNT} warmup=${WARMUP} iterations=${ITERATIONS} checkpointInterval=${CHECKPOINT_INTERVAL}`);

  const results: Record<string, any> = {};
  for (const chainType of ['feedback', 'response', 'revoke'] as const) {
    const bench = await runChainBench(chainType);
    results[chainType] = {
      ...bench,
      timing: {
        avgMs: round2(bench.timing.avgMs),
        p50Ms: round2(bench.timing.p50Ms),
        p95Ms: round2(bench.timing.p95Ms),
        minMs: round2(bench.timing.minMs),
        maxMs: round2(bench.timing.maxMs),
        avgEventsPerSec: round2(bench.timing.avgEventsPerSec),
      },
      finalDigestPrefix: bench.finalDigest.slice(0, 16),
    };
    delete results[chainType].finalDigest;
  }

  console.log(JSON.stringify(results, null, 2));
}

main().catch((error) => {
  console.error('HASHCHAIN_BENCH_FAILED');
  console.error(error);
  process.exit(1);
});
