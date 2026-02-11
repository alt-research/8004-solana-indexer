import { performance } from "node:perf_hooks";
import { PublicKey } from "@solana/web3.js";
import { ReplayVerifier } from "../src/services/replay-verifier.js";

type ScenarioName = "feedback-only" | "response-only" | "revoke-only" | "combined";

type Scenario = {
  name: ScenarioName;
  counts: { feedback: number; response: number; revoke: number };
};

type ScenarioResult = {
  scale: number;
  scenario: ScenarioName;
  counts: { feedback: number; response: number; revoke: number };
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
  minMs: number;
  maxMs: number;
  eventsPerSecond: number;
  checkpointWrites: number;
  rssDeltaMB: number;
  rssPeakMB: number;
};

const DEFAULT_EVENTS = 10000;
const DEFAULT_ITERATIONS = 3;
const DEFAULT_WARMUP = 1;
const DEFAULT_CHECKPOINT_INTERVAL = 1000;

const SCENARIO_BY_NAME: Record<ScenarioName, Scenario> = {
  "feedback-only": { name: "feedback-only", counts: { feedback: 0, response: 0, revoke: 0 } },
  "response-only": { name: "response-only", counts: { feedback: 0, response: 0, revoke: 0 } },
  "revoke-only": { name: "revoke-only", counts: { feedback: 0, response: 0, revoke: 0 } },
  combined: { name: "combined", counts: { feedback: 0, response: 0, revoke: 0 } },
};

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.max(0, Math.min(sorted.length - 1, Math.floor(sorted.length * p)));
  return sorted[idx];
}

function round2(value: number): number {
  return Number(value.toFixed(2));
}

function parseIntWithMin(raw: string | undefined, label: string, min: number): number | undefined {
  if (!raw || raw.trim() === "") return undefined;
  const value = Number.parseInt(raw.trim(), 10);
  if (!Number.isFinite(value) || value < min) {
    throw new Error(`Invalid ${label}: ${raw}`);
  }
  return value;
}

function parseArg(name: string): string | undefined {
  const prefix = `--${name}=`;
  return process.argv.find((arg) => arg.startsWith(prefix))?.slice(prefix.length);
}

function parseIntOption(name: string, fallback: number, envNames: string[], min = 1): number {
  const cli = parseArg(name);
  const envRaw = envNames.map((n) => process.env[n]).find((v) => !!v);
  return (
    parseIntWithMin(cli, `--${name}`, min) ??
    parseIntWithMin(envRaw, envNames.join("/"), min) ??
    fallback
  );
}

function parseScaleList(name: string, envNames: string[], fallback: number[]): number[] {
  const cli = parseArg(name);
  const envRaw = envNames.map((n) => process.env[n]).find((v) => !!v);
  const raw = cli ?? envRaw;
  if (!raw || raw.trim() === "") return fallback;
  const values = raw
    .split(",")
    .map((s) => parseIntWithMin(s, `${name} item`, 1))
    .filter((v): v is number => v !== undefined);
  if (values.length === 0) {
    throw new Error(`No valid scales provided for ${name}`);
  }
  return values;
}

function parseScenarios(name: string, fallback: ScenarioName[]): ScenarioName[] {
  const raw = parseArg(name) ?? process.env.HASHCHAIN_SCENARIOS;
  if (!raw || raw.trim() === "") return fallback;
  const parsed = raw
    .split(",")
    .map((s) => s.trim() as ScenarioName)
    .filter((s) => Object.hasOwn(SCENARIO_BY_NAME, s));
  if (parsed.length === 0) {
    throw new Error(`Invalid scenarios list: ${raw}`);
  }
  return parsed;
}

function pubkeyFromSeed(seed: number): string {
  const bytes = new Uint8Array(32);
  for (let i = 0; i < 32; i++) {
    bytes[i] = (seed + i * 17) & 0xff;
  }
  return new PublicKey(bytes).toBase58();
}

function hash32(seed: number): Uint8Array {
  const out = Buffer.allocUnsafe(32);
  let x = (seed ^ 0x9e3779b9) >>> 0;
  for (let i = 0; i < 8; i++) {
    x = (Math.imul(x, 1664525) + 1013904223) >>> 0;
    out.writeUInt32LE(x, i * 4);
  }
  return new Uint8Array(out);
}

function rssMB(): number {
  return process.memoryUsage().rss / (1024 * 1024);
}

function createSyntheticPrisma(
  counts: { feedback: number; response: number; revoke: number },
  checkpointInterval: number
) {
  const agentId = pubkeyFromSeed(1);
  const clientPool = Array.from({ length: 64 }, (_, i) => pubkeyFromSeed(100 + i));
  const responderPool = Array.from({ length: 32 }, (_, i) => pubkeyFromSeed(200 + i));

  let checkpointWrites = 0;
  let peakRssMB = rssMB();
  const checkpointMap = new Map<string, { digest: string; eventCount: bigint; chainType: string; agentId: string }>();

  const markMemory = () => {
    const current = rssMB();
    if (current > peakRssMB) peakRssMB = current;
  };

  const prisma = {
    feedback: {
      findMany: async (args: any) => {
        const gt: bigint = args?.where?.feedbackIndex?.gt ?? -1n;
        const take: number = Math.max(1, Number(args?.take ?? checkpointInterval));
        const start = Number(gt + 1n);
        if (start >= counts.feedback) return [];
        const end = Math.min(counts.feedback, start + take);
        const rows = new Array(end - start);
        for (let i = start; i < end; i++) {
          rows[i - start] = {
            agentId,
            client: clientPool[i % clientPool.length],
            feedbackIndex: BigInt(i),
            feedbackHash: hash32(i + 1),
            createdSlot: 1_000_000n + BigInt(i),
            runningDigest: null,
            status: "FINALIZED",
          };
        }
        markMemory();
        return rows;
      },
    },
    feedbackResponse: {
      findMany: async (args: any) => {
        const gt: bigint = args?.where?.responseCount?.gt ?? -1n;
        const take: number = Math.max(1, Number(args?.take ?? checkpointInterval));
        const start = Number(gt + 1n);
        if (start >= counts.response) return [];
        const end = Math.min(counts.response, start + take);
        const rows = new Array(end - start);
        for (let i = start; i < end; i++) {
          rows[i - start] = {
            responder: responderPool[i % responderPool.length],
            responseHash: hash32(100000 + i + 1),
            responseCount: BigInt(i),
            slot: 2_000_000n + BigInt(i),
            runningDigest: null,
            status: "FINALIZED",
            feedback: {
              agentId,
              client: clientPool[i % clientPool.length],
              feedbackIndex: BigInt(i),
              feedbackHash: hash32(i + 1),
            },
          };
        }
        markMemory();
        return rows;
      },
    },
    revocation: {
      findMany: async (args: any) => {
        const gt: bigint = args?.where?.revokeCount?.gt ?? -1n;
        const take: number = Math.max(1, Number(args?.take ?? checkpointInterval));
        const start = Number(gt + 1n);
        if (start >= counts.revoke) return [];
        const end = Math.min(counts.revoke, start + take);
        const rows = new Array(end - start);
        for (let i = start; i < end; i++) {
          rows[i - start] = {
            agentId,
            client: clientPool[i % clientPool.length],
            feedbackIndex: BigInt(i),
            feedbackHash: hash32(i + 1),
            revokeCount: BigInt(i),
            slot: 3_000_000n + BigInt(i),
            runningDigest: null,
            status: "FINALIZED",
          };
        }
        markMemory();
        return rows;
      },
    },
    hashChainCheckpoint: {
      findFirst: async () => null,
      upsert: async ({ where, create, update }: any) => {
        checkpointWrites++;
        const key = `${where.agentId_chainType_eventCount.agentId}:${where.agentId_chainType_eventCount.chainType}:${where.agentId_chainType_eventCount.eventCount.toString()}`;
        checkpointMap.set(key, {
          agentId: create.agentId,
          chainType: create.chainType,
          eventCount: create.eventCount,
          digest: update?.digest ?? create.digest,
        });
      },
    },
  };

  return {
    agentId,
    prisma,
    getCheckpointWrites: () => checkpointWrites,
    getPeakRssMB: () => peakRssMB,
  };
}

function buildScenario(name: ScenarioName, scale: number): Scenario {
  if (name === "feedback-only") {
    return { ...SCENARIO_BY_NAME[name], counts: { feedback: scale, response: 0, revoke: 0 } };
  }
  if (name === "response-only") {
    return { ...SCENARIO_BY_NAME[name], counts: { feedback: 0, response: scale, revoke: 0 } };
  }
  if (name === "revoke-only") {
    return { ...SCENARIO_BY_NAME[name], counts: { feedback: 0, response: 0, revoke: scale } };
  }
  return { ...SCENARIO_BY_NAME[name], counts: { feedback: scale, response: scale, revoke: scale } };
}

async function runScenario(
  scenario: Scenario,
  iterations: number,
  warmup: number,
  checkpointInterval: number
): Promise<ScenarioResult> {
  const eventTotal = scenario.counts.feedback + scenario.counts.response + scenario.counts.revoke;
  const durations: number[] = [];

  let measuredCheckpointWrites = 0;
  let measuredRssDeltaMB = 0;
  let measuredRssPeakMB = 0;

  const totalRuns = warmup + iterations;
  for (let run = 0; run < totalRuns; run++) {
    const fixture = createSyntheticPrisma(scenario.counts, checkpointInterval);
    const verifier = new ReplayVerifier(fixture.prisma as any);
    verifier.CHECKPOINT_INTERVAL = checkpointInterval;

    const rssBefore = rssMB();
    const start = performance.now();
    const result = await verifier.fullReplay(fixture.agentId);
    const elapsed = performance.now() - start;
    const rssAfter = rssMB();

    if (!result.valid) {
      throw new Error(`Invalid replay result in scenario ${scenario.name}`);
    }

    if (run >= warmup) {
      durations.push(elapsed);
      if (measuredCheckpointWrites === 0) {
        measuredCheckpointWrites = fixture.getCheckpointWrites();
      }
      measuredRssDeltaMB = Math.max(measuredRssDeltaMB, rssAfter - rssBefore);
      measuredRssPeakMB = Math.max(measuredRssPeakMB, fixture.getPeakRssMB());
    }
  }

  const sorted = [...durations].sort((a, b) => a - b);
  const avgMs = durations.reduce((sum, value) => sum + value, 0) / durations.length;
  const eventsPerSecond = eventTotal > 0 ? (eventTotal / avgMs) * 1000 : 0;

  return {
    scale: Math.max(scenario.counts.feedback, scenario.counts.response, scenario.counts.revoke),
    scenario: scenario.name,
    counts: scenario.counts,
    avgMs: round2(avgMs),
    p50Ms: round2(percentile(sorted, 0.5)),
    p95Ms: round2(percentile(sorted, 0.95)),
    minMs: round2(sorted[0]),
    maxMs: round2(sorted[sorted.length - 1]),
    eventsPerSecond: round2(eventsPerSecond),
    checkpointWrites: measuredCheckpointWrites,
    rssDeltaMB: round2(measuredRssDeltaMB),
    rssPeakMB: round2(measuredRssPeakMB),
  };
}

async function main() {
  const singleEvents =
    parseIntWithMin(parseArg("events"), "--events", 1) ??
    parseIntWithMin(process.env.HASHCHAIN_EVENTS, "HASHCHAIN_EVENTS", 1) ??
    parseIntWithMin(process.env.HASHCHAIN_BENCH_EVENTS, "HASHCHAIN_BENCH_EVENTS", 1);

  const scales = singleEvents
    ? [singleEvents]
    : parseScaleList("scales", ["HASHCHAIN_SCALES", "HASHCHAIN_BENCH_SCALES"], [DEFAULT_EVENTS]);

  const iterations = parseIntOption("iterations", DEFAULT_ITERATIONS, ["HASHCHAIN_ITERATIONS", "HASHCHAIN_BENCH_ITERATIONS"]);
  const warmup = parseIntOption("warmup", DEFAULT_WARMUP, ["HASHCHAIN_WARMUP", "HASHCHAIN_BENCH_WARMUP"], 0);
  const checkpointInterval = parseIntOption("checkpoint", DEFAULT_CHECKPOINT_INTERVAL, ["HASHCHAIN_CHECKPOINT_INTERVAL", "HASHCHAIN_BENCH_CHECKPOINT_INTERVAL"]);
  const scenarioNames = parseScenarios("scenarios", ["feedback-only", "response-only", "revoke-only", "combined"]);

  console.log("HASHCHAIN BENCHMARK");
  console.log(
    `scales=${scales.join(",")} iterations=${iterations} warmup=${warmup} checkpoint_interval=${checkpointInterval} scenarios=${scenarioNames.join(",")}`
  );

  const results: ScenarioResult[] = [];

  for (const scale of scales) {
    console.log(`\n## SCALE ${scale.toLocaleString()} events/chain`);
    console.log("| Scenario | Feedback | Response | Revoke | Avg ms | p50 ms | p95 ms | Min ms | Max ms | Events/s | Checkpoints | RSS delta MB | RSS peak MB |");
    console.log("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

    for (const scenarioName of scenarioNames) {
      const scenario = buildScenario(scenarioName, scale);
      const result = await runScenario(scenario, iterations, warmup, checkpointInterval);
      results.push(result);
      console.log(
        `| ${result.scenario} | ${result.counts.feedback} | ${result.counts.response} | ${result.counts.revoke} | ` +
          `${result.avgMs} | ${result.p50Ms} | ${result.p95Ms} | ${result.minMs} | ${result.maxMs} | ${result.eventsPerSecond} | ` +
          `${result.checkpointWrites} | ${result.rssDeltaMB} | ${result.rssPeakMB} |`
      );
    }
  }

  console.log("\nHASHCHAIN_BENCHMARK=PASS");
  console.log(JSON.stringify({ scales, iterations, warmup, checkpointInterval, scenarios: scenarioNames, results }, null, 2));
}

main().catch((error) => {
  console.error("HASHCHAIN_BENCHMARK=FAIL");
  console.error(error);
  process.exit(1);
});
