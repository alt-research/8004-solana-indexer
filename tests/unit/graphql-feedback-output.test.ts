import { describe, it, expect } from "vitest";

import { feedbackResolvers } from "../../src/api/graphql/resolvers/feedback.js";
import { solanaResolvers } from "../../src/api/graphql/resolvers/solana.js";

describe("GraphQL feedback output parity", () => {
  it("returns normalized feedback value as string", () => {
    const row = {
      value: "1234",
      value_decimals: 2,
    } as any;

    const normalized = feedbackResolvers.Feedback.value(row);
    expect(normalized).toBe("12.34");
    expect(typeof normalized).toBe("string");
  });

  it("keeps Solana extension valueRaw/valueDecimals lossless", () => {
    const row = {
      value: "170141183460469231731687303715884105727",
      value_decimals: 18,
    } as any;

    expect(solanaResolvers.SolanaFeedbackExtension.valueRaw(row)).toBe(
      "170141183460469231731687303715884105727"
    );
    expect(solanaResolvers.SolanaFeedbackExtension.valueDecimals(row)).toBe(18);
  });
});
