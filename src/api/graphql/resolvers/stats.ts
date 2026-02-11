import type { AgentStatsRow } from '../dataloaders.js';
import { encodeAgentId } from '../utils/ids.js';

function toUnixTimestamp(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const ms = new Date(dateStr).getTime();
  return isNaN(ms) ? null : String(Math.floor(ms / 1000));
}

export const statsResolvers = {
  AgentStats: {
    id(parent: AgentStatsRow & { _asset?: string }) {
      return parent._asset ? encodeAgentId(parent._asset) : parent.asset;
    },
    totalFeedback(parent: AgentStatsRow) {
      return parent.feedback_count;
    },
    averageFeedbackValue(parent: AgentStatsRow) {
      return parent.avg_value;
    },
    totalValidations(parent: AgentStatsRow) {
      return parent.validation_count;
    },
    completedValidations(parent: AgentStatsRow) {
      return parent.completed_validations;
    },
    averageValidationScore(parent: AgentStatsRow) {
      return parent.avg_validation_score;
    },
    lastActivity(parent: AgentStatsRow) {
      return toUnixTimestamp(parent.last_activity);
    },
  },

  Protocol: {
    id(parent: { id: string }) { return parent.id; },
    totalAgents(parent: { totalAgents: string }) { return parent.totalAgents; },
    totalFeedback(parent: { totalFeedback: string }) { return parent.totalFeedback; },
    totalValidations(parent: { totalValidations: string }) { return parent.totalValidations; },
    tags(parent: { tags: string[] }) { return parent.tags; },
  },

  GlobalStats: {
    id(parent: { id: string }) { return parent.id; },
    totalAgents(parent: { totalAgents: string }) { return parent.totalAgents; },
    totalFeedback(parent: { totalFeedback: string }) { return parent.totalFeedback; },
    totalValidations(parent: { totalValidations: string }) { return parent.totalValidations; },
    totalProtocols(parent: { totalProtocols: string }) { return parent.totalProtocols; },
    tags(parent: { tags: string[] }) { return parent.tags; },
  },
};
