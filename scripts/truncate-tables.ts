/**
 * Truncate all indexer tables
 * Run: npx tsx scripts/truncate-tables.ts
 */
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function truncateAllTables() {
  console.log('🗑️  Truncating all tables...\n');

  try {
    // Delete in correct order (respect foreign keys)
    const feedbackResponses = await prisma.feedbackResponse.deleteMany();
    console.log(`  FeedbackResponse: ${feedbackResponses.count} deleted`);

    const feedbacks = await prisma.feedback.deleteMany();
    console.log(`  Feedback: ${feedbacks.count} deleted`);

    const validations = await prisma.validation.deleteMany();
    console.log(`  Validation: ${validations.count} deleted`);

    const metadata = await prisma.agentMetadata.deleteMany();
    console.log(`  AgentMetadata: ${metadata.count} deleted`);

    const agents = await prisma.agent.deleteMany();
    console.log(`  Agent: ${agents.count} deleted`);

    const registries = await prisma.registry.deleteMany();
    console.log(`  Registry: ${registries.count} deleted`);

    const eventLogs = await prisma.eventLog.deleteMany();
    console.log(`  EventLog: ${eventLogs.count} deleted`);

    // Reset indexer state
    await prisma.indexerState.deleteMany();
    console.log(`  IndexerState: reset`);

    console.log('\n✅ All tables truncated successfully!');
  } catch (error) {
    console.error('❌ Error truncating tables:', error);
    throw error;
  } finally {
    await prisma.$disconnect();
  }
}

truncateAllTables();
