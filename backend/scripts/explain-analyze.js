const { pool } = require('../src/config/db');
const { runMigrations } = require('../src/db/migrate');
const { seedDatabase, TENANTS } = require('../src/db/seed');
const { runExplainAnalyzeBenchmark } = require('../src/services/benchmarkService');

async function main() {
  await runMigrations();

  const { rows } = await pool.query('SELECT COUNT(*)::int AS cnt FROM orders');
  if (rows[0].cnt < 50000) {
    console.log(`[EXPLAIN ANALYZE] Found ${rows[0].cnt} orders. Seeding 50,000 rows for realistic planner stats...`);
    await seedDatabase({ bulkOrderCount: 50000 });
  }

  const report = await runExplainAnalyzeBenchmark(TENANTS[0].id);

  console.log('\n==============================================================');
  console.log('EXPLAIN (ANALYZE, BUFFERS) - COMPOSITE INDEX BENCHMARK');
  console.log('==============================================================');
  console.log(`Tenant ID         : ${report.tenantId}`);
  console.log(`Total Rows        : ${report.totalOrdersInTable}`);
  console.log(`Composite Index   : ${report.compositeIndex}`);
  console.log('--------------------------------------------------------------');
  console.log(`BEFORE (No Index) : ${report.beforeIndexing.nodeType} | ${report.beforeIndexing.executionTimeMs} ms`);
  console.log(report.beforeIndexing.planText);
  console.log('--------------------------------------------------------------');
  console.log(`AFTER (Indexed)   : ${report.afterIndexing.nodeType} | ${report.afterIndexing.executionTimeMs} ms`);
  console.log(report.afterIndexing.planText);
  console.log('--------------------------------------------------------------');
  console.log(`Speedup Factor    : ${report.improvementFactor}x faster`);
  console.log('==============================================================\n');
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error('[EXPLAIN ANALYZE] Failed:', err.message);
    process.exit(1);
  });
