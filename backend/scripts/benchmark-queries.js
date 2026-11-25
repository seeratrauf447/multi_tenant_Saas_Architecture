const { pool } = require('../src/config/db');
const { TENANTS } = require('../src/db/seed');
const { runNPlusOneBenchmark } = require('../src/services/benchmarkService');

async function main() {
  const limit = Number.parseInt(process.argv[2] || '25', 10);
  const result = await runNPlusOneBenchmark(TENANTS[0].id, limit);

  console.log('\n==============================================================');
  console.log('N+1 QUERY VS SINGLE-JOIN BENCHMARK');
  console.log('==============================================================');
  console.log(`Tenant ID          : ${result.tenantId}`);
  console.log(`Orders Fetched     : ${result.rowCount}`);
  console.log(`Unoptimized (N+1)  : ${result.unoptimized.queryCount} SQL queries | ${result.unoptimized.durationMs} ms`);
  console.log(`Optimized (JOIN)   : ${result.optimized.queryCount} SQL query   | ${result.optimized.durationMs} ms`);
  console.log(`Queries Eliminated : ${result.queriesSaved}`);
  console.log(`Speedup Factor     : ${result.speedupFactor}x`);
  console.log('==============================================================\n');
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error('[N+1 Benchmark] Failed:', err.message);
    process.exit(1);
  });
