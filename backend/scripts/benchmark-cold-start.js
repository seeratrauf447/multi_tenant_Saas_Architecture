const { invokeInvoiceWorker } = require('../src/services/serverlessClient');

async function main() {
  const samplePayload = {
    orderId: `cli-bench-${Date.now()}`,
    tenantId: '11111111-1111-4111-8111-111111111111',
    tenantName: 'Acme Corp (Tenant A)',
    customerEmail: 'admin@acme-corp.io',
    items: [
      { name: 'Managed Kubernetes Node Pack', sku: 'ACME-K8S-NODE', quantity: 2, unitPrice: 249 },
      { name: 'Dedicated Edge API Gateway', sku: 'ACME-EDGE-GW', quantity: 1, unitPrice: 129.5 },
    ],
  };

  console.log('\n==============================================================');
  console.log('SERVERLESS INVOICE WORKER - COLD START VS WARM START');
  console.log('==============================================================');

  const cold = await invokeInvoiceWorker(samplePayload, { forceColdStart: true });
  const coldMs = cold.roundTripMs;
  console.log(
    `[Run 0 - COLD START] mode=${cold.invocationMode} instance=${cold.workerTelemetry.instanceId.slice(0, 8)} init=${cold.workerTelemetry.initDurationMs}ms exec=${cold.workerTelemetry.executionDurationMs}ms totalColdRoundTrip=${coldMs}ms`
  );

  // Prime the warm in-process container once before measuring steady-state warm invocations
  await invokeInvoiceWorker(samplePayload, { forceColdStart: false });

  const warmLatencies = [];
  for (let i = 1; i <= 5; i += 1) {
    const warm = await invokeInvoiceWorker(samplePayload, { forceColdStart: false });
    warmLatencies.push(warm.workerTelemetry.totalLatencyMs);
    console.log(
      `[Run ${i} - WARM START] mode=${warm.invocationMode} instance=${warm.workerTelemetry.instanceId.slice(0, 8)} init=${warm.workerTelemetry.initDurationMs}ms exec=${warm.workerTelemetry.executionDurationMs}ms total=${warm.workerTelemetry.totalLatencyMs}ms`
    );
  }

  const avgWarm = Number(
    (warmLatencies.reduce((a, b) => a + b, 0) / warmLatencies.length).toFixed(3)
  );
  console.log('--------------------------------------------------------------');
  console.log(`Cold Start Total Latency : ${coldMs} ms`);
  console.log(`Warm Start Avg Latency   : ${avgWarm} ms`);
  console.log(`Cold-to-Warm Ratio       : ${(coldMs / avgWarm).toFixed(2)}x`);
  console.log('==============================================================\n');
}

main().catch((err) => {
  console.error('[Cold-Start Benchmark] Failed:', err);
  process.exit(1);
});
