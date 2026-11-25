const test = require('node:test');
const assert = require('node:assert/strict');
const { processInvoiceTask } = require('../../api/invoice-worker');

test('serverless invoice worker records Cold Start on first run and Warm Start on subsequent runs', async () => {
  const payload = {
    orderId: 'ord-test-1001',
    tenantId: '11111111-1111-4111-8111-111111111111',
    tenantName: 'Acme Corp (Tenant A)',
    customerEmail: 'admin@acme-corp.io',
    items: [{ name: 'Managed Kubernetes Node Pack', sku: 'ACME-K8S-NODE', quantity: 2, unitPrice: 249 }],
  };

  const first = await processInvoiceTask(payload);
  assert.equal(first.workerTelemetry.coldStart, true);
  assert.equal(first.workerTelemetry.startType, 'COLD_START');
  assert.ok(first.workerTelemetry.initDurationMs > 0);
  assert.equal(first.subtotal, 498);
  assert.equal(first.taxAmount, 39.84);
  assert.equal(first.totalAmount, 537.84);

  const pdfText = Buffer.from(first.pdfBase64, 'base64').toString('utf8');
  assert.ok(pdfText.startsWith('%PDF-1.4'));
  assert.ok(pdfText.includes('%%EOF'));
  assert.ok(pdfText.includes('Acme Corp'));

  const second = await processInvoiceTask(payload);
  assert.equal(second.workerTelemetry.coldStart, false);
  assert.equal(second.workerTelemetry.startType, 'WARM_START');
  assert.equal(second.workerTelemetry.initDurationMs, 0);
  assert.equal(second.workerTelemetry.instanceId, first.workerTelemetry.instanceId);
  assert.equal(second.workerTelemetry.invocationCount, 2);
});
