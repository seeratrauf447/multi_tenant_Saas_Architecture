const express = require('express');
const { authenticateJwt } = require('../middleware/auth');
const { resolveTenant } = require('../middleware/tenantResolver');
const { getPoolMetrics, UUID_REGEX } = require('../config/db');
const { runExplainAnalyzeBenchmark } = require('../services/benchmarkService');
const { invokeInvoiceWorker } = require('../services/serverlessClient');
const invoiceRepository = require('../repositories/invoiceRepository');

const router = express.Router();

router.use(authenticateJwt, resolveTenant);

router.get('/pool', (_req, res) => {
  return res.json({
    pool: getPoolMetrics(),
    timestamp: new Date().toISOString(),
  });
});

router.get('/explain', async (req, res, next) => {
  try {
    const report = await runExplainAnalyzeBenchmark(req.tenant.id);
    return res.json(report);
  } catch (err) {
    return next(err);
  }
});

router.get('/kafka-events', async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit || '30', 10) || 30, 1), 100);
    const events = await invoiceRepository.listKafkaEvents(req.tenant.id, limit);
    return res.json({
      tenant: req.tenant,
      events,
    });
  } catch (err) {
    return next(err);
  }
});

router.get('/invoices', async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit || '20', 10) || 20, 1), 100);
    const invoices = await invoiceRepository.listRecentInvoices(req.tenant.id, limit);
    return res.json({
      tenant: req.tenant,
      invoices,
    });
  } catch (err) {
    return next(err);
  }
});

router.get('/invoices/:orderId/pdf', async (req, res, next) => {
  try {
    const { orderId } = req.params;
    if (!UUID_REGEX.test(orderId)) {
      return res.status(400).json({ error: 'Invalid orderId UUID' });
    }

    const invoice = await invoiceRepository.getInvoiceByOrderId(req.tenant.id, orderId);
    if (!invoice) {
      return res.status(404).json({
        error: 'Invoice not found',
        message: `No invoice found for order ${orderId} in tenant "${req.tenant.slug}".`,
      });
    }

    const pdfBuffer = Buffer.from(invoice.pdf_base64, 'base64');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${invoice.invoice_number}.pdf"`
    );
    return res.send(pdfBuffer);
  } catch (err) {
    return next(err);
  }
});

/**
 * POST /api/v1/diagnostics/cold-start-test
 * Measures 1 Cold Start invocation (fresh runtime) followed by N Warm Start invocations.
 */
router.post('/cold-start-test', async (req, res, next) => {
  try {
    const warmCount = Math.min(Math.max(Number(req.body?.warmCount || 3), 1), 5);
    const samplePayload = {
      orderId: `bench-${Date.now()}`,
      tenantId: req.tenant.id,
      tenantName: req.tenant.name,
      customerEmail: req.user.email,
      items: [
        { name: 'Benchmark Enterprise Node', sku: 'BENCH-01', quantity: 2, unitPrice: 149 },
      ],
    };

    const coldRun = await invokeInvoiceWorker(samplePayload, { forceColdStart: true });
    // Ensure local warm adapter is primed if this is the first call in this API process
    await invokeInvoiceWorker(samplePayload, { forceColdStart: false });
    const warmRuns = [];
    for (let i = 0; i < warmCount; i += 1) {
      const warm = await invokeInvoiceWorker(samplePayload, { forceColdStart: false });
      warmRuns.push({
        invocation: i + 1,
        invocationMode: warm.invocationMode,
        roundTripMs: warm.roundTripMs,
        workerTelemetry: warm.workerTelemetry,
      });
    }

    const coldTotalMs = coldRun.roundTripMs;
    const avgWarmMs = Number(
      (
        warmRuns.reduce((sum, r) => sum + r.workerTelemetry.totalLatencyMs, 0) /
        warmRuns.length
      ).toFixed(3)
    );

    return res.json({
      tenant: req.tenant,
      coldStart: {
        invocationMode: coldRun.invocationMode,
        roundTripMs: coldRun.roundTripMs,
        workerTelemetry: coldRun.workerTelemetry,
      },
      warmStarts: warmRuns,
      summary: {
        coldLatencyMs: coldTotalMs,
        avgWarmLatencyMs: avgWarmMs,
        coldVsWarmRatio: avgWarmMs > 0 ? Number((coldTotalMs / avgWarmMs).toFixed(2)) : 1,
      },
    });
  } catch (err) {
    return next(err);
  }
});

module.exports = router;
