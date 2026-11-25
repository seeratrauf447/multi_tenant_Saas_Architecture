const path = require('node:path');
const { execFile } = require('node:child_process');
const { performance } = require('node:perf_hooks');
const config = require('../config/env');

const localWorkerPath = path.resolve(__dirname, '../../../api/invoice-worker.js');
const { processInvoiceTask } = require(localWorkerPath);

/**
 * Invokes the serverless invoice worker:
 * - In cloud mode (SERVERLESS_INVOICE_URL configured), dispatches over HTTP to Vercel Serverless Function.
 * - In local mode, uses the in-process warm module OR spawns a fresh Node process if forceColdStart=true
 *   to measure real OS-process + module bootstrap Cold Start latency.
 */
async function invokeInvoiceWorker(payload, { forceColdStart = false } = {}) {
  const networkStart = performance.now();

  if (config.serverlessInvoiceUrl) {
    const response = await fetch(config.serverlessInvoiceUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-tenant-id': String(payload.tenantId || ''),
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `Serverless worker HTTP ${response.status}: ${errorText.slice(0, 200)}`
      );
    }

    const data = await response.json();
    return {
      ...data,
      invocationMode: 'REMOTE_SERVERLESS_HTTP',
      endpoint: config.serverlessInvoiceUrl,
      roundTripMs: Number((performance.now() - networkStart).toFixed(3)),
    };
  }

  if (forceColdStart) {
    return new Promise((resolve, reject) => {
      const script = `
        const { performance } = require('node:perf_hooks');
        const t0 = performance.now();
        const worker = require(${JSON.stringify(localWorkerPath)});
        const payload = JSON.parse(process.env.INVOICE_PAYLOAD || '{}');
        worker.processInvoiceTask(payload).then((res) => {
          const processTotalMs = Number((performance.now() - t0).toFixed(3));
          res.workerTelemetry.coldStart = true;
          res.workerTelemetry.startType = 'COLD_START';
          res.workerTelemetry.processBootPlusExecMs = processTotalMs;
          process.stdout.write(JSON.stringify(res));
        }).catch((err) => {
          process.stderr.write(err.stack || err.message);
          process.exit(1);
        });
      `;

      execFile(
        process.execPath,
        ['-e', script],
        {
          env: {
            ...process.env,
            INVOICE_PAYLOAD: JSON.stringify(payload),
          },
          maxBuffer: 5 * 1024 * 1024,
        },
        (err, stdout, stderr) => {
          if (err) {
            return reject(new Error(stderr || err.message));
          }
          try {
            const parsed = JSON.parse(stdout);
            return resolve({
              ...parsed,
              invocationMode: 'LOCAL_ISOLATED_COLD_PROCESS',
              roundTripMs: Number((performance.now() - networkStart).toFixed(3)),
            });
          } catch (parseErr) {
            return reject(parseErr);
          }
        }
      );
    });
  }

  const result = await processInvoiceTask(payload);
  return {
    ...result,
    invocationMode: 'LOCAL_WARM_ADAPTER',
    roundTripMs: Number((performance.now() - networkStart).toFixed(3)),
  };
}

module.exports = {
  invokeInvoiceWorker,
};
