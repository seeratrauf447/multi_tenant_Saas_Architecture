import React, { useState, useEffect, useCallback } from 'react';
import { apiRequest, downloadInvoicePdf } from '../api/client';

export default function ServerlessMonitor({ session }) {
  const [report, setReport] = useState(null);
  const [invoices, setInvoices] = useState([]);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);

  const loadInvoices = useCallback(async () => {
    try {
      const { data } = await apiRequest('/api/v1/diagnostics/invoices?limit=15', {
        token: session.token,
        tenantHeader: session.tenant.slug,
      });
      setInvoices(data.invoices || []);
    } catch (err) {
      setError(err.message);
    }
  }, [session]);

  useEffect(() => {
    loadInvoices();
  }, [loadInvoices]);

  async function runColdStartTest() {
    setRunning(true);
    setError(null);
    try {
      const { data } = await apiRequest('/api/v1/diagnostics/cold-start-test', {
        method: 'POST',
        token: session.token,
        tenantHeader: session.tenant.slug,
        body: { warmCount: 3 },
      });
      setReport(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="grid-2">
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h3 style={{ marginTop: 0 }}>Serverless Cold Start vs Warm Start</h3>
          <button className="btn btn-primary" disabled={running} onClick={runColdStartTest}>
            {running ? 'Running Probe...' : 'Run Cold vs Warm Test'}
          </button>
        </div>
        {error && <div className="alert alert-err">{error}</div>}
        {report && (
          <div>
            <div className="grid-2" style={{ marginBottom: 12 }}>
              <div className="stat-card">
                <div className="stat-label">Cold Start Latency</div>
                <div className="stat-value" style={{ color: '#fcd34d' }}>
                  {report.summary.coldLatencyMs}ms
                </div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Avg Warm Start Latency</div>
                <div className="stat-value" style={{ color: '#6ee7b7' }}>
                  {report.summary.avgWarmLatencyMs}ms
                </div>
              </div>
            </div>
            <span className="badge badge-info" style={{ marginBottom: 10 }}>
              Cold/Warm Ratio: {report.summary.coldVsWarmRatio}x
            </span>
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Type</th>
                  <th>Init</th>
                  <th>Exec</th>
                  <th>Total</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className="mono">#0</td>
                  <td><span className="badge badge-warn">COLD_START</span></td>
                  <td className="mono">{report.coldStart.workerTelemetry.initDurationMs}ms</td>
                  <td className="mono">{report.coldStart.workerTelemetry.executionDurationMs}ms</td>
                  <td className="mono">{report.summary.coldLatencyMs}ms</td>
                </tr>
                {report.warmStarts.map((w) => (
                  <tr key={w.invocation}>
                    <td className="mono">#{w.invocation}</td>
                    <td><span className="badge badge-ok">WARM_START</span></td>
                    <td className="mono">{w.workerTelemetry.initDurationMs}ms</td>
                    <td className="mono">{w.workerTelemetry.executionDurationMs}ms</td>
                    <td className="mono">{w.workerTelemetry.totalLatencyMs}ms</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h3 style={{ marginTop: 0 }}>Generated Tenant Invoices</h3>
          <button className="btn" onClick={loadInvoices}>Refresh</button>
        </div>
        <table>
          <thead>
            <tr>
              <th>Invoice #</th>
              <th>Total</th>
              <th>Start Type</th>
              <th>Latency</th>
              <th>PDF</th>
            </tr>
          </thead>
          <tbody>
            {invoices.length === 0 ? (
              <tr>
                <td colSpan="5" style={{ color: 'var(--muted)' }}>
                  No invoices generated yet. Place an order with the consumer running.
                </td>
              </tr>
            ) : (
              invoices.map((inv) => (
                <tr key={inv.id}>
                  <td className="mono">{inv.invoice_number}</td>
                  <td className="mono">${inv.total_amount.toFixed(2)}</td>
                  <td>
                    <span className={`badge ${inv.cold_start ? 'badge-warn' : 'badge-ok'}`}>
                      {inv.cold_start ? 'COLD' : 'WARM'}
                    </span>
                  </td>
                  <td className="mono">{inv.total_latency_ms}ms</td>
                  <td>
                    <button
                      className="btn btn-success"
                      style={{ padding: '4px 8px', fontSize: '0.75rem' }}
                      onClick={() =>
                        downloadInvoicePdf(inv.order_id, {
                          token: session.token,
                          tenantHeader: session.tenant.slug,
                        }).catch((err) => setError(err.message))
                      }
                    >
                      Download
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
