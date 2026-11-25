import React, { useState, useEffect } from 'react';
import { apiRequest } from '../api/client';

export default function PerformanceLab({ session }) {
  const [n1Report, setN1Report] = useState(null);
  const [explainReport, setExplainReport] = useState(null);
  const [poolStats, setPoolStats] = useState(null);
  const [loading, setLoading] = useState({ n1: false, explain: false });
  const [error, setError] = useState(null);

  async function fetchPool() {
    try {
      const { data } = await apiRequest('/api/v1/diagnostics/pool', {
        token: session.token,
        tenantHeader: session.tenant.slug,
      });
      setPoolStats(data.pool);
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    fetchPool();
  }, [session?.tenant?.id]);

  async function runNPlus1() {
    setLoading((s) => ({ ...s, n1: true }));
    setError(null);
    try {
      const { data } = await apiRequest('/api/v1/orders/benchmark/n-plus-1?limit=25', {
        token: session.token,
        tenantHeader: session.tenant.slug,
      });
      setN1Report(data);
      fetchPool();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading((s) => ({ ...s, n1: false }));
    }
  }

  async function runExplain() {
    setLoading((s) => ({ ...s, explain: true }));
    setError(null);
    try {
      const { data } = await apiRequest('/api/v1/diagnostics/explain', {
        token: session.token,
        tenantHeader: session.tenant.slug,
      });
      setExplainReport(data);
      fetchPool();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading((s) => ({ ...s, explain: false }));
    }
  }

  return (
    <div>
      {error && <div className="alert alert-err" style={{ marginBottom: 14 }}>{error}</div>}

      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h3 style={{ marginTop: 0 }}>pg.Pool Connection Pooling Telemetry</h3>
          <button className="btn" onClick={fetchPool}>Refresh Pool</button>
        </div>
        {poolStats && (
          <div className="grid-4">
            <div className="stat-card">
              <div className="stat-label">Total Connections</div>
              <div className="stat-value">{poolStats.totalCount} / {poolStats.maxConnections}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Idle Clients</div>
              <div className="stat-value">{poolStats.idleCount}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Waiting Queue</div>
              <div className="stat-value">{poolStats.waitingCount}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Idle Timeout</div>
              <div className="stat-value">{poolStats.idleTimeoutMillis}ms</div>
            </div>
          </div>
        )}
      </div>

      <div className="grid-2">
        <div className="panel">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h3 style={{ marginTop: 0 }}>N+1 Query vs Single JOIN</h3>
            <button className="btn btn-primary" disabled={loading.n1} onClick={runNPlus1}>
              {loading.n1 ? 'Benchmarking...' : 'Run N+1 Benchmark'}
            </button>
          </div>
          {n1Report && (
            <div>
              <div className="grid-2" style={{ marginBottom: 10 }}>
                <div className="stat-card">
                  <div className="stat-label">Unoptimized (N+1)</div>
                  <div className="stat-value" style={{ color: '#fca5a5' }}>
                    {n1Report.unoptimized.queryCount} queries · {n1Report.unoptimized.durationMs}ms
                  </div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Optimized (JOIN)</div>
                  <div className="stat-value" style={{ color: '#6ee7b7' }}>
                    {n1Report.optimized.queryCount} query · {n1Report.optimized.durationMs}ms
                  </div>
                </div>
              </div>
              <span className="badge badge-ok">
                {n1Report.queriesSaved} SQL round-trips eliminated · {n1Report.speedupFactor}x faster
              </span>
            </div>
          )}
        </div>

        <div className="panel">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <h3 style={{ marginTop: 0 }}>EXPLAIN ANALYZE (Composite Index)</h3>
            <button className="btn btn-primary" disabled={loading.explain} onClick={runExplain}>
              {loading.explain ? 'Analyzing...' : 'Run EXPLAIN ANALYZE'}
            </button>
          </div>
          {explainReport && (
            <div>
              <div className="grid-2" style={{ marginBottom: 10 }}>
                <div className="stat-card">
                  <div className="stat-label">Before Index ({explainReport.beforeIndexing.nodeType})</div>
                  <div className="stat-value" style={{ color: '#fca5a5' }}>
                    {explainReport.beforeIndexing.executionTimeMs}ms
                  </div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">After Index ({explainReport.afterIndexing.nodeType})</div>
                  <div className="stat-value" style={{ color: '#6ee7b7' }}>
                    {explainReport.afterIndexing.executionTimeMs}ms
                  </div>
                </div>
              </div>
              <span className="badge badge-info">
                Rows scanned: {explainReport.totalOrdersInTable} · Speedup: {explainReport.improvementFactor}x
              </span>
              <h4>Before Composite Index</h4>
              <pre className="code-box">{explainReport.beforeIndexing.planText}</pre>
              <h4>After Composite Index ({explainReport.compositeIndex})</h4>
              <pre className="code-box">{explainReport.afterIndexing.planText}</pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
