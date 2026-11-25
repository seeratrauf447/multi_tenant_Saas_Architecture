import React, { useState } from 'react';
import { DEMO_ACCOUNTS, apiRequest } from '../api/client';

export default function TenantSwitcherBar({ session, onLoginSuccess, onLogout }) {
  const [loadingSlug, setLoadingSlug] = useState(null);
  const [probeResult, setProbeResult] = useState(null);

  async function handleDemoLogin(account) {
    setLoadingSlug(account.tenantSlug);
    setProbeResult(null);
    try {
      const { data } = await apiRequest('/api/v1/auth/login', {
        method: 'POST',
        body: {
          email: account.email,
          password: account.password,
          tenantSlug: account.tenantSlug,
        },
        tenantHeader: account.tenantSlug,
      });
      onLoginSuccess(data);
    } catch (err) {
      setProbeResult({ ok: false, text: `Login error: ${err.message}` });
    } finally {
      setLoadingSlug(null);
    }
  }

  async function handleCrossTenantProbe() {
    if (!session) return;
    const target = DEMO_ACCOUNTS.find((a) => a.tenantSlug !== session.tenant.slug);
    try {
      await apiRequest('/api/v1/orders', {
        token: session.token,
        tenantHeader: target.tenantId, // Intentionally spoofed x-tenant-id!
      });
      setProbeResult({
        ok: false,
        text: 'SECURITY FAILURE: Cross-tenant request was unexpectedly allowed!',
      });
    } catch (err) {
      setProbeResult({
        ok: true,
        text: `Boundary Enforced (${err.status}): ${err.message}`,
      });
    }
  }

  return (
    <div className="header-card">
      <div className="header-top">
        <div className="brand">
          <h1>CloudFlow B2B — Multi-Tenant SaaS Platform</h1>
          <p>
            CSC337 Lab 06 · PostgreSQL RLS &amp; Composite Indexing · Kafka Event Bus · Serverless Cold-Start Lab
          </p>
        </div>

        <div className="tenant-controls">
          {DEMO_ACCOUNTS.map((acc) => {
            const isActive = session?.tenant?.slug === acc.tenantSlug;
            return (
              <button
                key={acc.tenantSlug}
                className={`btn ${isActive ? 'btn-primary' : ''}`}
                disabled={loadingSlug === acc.tenantSlug}
                onClick={() => handleDemoLogin(acc)}
              >
                {loadingSlug === acc.tenantSlug ? 'Signing in...' : acc.label}
              </button>
            );
          })}

          {session && (
            <>
              <button className="btn btn-danger" onClick={handleCrossTenantProbe}>
                Test Cross-Tenant Spoof (403)
              </button>
              <button className="btn" onClick={onLogout}>
                Sign Out
              </button>
            </>
          )}
        </div>
      </div>

      {session && (
        <div style={{ marginTop: 12, display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'center' }}>
          <span className="badge badge-info">Active Tenant: {session.tenant.name}</span>
          <span className="badge badge-ok">Slug: {session.tenant.slug}</span>
          <span className="mono" style={{ color: 'var(--muted)' }}>
            tenant_id={session.tenant.id} · user={session.user.email}
          </span>
        </div>
      )}

      {probeResult && (
        <div className={`alert ${probeResult.ok ? 'alert-ok' : 'alert-err'}`}>
          {probeResult.text}
        </div>
      )}
    </div>
  );
}
