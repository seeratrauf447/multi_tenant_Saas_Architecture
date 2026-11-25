import React, { useState } from 'react';
import TenantSwitcherBar from './components/TenantSwitcherBar';
import OrdersConsole from './components/OrdersConsole';
import PerformanceLab from './components/PerformanceLab';
import KafkaMonitor from './components/KafkaMonitor';
import ServerlessMonitor from './components/ServerlessMonitor';
import { apiRequest, DEMO_ACCOUNTS } from './api/client';

const TABS = [
  { id: 'orders', label: '1. Orders & Tenant Isolation' },
  { id: 'perf', label: '2. N+1, EXPLAIN & pg.Pool' },
  { id: 'kafka', label: '3. Kafka Consumer Offsets' },
  { id: 'serverless', label: '4. Serverless Cold vs Warm' },
];

export default function App() {
  const [session, setSession] = useState(null);
  const [activeTab, setActiveTab] = useState('orders');
  const [email, setEmail] = useState(DEMO_ACCOUNTS[0].email);
  const [password, setPassword] = useState(DEMO_ACCOUNTS[0].password);
  const [tenantSlug, setTenantSlug] = useState(DEMO_ACCOUNTS[0].tenantSlug);
  const [loginError, setLoginError] = useState(null);

  async function handleManualLogin(e) {
    e.preventDefault();
    setLoginError(null);
    try {
      const { data } = await apiRequest('/api/v1/auth/login', {
        method: 'POST',
        body: { email, password, tenantSlug },
        tenantHeader: tenantSlug,
      });
      setSession(data);
    } catch (err) {
      setLoginError(err.message);
    }
  }

  return (
    <div className="app-shell">
      <TenantSwitcherBar
        session={session}
        onLoginSuccess={(data) => setSession(data)}
        onLogout={() => setSession(null)}
      />

      {!session ? (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>Sign In to Tenant Workspace</h3>
          <p style={{ color: 'var(--muted)', fontSize: '0.9rem' }}>
            Click <strong>Tenant A — Acme Corp</strong> or <strong>Tenant B — Globex Inc</strong> above for instant demo sign-in, or sign in with explicit tenant credentials below.
          </p>
          <form onSubmit={handleManualLogin} className="form-row">
            <select value={tenantSlug} onChange={(e) => setTenantSlug(e.target.value)}>
              <option value="acme-corp">acme-corp (Tenant A)</option>
              <option value="globex-inc">globex-inc (Tenant B)</option>
            </select>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="Email"
              required
            />
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Password"
              required
            />
            <button className="btn btn-primary" type="submit">Sign In</button>
          </form>
          {loginError && <div className="alert alert-err">{loginError}</div>}
        </div>
      ) : (
        <>
          <div className="tabs">
            {TABS.map((t) => (
              <button
                key={t.id}
                className={`tab ${activeTab === t.id ? 'active' : ''}`}
                onClick={() => setActiveTab(t.id)}
              >
                {t.label}
              </button>
            ))}
          </div>

          {activeTab === 'orders' && <OrdersConsole session={session} />}
          {activeTab === 'perf' && <PerformanceLab session={session} />}
          {activeTab === 'kafka' && <KafkaMonitor session={session} />}
          {activeTab === 'serverless' && <ServerlessMonitor session={session} />}
        </>
      )}
    </div>
  );
}
