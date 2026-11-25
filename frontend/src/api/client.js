const API_BASE = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');

export const DEMO_ACCOUNTS = [
  {
    tenantSlug: 'acme-corp',
    label: 'Tenant A — Acme Corp',
    email: 'admin@acme-corp.io',
    password: 'TenantA!Password123',
    tenantId: '11111111-1111-4111-8111-111111111111',
  },
  {
    tenantSlug: 'globex-inc',
    label: 'Tenant B — Globex Inc',
    email: 'admin@globex-inc.io',
    password: 'TenantB!Password123',
    tenantId: '22222222-2222-4222-8222-222222222222',
  },
];

export async function apiRequest(path, { method = 'GET', body, token, tenantHeader } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (tenantHeader) headers['x-tenant-id'] = tenantHeader;

  const response = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const err = new Error(data.message || data.error || `HTTP ${response.status}`);
    err.status = response.status;
    err.payload = data;
    throw err;
  }

  return { status: response.status, data };
}

export async function downloadInvoicePdf(orderId, { token, tenantHeader }) {
  const headers = {};
  if (token) headers.Authorization = `Bearer ${token}`;
  if (tenantHeader) headers['x-tenant-id'] = tenantHeader;

  const res = await fetch(`${API_BASE}/api/v1/diagnostics/invoices/${orderId}/pdf`, { headers });
  if (!res.ok) {
    const errData = await res.json().catch(() => ({}));
    throw new Error(errData.message || `Failed to download PDF (${res.status})`);
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `invoice-${orderId.slice(0, 8)}.pdf`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}
