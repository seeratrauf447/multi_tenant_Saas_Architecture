const test = require('node:test');
const assert = require('node:assert/strict');
const {
  createTenantResolver,
  extractSubdomainTenant,
  getActiveTenantContext,
} = require('../src/middleware/tenantResolver');

const TENANT_A = {
  id: '11111111-1111-4111-8111-111111111111',
  slug: 'acme-corp',
  name: 'Acme Corp (Tenant A)',
  plan: 'ENTERPRISE',
};

const TENANT_B = {
  id: '22222222-2222-4222-8222-222222222222',
  slug: 'globex-inc',
  name: 'Globex Inc (Tenant B)',
  plan: 'BUSINESS',
};

const mockLookup = async (identifier) => {
  if (identifier === TENANT_A.id || identifier === TENANT_A.slug) return TENANT_A;
  if (identifier === TENANT_B.id || identifier === TENANT_B.slug) return TENANT_B;
  return null;
};

function createMockRes() {
  const headers = {};
  return {
    statusCode: 200,
    body: null,
    headers,
    setHeader(k, v) {
      headers[k.toLowerCase()] = v;
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
}

test('extractSubdomainTenant parses local and production subdomains accurately', () => {
  assert.equal(extractSubdomainTenant('acme-corp.localhost:4000'), 'acme-corp');
  assert.equal(extractSubdomainTenant('globex-inc.saas.vercel.app'), 'globex-inc');
  assert.equal(extractSubdomainTenant('localhost:4000'), null);
  assert.equal(extractSubdomainTenant('www.saas.com'), null);
  assert.equal(extractSubdomainTenant('127.0.0.1:4000'), null);
});

test('resolveTenant resolves via JWT + matching x-tenant-id and populates AsyncLocalStorage', async () => {
  const resolver = createTenantResolver({ lookupTenant: mockLookup });
  const req = {
    user: { tenantId: TENANT_A.id },
    headers: { 'x-tenant-id': TENANT_A.slug, host: 'localhost:4000' },
  };
  const res = createMockRes();

  let observedAlsTenant = null;
  await resolver(req, res, () => {
    observedAlsTenant = getActiveTenantContext();
  });

  assert.equal(res.statusCode, 200);
  assert.equal(req.tenant.id, TENANT_A.id);
  assert.equal(observedAlsTenant.id, TENANT_A.id);
  assert.deepEqual(req.tenant.resolvedVia, ['jwt', 'x-tenant-id']);
  assert.equal(res.headers['x-resolved-tenant-slug'], 'acme-corp');
});

test('resolveTenant blocks cross-tenant header spoofing with 403 Forbidden', async () => {
  const resolver = createTenantResolver({ lookupTenant: mockLookup });
  const req = {
    user: { tenantId: TENANT_A.id },
    headers: { 'x-tenant-id': TENANT_B.id, host: 'localhost:4000' },
  };
  const res = createMockRes();
  let nextCalled = false;

  await resolver(req, res, () => {
    nextCalled = true;
  });

  assert.equal(nextCalled, false);
  assert.equal(res.statusCode, 403);
  assert.equal(res.body.error, 'Cross-tenant boundary violation');
});

test('resolveTenant blocks cross-tenant subdomain mismatch with 403 Forbidden', async () => {
  const resolver = createTenantResolver({ lookupTenant: mockLookup });
  const req = {
    user: { tenantId: TENANT_A.id },
    headers: { host: 'globex-inc.localhost:4000' },
  };
  const res = createMockRes();
  let nextCalled = false;

  await resolver(req, res, () => {
    nextCalled = true;
  });

  assert.equal(nextCalled, false);
  assert.equal(res.statusCode, 403);
});

test('resolveTenant returns 400 when all tenant resolution channels are absent', async () => {
  const resolver = createTenantResolver({ lookupTenant: mockLookup });
  const req = { headers: { host: 'localhost:4000' } };
  const res = createMockRes();

  await resolver(req, res, () => {});
  assert.equal(res.statusCode, 400);
});
