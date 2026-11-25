const { AsyncLocalStorage } = require('node:async_hooks');
const { pool, UUID_REGEX } = require('../config/db');

const tenantContextStore = new AsyncLocalStorage();

function getActiveTenantContext() {
  return tenantContextStore.getStore() || null;
}

/**
 * Extracts a tenant subdomain from Host / X-Forwarded-Host headers.
 * Examples:
 *   acme-corp.localhost:4000 -> acme-corp
 *   globex-inc.saas.vercel.app -> globex-inc
 *   localhost:4000 -> null
 */
function extractSubdomainTenant(hostHeader) {
  if (!hostHeader) return null;
  const hostname = hostHeader.split(':')[0].trim().toLowerCase();
  if (!hostname || hostname === 'localhost' || /^\d+\.\d+\.\d+\.\d+$/.test(hostname)) {
    return null;
  }

  const parts = hostname.split('.');
  // e.g. acme-corp.localhost (2 parts) or acme-corp.domain.com (3+ parts)
  if (parts.length === 2 && parts[1] === 'localhost') {
    return parts[0];
  }
  if (parts.length >= 3) {
    const candidate = parts[0];
    if (!['www', 'api', 'app'].includes(candidate)) {
      return candidate;
    }
  }
  return null;
}

async function defaultLookupTenant(identifier) {
  if (!identifier) return null;
  const isUuid = UUID_REGEX.test(identifier);
  const query = isUuid
    ? 'SELECT id, slug, name, plan FROM tenants WHERE id = $1 LIMIT 1'
    : 'SELECT id, slug, name, plan FROM tenants WHERE slug = $1 LIMIT 1';
  const { rows } = await pool.query(query, [identifier]);
  return rows[0] || null;
}

/**
 * Factory for Tenant Resolution Middleware (Lecture 18).
 * Supports resolving tenant_id from:
 *  1. JWT claim (req.user.tenantId)
 *  2. Custom header (x-tenant-id: UUID or slug)
 *  3. Subdomain (e.g. acme-corp.localhost)
 *
 * Enforces strict tenant consistency: if JWT and header/subdomain disagree, returns 403 Forbidden.
 */
function createTenantResolver({ lookupTenant = defaultLookupTenant } = {}) {
  return async function resolveTenant(req, res, next) {
    try {
      const jwtTenantId = req.user?.tenantId || null;
      const headerTenantRaw = req.headers['x-tenant-id']
        ? String(req.headers['x-tenant-id']).trim()
        : null;
      const subdomainTenantRaw = extractSubdomainTenant(
        req.headers['x-forwarded-host'] || req.headers.host
      );

      if (!jwtTenantId && !headerTenantRaw && !subdomainTenantRaw) {
        return res.status(400).json({
          error: 'Tenant context missing',
          message:
            'Provide tenant context via JWT claim, x-tenant-id header, or tenant subdomain.',
        });
      }

      const resolvedSources = [];

      if (jwtTenantId) {
        const tenant = await lookupTenant(jwtTenantId);
        if (!tenant) {
          return res.status(403).json({
            error: 'Invalid tenant in JWT claim',
          });
        }
        resolvedSources.push({ source: 'jwt', tenant });
      }

      if (headerTenantRaw) {
        const tenant = await lookupTenant(headerTenantRaw);
        if (!tenant) {
          return res.status(404).json({
            error: 'Unknown tenant in x-tenant-id header',
            tenantIdentifier: headerTenantRaw,
          });
        }
        resolvedSources.push({ source: 'x-tenant-id', tenant });
      }

      if (subdomainTenantRaw) {
        const tenant = await lookupTenant(subdomainTenantRaw);
        if (tenant) {
          resolvedSources.push({ source: 'subdomain', tenant });
        }
      }

      if (resolvedSources.length === 0) {
        return res.status(404).json({
          error: 'Tenant not found',
        });
      }

      const primary = resolvedSources[0];
      for (const entry of resolvedSources.slice(1)) {
        if (entry.tenant.id !== primary.tenant.id) {
          return res.status(403).json({
            error: 'Cross-tenant boundary violation',
            message: `Tenant mismatch between ${primary.source} (${primary.tenant.slug}) and ${entry.source} (${entry.tenant.slug}).`,
          });
        }
      }

      const tenantContext = {
        ...primary.tenant,
        resolvedVia: resolvedSources.map((s) => s.source),
      };

      req.tenant = tenantContext;
      res.setHeader('x-resolved-tenant-id', tenantContext.id);
      res.setHeader('x-resolved-tenant-slug', tenantContext.slug);

      return tenantContextStore.run(tenantContext, () => next());
    } catch (err) {
      return next(err);
    }
  };
}

const resolveTenant = createTenantResolver();

module.exports = {
  createTenantResolver,
  resolveTenant,
  extractSubdomainTenant,
  getActiveTenantContext,
  tenantContextStore,
};
