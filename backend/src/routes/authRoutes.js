const express = require('express');
const bcrypt = require('bcryptjs');
const { z } = require('zod');
const { pool, withSystemTransaction, UUID_REGEX } = require('../config/db');
const { authenticateJwt, signUserToken } = require('../middleware/auth');
const { resolveTenant, extractSubdomainTenant } = require('../middleware/tenantResolver');

const router = express.Router();

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1),
  tenantSlug: z.string().min(1).optional(),
});

router.get('/tenants', async (_req, res, next) => {
  try {
    const { rows } = await pool.query(
      'SELECT id, slug, name, plan, created_at FROM tenants ORDER BY slug ASC'
    );
    return res.json({ tenants: rows });
  } catch (err) {
    return next(err);
  }
});

router.post('/auth/login', async (req, res, next) => {
  try {
    const { email, password, tenantSlug } = loginSchema.parse(req.body);
    const headerTenant = req.headers['x-tenant-id']
      ? String(req.headers['x-tenant-id']).trim()
      : null;
    const subdomainTenant = extractSubdomainTenant(
      req.headers['x-forwarded-host'] || req.headers.host
    );
    const tenantIdentifier = tenantSlug || headerTenant || subdomainTenant;

    const authResult = await withSystemTransaction(async (client) => {
      let tenant = null;

      if (tenantIdentifier) {
        const isUuid = UUID_REGEX.test(tenantIdentifier);
        const { rows: tRows } = await client.query(
          isUuid
            ? 'SELECT id, slug, name, plan FROM tenants WHERE id = $1 LIMIT 1'
            : 'SELECT id, slug, name, plan FROM tenants WHERE slug = $1 LIMIT 1',
          [tenantIdentifier]
        );
        tenant = tRows[0] || null;
        if (!tenant) {
          return { errorStatus: 404, errorMessage: 'Tenant not found.' };
        }
      }

      const userQuery = tenant
        ? 'SELECT id, tenant_id, email, password_hash, full_name, role FROM users WHERE tenant_id = $1 AND LOWER(email) = LOWER($2) LIMIT 1'
        : 'SELECT id, tenant_id, email, password_hash, full_name, role FROM users WHERE LOWER(email) = LOWER($1) LIMIT 1';
      const userParams = tenant ? [tenant.id, email] : [email];
      const { rows: uRows } = await client.query(userQuery, userParams);
      const user = uRows[0];

      if (!user) {
        return { errorStatus: 401, errorMessage: 'Invalid email or password.' };
      }

      const passwordValid = await bcrypt.compare(password, user.password_hash);
      if (!passwordValid) {
        return { errorStatus: 401, errorMessage: 'Invalid email or password.' };
      }

      if (!tenant) {
        const { rows: tRows } = await client.query(
          'SELECT id, slug, name, plan FROM tenants WHERE id = $1 LIMIT 1',
          [user.tenant_id]
        );
        tenant = tRows[0];
      }

      return { user, tenant };
    });

    if (authResult.errorStatus) {
      return res.status(authResult.errorStatus).json({
        error: 'Authentication failed',
        message: authResult.errorMessage,
      });
    }

    const token = signUserToken(authResult.user, authResult.tenant);
    return res.json({
      token,
      user: {
        id: authResult.user.id,
        email: authResult.user.email,
        fullName: authResult.user.full_name,
        role: authResult.user.role,
        tenantId: authResult.tenant.id,
      },
      tenant: authResult.tenant,
    });
  } catch (err) {
    return next(err);
  }
});

router.get('/auth/me', authenticateJwt, resolveTenant, (req, res) => {
  res.json({
    user: req.user,
    tenant: req.tenant,
  });
});

module.exports = router;
