const jwt = require('jsonwebtoken');
const config = require('../config/env');

function extractBearerToken(req) {
  const authHeader = req.headers.authorization || '';
  if (!authHeader.startsWith('Bearer ')) return null;
  return authHeader.slice(7).trim();
}

function authenticateJwt(req, res, next) {
  const token = extractBearerToken(req);
  if (!token) {
    return res.status(401).json({
      error: 'Authentication required',
      message: 'Missing Bearer JWT in Authorization header.',
    });
  }

  try {
    const decoded = jwt.verify(token, config.jwtSecret);
    req.user = {
      userId: decoded.sub,
      email: decoded.email,
      fullName: decoded.fullName,
      role: decoded.role,
      tenantId: decoded.tenantId,
      tenantSlug: decoded.tenantSlug,
    };
    return next();
  } catch (err) {
    return res.status(401).json({
      error: 'Invalid or expired token',
      message: err.message,
    });
  }
}

function signUserToken(user, tenant) {
  return jwt.sign(
    {
      sub: user.id,
      email: user.email,
      fullName: user.full_name,
      role: user.role,
      tenantId: tenant.id,
      tenantSlug: tenant.slug,
    },
    config.jwtSecret,
    { expiresIn: config.jwtExpiresIn }
  );
}

module.exports = {
  authenticateJwt,
  signUserToken,
  extractBearerToken,
};
