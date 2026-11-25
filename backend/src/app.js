const fs = require('node:fs');
const path = require('node:path');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const config = require('./config/env');
const { errorHandler } = require('./middleware/errorHandler');

const healthRoutes = require('./routes/healthRoutes');
const authRoutes = require('./routes/authRoutes');
const productRoutes = require('./routes/productRoutes');
const orderRoutes = require('./routes/orderRoutes');
const diagnosticsRoutes = require('./routes/diagnosticsRoutes');
const invoiceWorkerHandler = require('../../api/invoice-worker');

const app = express();

app.use(
  helmet({
    contentSecurityPolicy: false,
  })
);
app.use(
  cors({
    origin: config.corsOrigin === '*' ? true : config.corsOrigin.split(',').map((s) => s.trim()),
    credentials: true,
    allowedHeaders: ['Content-Type', 'Authorization', 'x-tenant-id'],
    exposedHeaders: ['x-resolved-tenant-id', 'x-resolved-tenant-slug'],
  })
);
app.use(express.json({ limit: '1mb' }));

app.use('/api/v1/health', healthRoutes);
app.use('/api/v1', authRoutes);
app.use('/api/v1/products', productRoutes);
app.use('/api/v1/orders', orderRoutes);
app.use('/api/v1/diagnostics', diagnosticsRoutes);

// Local mirror of the Vercel Serverless Function (/api/invoice-worker)
app.all('/api/invoice-worker', (req, res) => invoiceWorkerHandler(req, res));

// Optional static frontend serving for single-service Render/Railway deployment
const frontendDistPath = path.resolve(__dirname, '../../frontend/dist');
if (config.serveFrontend && fs.existsSync(frontendDistPath)) {
  app.use(express.static(frontendDistPath));
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api/')) return next();
    return res.sendFile(path.join(frontendDistPath, 'index.html'));
  });
}

app.use('/api/*', (_req, res) => {
  res.status(404).json({ error: 'API route not found' });
});

app.use(errorHandler);

module.exports = app;
