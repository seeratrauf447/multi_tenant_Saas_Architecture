const express = require('express');
const { z } = require('zod');
const { authenticateJwt } = require('../middleware/auth');
const { resolveTenant } = require('../middleware/tenantResolver');
const { UUID_REGEX } = require('../config/db');
const orderRepository = require('../repositories/orderRepository');
const { defaultOrderService } = require('../services/orderService');
const { runNPlusOneBenchmark } = require('../services/benchmarkService');

const router = express.Router();

const createOrderSchema = z.object({
  items: z
    .array(
      z.object({
        productId: z.string().uuid(),
        quantity: z.number().int().positive().max(1000),
      })
    )
    .min(1),
});

router.use(authenticateJwt, resolveTenant);

/**
 * POST /api/v1/orders
 * Publishes OrderCreated event to Kafka ('tenant.orders') partitioned by tenant_id
 * and immediately returns 202 Accepted.
 */
router.post('/', async (req, res, next) => {
  try {
    const { items } = createOrderSchema.parse(req.body);
    const result = await defaultOrderService.placeOrder({
      tenant: req.tenant,
      user: req.user,
      items,
    });
    return res.status(202).json(result);
  } catch (err) {
    return next(err);
  }
});

/**
 * GET /api/v1/orders
 * Optimized single-JOIN query scoped strictly to req.tenant.id.
 */
router.get('/', async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit || '25', 10) || 25, 1), 100);
    const result = await orderRepository.listOrdersOptimized(req.tenant.id, limit);
    return res.json({
      tenant: req.tenant,
      ...result,
    });
  } catch (err) {
    return next(err);
  }
});

/**
 * GET /api/v1/orders/benchmark/n-plus-1
 * Benchmarks un-optimized N+1 query vs optimized single-JOIN query for the active tenant.
 */
router.get('/benchmark/n-plus-1', async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit || '25', 10) || 25, 1), 100);
    const benchmark = await runNPlusOneBenchmark(req.tenant.id, limit);
    return res.json(benchmark);
  } catch (err) {
    return next(err);
  }
});

/**
 * GET /api/v1/orders/:orderId
 * Strict tenant boundary: returns 404 if order belongs to another tenant.
 */
router.get('/:orderId', async (req, res, next) => {
  try {
    const { orderId } = req.params;
    if (!UUID_REGEX.test(orderId)) {
      return res.status(400).json({ error: 'Invalid orderId UUID' });
    }
    const order = await orderRepository.getOrderById(req.tenant.id, orderId);
    if (!order) {
      return res.status(404).json({
        error: 'Order not found',
        message: `Order ${orderId} does not exist in tenant "${req.tenant.slug}".`,
      });
    }
    return res.json({ tenant: req.tenant, order });
  } catch (err) {
    return next(err);
  }
});

module.exports = router;
