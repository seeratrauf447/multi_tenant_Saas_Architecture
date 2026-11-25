const express = require('express');
const { z } = require('zod');
const { authenticateJwt } = require('../middleware/auth');
const { resolveTenant } = require('../middleware/tenantResolver');
const productRepository = require('../repositories/productRepository');

const router = express.Router();

const createProductSchema = z.object({
  sku: z.string().min(2).max(64),
  name: z.string().min(2).max(150),
  unitPrice: z.number().positive(),
  stock: z.number().int().nonnegative(),
});

router.use(authenticateJwt, resolveTenant);

router.get('/', async (req, res, next) => {
  try {
    const products = await productRepository.listProducts(req.tenant.id);
    return res.json({
      tenant: req.tenant,
      products,
    });
  } catch (err) {
    return next(err);
  }
});

router.post('/', async (req, res, next) => {
  try {
    const payload = createProductSchema.parse(req.body);
    const product = await productRepository.createProduct(req.tenant.id, payload);
    return res.status(201).json({
      tenant: req.tenant,
      product,
    });
  } catch (err) {
    return next(err);
  }
});

module.exports = router;
