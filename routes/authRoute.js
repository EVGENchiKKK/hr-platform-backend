const express = require('express');
const {
  registerValidation,
  loginValidation,
  handleValidationErrors
} = require('../middleware/validator');
const { authenticate } = require('../middleware/auth');
const authController = require('../controllers/authController');

const router = express.Router();

router.post(
  '/register',
  registerValidation,
  handleValidationErrors,
  authController.register
);

router.post(
  '/login',
  loginValidation,
  handleValidationErrors,
  authController.login
);

router.get('/me', authenticate, authController.getMe);

router.post('/logout', authenticate, authController.logout);

module.exports = router;