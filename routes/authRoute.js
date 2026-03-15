const express = require('express');
const {
  registerValidation,
  loginValidation,
  handleValidationErrors
} = require('../middleware/validator');
const { authenticate } = require('../middleware/auth');
const authController = require('../controllers/authController');

const router = express.Router();

/**
 * @route   POST /api/auth/register
 * @desc    Регистрация нового пользователя
 * @access  Public
 */
router.post(
  '/register',
  registerValidation,
  handleValidationErrors,
  authController.register
);

/**
 * @route   POST /api/auth/login
 * @desc    Авторизация пользователя
 * @access  Public
 */
router.post(
  '/login',
  loginValidation,
  handleValidationErrors,
  authController.login
);

/**
 * @route   GET /api/auth/me
 * @desc    Получение данных текущего пользователя
 * @access  Private
 */
router.get('/me', authenticate, authController.getMe);

/**
 * @route   POST /api/auth/logout
 * @desc    Выход из системы
 * @access  Private
 */
router.post('/logout', authenticate, authController.logout);

module.exports = router;