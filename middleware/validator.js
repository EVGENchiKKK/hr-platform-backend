const { body, validationResult } = require('express-validator');

// Валидация для регистрации
const registerValidation = [
  body('firstName')
    .trim()
    .notEmpty().withMessage('Имя обязательно')
    .isLength({ min: 2, max: 50 }).withMessage('Имя должно быть от 2 до 50 символов')
    .matches(/^[а-яА-Яa-zA-Z\s-]+$/).withMessage('Имя содержит недопустимые символы'),
  
  body('lastName')
    .trim()
    .notEmpty().withMessage('Фамилия обязательна')
    .isLength({ min: 2, max: 50 }).withMessage('Фамилия должна быть от 2 до 50 символов')
    .matches(/^[а-яА-Яa-zA-Z\s-]+$/).withMessage('Фамилия содержит недопустимые символы'),
  
  body('email')
    .trim()
    .notEmpty().withMessage('Email обязателен')
    .isEmail().withMessage('Некорректный формат email')
    .normalizeEmail(),
  
  body('password')
    .notEmpty().withMessage('Пароль обязателен')
    .isLength({ min: 6 }).withMessage('Пароль должен содержать минимум 6 символов'),
  
  body('confirmPassword')
    .custom((value, { req }) => {
      if (value !== req.body.password) {
        throw new Error('Пароли не совпадают');
      }
      return true;
    }),
  
  body('agreeTerms')
    .equals('true').withMessage('Необходимо согласиться с условиями использования')
];

// Валидация для входа
const loginValidation = [
  body('email')
    .trim()
    .notEmpty().withMessage('Email обязателен')
    .isEmail().withMessage('Некорректный формат email')
    .normalizeEmail(),
  
  body('password')
    .notEmpty().withMessage('Пароль обязателен')
];

// Обработка ошибок валидации
const handleValidationErrors = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      success: false,
      error: 'Ошибка валидации',
      details: errors.array().map(err => ({
        field: err.path,
        message: err.msg
      }))
    });
  }
  next();
};

module.exports = {
  registerValidation,
  loginValidation,
  handleValidationErrors
};