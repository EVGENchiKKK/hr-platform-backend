const { body, validationResult } = require('express-validator');

const namePattern = /^[A-Za-zА-Яа-яЁё\s-]+$/;
const phonePattern = /^\+?[0-9()\-\s]{7,20}$/;
const loginPattern = /^[a-zA-Z0-9._-]{3,50}$/;

const registerValidation = [
  body('login')
    .trim()
    .notEmpty().withMessage('Логин обязателен')
    .isLength({ min: 3, max: 50 }).withMessage('Логин должен быть от 3 до 50 символов')
    .matches(loginPattern).withMessage('Логин может содержать только латинские буквы, цифры, точку, дефис и нижнее подчёркивание'),

  body('firstName')
    .trim()
    .notEmpty().withMessage('Имя обязательно')
    .isLength({ min: 2, max: 50 }).withMessage('Имя должно быть от 2 до 50 символов')
    .matches(namePattern).withMessage('Имя содержит недопустимые символы'),

  body('lastName')
    .trim()
    .notEmpty().withMessage('Фамилия обязательна')
    .isLength({ min: 2, max: 50 }).withMessage('Фамилия должна быть от 2 до 50 символов')
    .matches(namePattern).withMessage('Фамилия содержит недопустимые символы'),

  body('middleName')
    .optional({ values: 'falsy' })
    .trim()
    .isLength({ max: 50 }).withMessage('Отчество должно быть не длиннее 50 символов')
    .matches(namePattern).withMessage('Отчество содержит недопустимые символы'),

  body('email')
    .trim()
    .notEmpty().withMessage('Email обязателен')
    .isEmail().withMessage('Некорректный формат email')
    .normalizeEmail(),

  body('phone')
    .optional({ values: 'falsy' })
    .trim()
    .matches(phonePattern).withMessage('Некорректный формат телефона'),

  body('hireDate')
    .notEmpty().withMessage('Дата приёма на работу обязательна')
    .isISO8601({ strict: true, strictSeparator: true }).withMessage('Некорректная дата приёма на работу'),

  body('roleId')
    .notEmpty().withMessage('Роль обязательна')
    .isInt({ min: 1 }).withMessage('Некорректная роль')
    .toInt(),

  body('departmentId')
    .notEmpty().withMessage('Отдел обязателен')
    .isInt({ min: 1 }).withMessage('Некорректный отдел')
    .toInt(),

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
    .custom((value) => value === true || value === 'true')
    .withMessage('Необходимо согласиться с условиями использования')
];

const loginValidation = [
  body('email')
    .trim()
    .notEmpty().withMessage('Email обязателен')
    .isEmail().withMessage('Некорректный формат email')
    .normalizeEmail(),

  body('password')
    .notEmpty().withMessage('Пароль обязателен')
];

const handleValidationErrors = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      success: false,
      error: 'Ошибка валидации',
      details: errors.array().map((err) => ({
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
