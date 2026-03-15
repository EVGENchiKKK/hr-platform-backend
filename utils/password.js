const bcrypt = require('bcryptjs');

const SALT_ROUNDS = 10;

const hashPassword = async (password) => {
  return await bcrypt.hash(password, SALT_ROUNDS);
};

const comparePassword = async (password, hash) => {
  return await bcrypt.compare(password, hash);
};

const validatePasswordStrength = (password) => {
  const errors = [];
  
  if (password.length < 6) {
    errors.push('Пароль должен содержать минимум 6 символов');
  }
  if (!/[A-Z]/.test(password)) {
    errors.push('Пароль должен содержать заглавную букву');
  }
  if (!/[a-z]/.test(password)) {
    errors.push('Пароль должен содержать строчную букву');
  }
  if (!/[0-9]/.test(password)) {
    errors.push('Пароль должен содержать цифру');
  }
  
  return {
    isValid: errors.length === 0,
    errors
  };
};

module.exports = {
  hashPassword,
  comparePassword,
  validatePasswordStrength
};