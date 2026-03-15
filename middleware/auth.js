const { verifyToken, extractToken } = require('../utils/jwt');
const { pool } = require('../config/database');

/**
 * Middleware для проверки аутентификации
 */
const authenticate = async (req, res, next) => {
  try {
    const token = extractToken(req.headers.authorization);
    
    if (!token) {
      return res.status(401).json({
        success: false,
        error: 'Требуется авторизация'
      });
    }

    const decoded = verifyToken(token);
    
    if (!decoded) {
      return res.status(401).json({
        success: false,
        error: 'Неверный или истёкший токен'
      });
    }

    // Проверка существования пользователя в БД
    const [users] = await pool.query(
      'SELECT User_ID, U_is_active, Role_ID FROM user WHERE User_ID = ?',
      [decoded.userId]
    );

    if (users.length === 0) {
      return res.status(401).json({
        success: false,
        error: 'Пользователь не найден'
      });
    }

    if (!users[0].U_is_active) {
      return res.status(403).json({
        success: false,
        error: 'Аккаунт деактивирован'
      });
    }

    // Добавляем пользователя в запрос
    req.user = {
      userId: users[0].User_ID,
      roleId: users[0].Role_ID,
      ...decoded
    };

    next();
  } catch (error) {
    console.error('Auth middleware error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера'
    });
  }
};

/**
 * Middleware для проверки роли
 */
const requireRole = (...allowedRoles) => {
  return (req, res, next) => {
    if (!req.user || !allowedRoles.includes(req.user.roleName)) {
      return res.status(403).json({
        success: false,
        error: 'Недостаточно прав'
      });
    }
    next();
  };
};

module.exports = {
  authenticate,
  requireRole
};