const { pool } = require('../config/database');
const { hashPassword, comparePassword, validatePasswordStrength } = require('../utils/password');
const { generateToken } = require('../utils/jwt');

exports.register = async (req, res) => {
  const { firstName, lastName, email, password } = req.body;

  try {
    const passwordValidation = validatePasswordStrength(password);
    if (!passwordValidation.isValid) {
      return res.status(400).json({
        success: false,
        error: 'Слабый пароль',
        details: passwordValidation.errors
      });
    }

    const [existingUsers] = await pool.query(
      'SELECT User_ID FROM user WHERE U_email = ?',
      [email]
    );

    if (existingUsers.length > 0) {
      return res.status(409).json({
        success: false,
        error: 'Пользователь с таким email уже существует'
      });
    }

    const passwordHash = await hashPassword(password);

    const defaultRoleId = 4;
    
    const [departments] = await pool.query(
      'SELECT Department_ID FROM department WHERE D_is_active = TRUE LIMIT 1'
    );
    
    if (departments.length === 0) {
      return res.status(500).json({
        success: false,
        error: 'Не найдено активных отделов'
      });
    }

    const defaultDepartmentId = departments[0].Department_ID;

    const [result] = await pool.query(
      `INSERT INTO user (
        U_login, U_password_hash, U_name, U_surname, U_email, 
        U_hire_date, Role_ID, Department_ID, U_is_active
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        email.split('@')[0],
        passwordHash,
        firstName.trim(),
        lastName.trim(),
        email.toLowerCase(),
        new Date(),
        defaultRoleId,
        defaultDepartmentId,
        true
      ]
    );

    const userId = result.insertId;

    const token = generateToken({
      userId,
      email,
      role: 'employee'
    });

    res.status(201).json({
      success: true,
      message: 'Регистрация успешна',
      data: {
        token,
        user: {
          id: userId,
          firstName,
          lastName,
          email,
          role: 'employee'
        }
      }
    });

  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при регистрации'
    });
  }
};

exports.login = async (req, res) => {
  const { email, password, remember } = req.body;

  try {
    const [users] = await pool.query(
      `SELECT 
        u.User_ID, u.U_login, u.U_password_hash, u.U_name, u.U_surname, 
        u.U_email, u.U_phone, u.U_points_balance, u.U_is_active,
        u.Role_ID, u.Department_ID,
        r.R_name as role_name, r.R_permissions,
        d.D_name as department_name
      FROM user u
      LEFT JOIN role r ON u.Role_ID = r.Role_ID
      LEFT JOIN department d ON u.Department_ID = d.Department_ID
      WHERE u.U_email = ?`,
      [email.toLowerCase()]
    );

    if (users.length === 0) {
      return res.status(401).json({
        success: false,
        error: 'Неверный email или пароль'
      });
    }

    const user = users[0];

    if (!user.U_is_active) {
      return res.status(403).json({
        success: false,
        error: 'Аккаунт деактивирован'
      });
    }

    const isPasswordValid = await comparePassword(password, user.U_password_hash);
    
    if (!isPasswordValid) {
      return res.status(401).json({
        success: false,
        error: 'Неверный email или пароль'
      });
    }

    await pool.query(
      'UPDATE user SET U_last_login = NOW() WHERE User_ID = ?',
      [user.User_ID]
    );

    const expiresIn = remember ? '30d' : '7d';
    const token = generateToken({
      userId: user.User_ID,
      email: user.U_email,
      role: user.role_name
    });

    const userData = {
      id: user.User_ID,
      login: user.U_login,
      firstName: user.U_name,
      lastName: user.U_surname,
      email: user.U_email,
      phone: user.U_phone,
      role: user.role_name,
      permissions: user.R_permissions,
      department: user.department_name,
      pointsBalance: user.U_points_balance
    };

    res.json({
      success: true,
      message: 'Вход выполнен успешно',
      data: {
        token,
        expiresIn,
        user: userData
      }
    });

  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при авторизации'
    });
  }
};

exports.getMe = async (req, res) => {
  try {
    const [users] = await pool.query(
      `SELECT 
        u.User_ID, u.U_login, u.U_name, u.U_surname, u.U_email, 
        u.U_phone, u.U_hire_date, u.U_points_balance,
        r.R_name as role_name, r.R_permissions,
        d.D_name as department_name
      FROM user u
      LEFT JOIN role r ON u.Role_ID = r.Role_ID
      LEFT JOIN department d ON u.Department_ID = d.Department_ID
      WHERE u.User_ID = ?`,
      [req.user.userId]
    );

    if (users.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Пользователь не найден'
      });
    }

    const user = users[0];
    
    res.json({
      success: true,
      data: {
        id: user.User_ID,
        login: user.U_login,
        firstName: user.U_name,
        lastName: user.U_surname,
        email: user.U_email,
        phone: user.U_phone,
        hireDate: user.U_hire_date,
        role: user.role_name,
        permissions: user.R_permissions,
        department: user.department_name,
        pointsBalance: user.U_points_balance
      }
    });

  } catch (error) {
    console.error('GetMe error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера'
    });
  }
};

exports.logout = (req, res) => {
  res.json({
    success: true,
    message: 'Выход выполнен успешно'
  });
};