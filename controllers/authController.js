const { pool } = require('../config/database');
const { hashPassword, comparePassword, validatePasswordStrength } = require('../utils/password');
const { generateToken } = require('../utils/jwt');

const buildUserResponse = (user) => ({
  id: user.User_ID,
  login: user.U_login,
  firstName: user.U_name,
  lastName: user.U_surname,
  middleName: user.U_lastname,
  email: user.U_email,
  phone: user.U_phone,
  hireDate: user.U_hire_date,
  roleId: user.Role_ID,
  role: user.role_name,
  permissions: user.R_permissions,
  departmentId: user.Department_ID,
  department: user.department_name,
  pointsBalance: user.U_points_balance
});

exports.getRegisterMeta = async (req, res) => {
  try {
    const [roles] = await pool.query(
      `SELECT Role_ID, R_name, R_description
       FROM role
       ORDER BY R_name`
    );

    const [departments] = await pool.query(
      `SELECT Department_ID, D_name, D_code
       FROM department
       WHERE D_is_active = TRUE
       ORDER BY D_name`
    );

    res.json({
      success: true,
      data: {
        roles: roles.map((role) => ({
          id: role.Role_ID,
          name: role.R_name,
          description: role.R_description
        })),
        departments: departments.map((department) => ({
          id: department.Department_ID,
          name: department.D_name,
          code: department.D_code
        }))
      }
    });
  } catch (error) {
    console.error('Get register meta error:', error);
    res.status(500).json({
      success: false,
      error: 'Ошибка сервера при загрузке данных для регистрации'
    });
  }
};

exports.register = async (req, res) => {
  const {
    login,
    firstName,
    lastName,
    middleName,
    email,
    phone,
    hireDate,
    roleId,
    departmentId,
    password
  } = req.body;

  try {
    const passwordValidation = validatePasswordStrength(password);
    if (!passwordValidation.isValid) {
      return res.status(400).json({
        success: false,
        error: 'Слабый пароль',
        details: passwordValidation.errors
      });
    }

    const normalizedEmail = email.toLowerCase();
    const normalizedLogin = login.trim().toLowerCase();
    const normalizedFirstName = firstName.trim();
    const normalizedLastName = lastName.trim();
    const normalizedMiddleName = middleName?.trim() || null;
    const normalizedPhone = phone?.trim() || null;

    const [existingUsers] = await pool.query(
      'SELECT User_ID, U_email, U_login FROM user WHERE U_email = ? OR U_login = ?',
      [normalizedEmail, normalizedLogin]
    );

    if (existingUsers.length > 0) {
      const existingByEmail = existingUsers.find((user) => user.U_email === normalizedEmail);
      if (existingByEmail) {
        return res.status(409).json({
          success: false,
          error: 'Пользователь с таким email уже существует'
        });
      }

      return res.status(409).json({
        success: false,
        error: 'Пользователь с таким логином уже существует'
      });
    }

    const [[roleRows], [departmentRows]] = await Promise.all([
      pool.query(
        'SELECT Role_ID, R_name FROM role WHERE Role_ID = ? LIMIT 1',
        [roleId]
      ),
      pool.query(
        'SELECT Department_ID, D_name FROM department WHERE Department_ID = ? AND D_is_active = TRUE LIMIT 1',
        [departmentId]
      )
    ]);

    if (roleRows.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Выбранная роль не найдена'
      });
    }

    if (departmentRows.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Выбранный отдел не найден'
      });
    }

    const passwordHash = await hashPassword(password);

    const [result] = await pool.query(
      `INSERT INTO user (
        U_login, U_password_hash, U_name, U_surname, U_lastname, U_email,
        U_phone, U_hire_date, Role_ID, Department_ID, U_is_active
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        normalizedLogin,
        passwordHash,
        normalizedFirstName,
        normalizedLastName,
        normalizedMiddleName,
        normalizedEmail,
        normalizedPhone,
        hireDate,
        roleId,
        departmentId,
        true
      ]
    );

    const [[users]] = await Promise.all([
      pool.query(
        `SELECT
          u.User_ID, u.U_login, u.U_name, u.U_surname, u.U_lastname, u.U_email,
          u.U_phone, u.U_hire_date, u.U_points_balance, u.U_is_active,
          u.Role_ID, u.Department_ID,
          r.R_name AS role_name, r.R_permissions,
          d.D_name AS department_name
        FROM user u
        LEFT JOIN role r ON u.Role_ID = r.Role_ID
        LEFT JOIN department d ON u.Department_ID = d.Department_ID
        WHERE u.User_ID = ?`,
        [result.insertId]
      )
    ]);

    const createdUser = users[0];
    const token = generateToken({
      userId: createdUser.User_ID,
      email: createdUser.U_email,
      role: createdUser.role_name
    });

    res.status(201).json({
      success: true,
      message: 'Регистрация успешна',
      data: {
        token,
        user: buildUserResponse(createdUser)
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
        u.User_ID, u.U_login, u.U_password_hash, u.U_name, u.U_surname, u.U_lastname,
        u.U_email, u.U_phone, u.U_hire_date, u.U_points_balance, u.U_is_active,
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

    res.json({
      success: true,
      message: 'Вход выполнен успешно',
      data: {
        token,
        expiresIn,
        user: buildUserResponse(user)
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
        u.User_ID, u.U_login, u.U_name, u.U_surname, u.U_lastname, u.U_email,
        u.U_phone, u.U_hire_date, u.U_points_balance,
        u.Role_ID, u.Department_ID,
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

    res.json({
      success: true,
      data: buildUserResponse(users[0])
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
