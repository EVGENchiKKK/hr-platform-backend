const mysql = require('mysql2/promise');
require('dotenv').config();

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  charset: 'utf8mb4'
});

const testConnection = async () => {
  try {
    const connection = await pool.getConnection();
    console.log('✅ Подключение к базе данных успешно');
    connection.release();
    return true;
  } catch (error) {
    console.error('❌ Ошибка подключения к БД:', error.message);
    return false;
  }
};

module.exports = { pool, testConnection };