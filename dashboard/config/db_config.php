<?php
// config/db_config.php

define('DB_HOST', 'localhost');
define('DB_PORT', 5433);
define('DB_NAME', 'Quant_Trading');
define('DB_USER', 'pb_bot_user');  // 🔐 Use restricted user
define('DB_PASSWORD', 'test');
define('DB_SCHEMA', 'trading');

function get_db_connection()
{
  try {
    $dsn = "pgsql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME;
    return new PDO($dsn, DB_USER, DB_PASSWORD, [
      PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
  } catch (PDOException $e) {
    throw new Exception("Database connection failed: " . $e->getMessage());
  }
}
