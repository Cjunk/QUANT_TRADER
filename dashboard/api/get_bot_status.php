<?php
// api/get_bot_status.php

header('Content-Type: application/json');
require_once '../config/db_config.php';

try {
  $pdo = get_db_connection();

  $sql = "SELECT bot_name, status, started_at, last_updated, metadata
              FROM " . DB_SCHEMA . ".bots
              ORDER BY last_updated DESC";

  $stmt = $pdo->query($sql);
  $bots = $stmt->fetchAll(PDO::FETCH_ASSOC);

  $formattedBots = array_map(function ($bot) {
    $meta = json_decode($bot['metadata'], true) ?? [];
    return [
      'bot_name'     => $bot['bot_name'],
      'status'       => $bot['status'],
      'started_at'   => $bot['started_at'],
      'last_updated' => $bot['last_updated'],
      'pid'          => $meta['pid'] ?? null,
      'version'      => $meta['version'] ?? null,
      'strategy'     => $meta['strategy'] ?? null,
      'vitals'       => $meta['vitals'] ?? null // <-- This will only work if you store vitals in metadata!
    ];
  }, $bots);

  echo json_encode(['success' => true, 'bots' => $formattedBots], JSON_PRETTY_PRINT);
} catch (Exception $e) {
  echo json_encode([
    'success' => false,
    'message' => 'Database error',
    'error' => $e->getMessage()
  ], JSON_PRETTY_PRINT);
}
