<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

header('Content-Type: application/json');

if (!check_bitrix_sessid()) {
    echo json_encode(['success' => false, 'error' => 'Invalid sessid']);
    die();
}

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$moduleId = 'bitrix_migrator';

$currentStatus = Option::get($moduleId, 'migration_status', 'idle');
if (!in_array($currentStatus, ['running', 'paused', 'stopping'])) {
    echo json_encode(['success' => false, 'error' => 'Migration is not running or paused']);
    die();
}

// Set stop flag — the worker checks this on each iteration
Option::set($moduleId, 'migration_stop', '1');

// Try to kill the process if PID is saved
$pid = (int)Option::get($moduleId, 'migration_pid', '0');
if ($pid > 0) {
    // Send SIGTERM for graceful shutdown
    if (function_exists('posix_kill')) {
        posix_kill($pid, 15); // SIGTERM
    }
}

Option::set($moduleId, 'migration_status', 'stopping');
Option::set($moduleId, 'migration_message', 'Остановка миграции...');

echo json_encode(['success' => true, 'message' => 'Stop signal sent', 'pid' => $pid]);
