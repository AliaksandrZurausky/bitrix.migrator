<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

header('Content-Type: application/json');

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$moduleId = 'bitrix_migrator';

$status  = Option::get($moduleId, 'migration_status', 'idle');
$message = Option::get($moduleId, 'migration_message', '');
$stats   = json_decode(Option::get($moduleId, 'migration_stats', '{}'), true);
$log     = json_decode(Option::get($moduleId, 'migration_log', '[]'), true);

echo json_encode([
    'success' => true,
    'status'  => $status,
    'message' => $message,
    'stats'   => $stats,
    'log'     => $log,
]);
