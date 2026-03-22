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
if ($currentStatus !== 'paused') {
    echo json_encode(['success' => false, 'error' => 'Migration is not paused']);
    die();
}

// Clear pause flag — the sleeping checkStop() loop will pick this up
Option::set($moduleId, 'migration_stop', '0');

echo json_encode(['success' => true, 'message' => 'Resume signal sent']);
