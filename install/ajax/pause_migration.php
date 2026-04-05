<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

global $USER;
if (!$USER->IsAdmin()) {
    echo json_encode(['success' => false, 'error' => 'Access denied']);
    die();
}

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
if ($currentStatus !== 'running') {
    echo json_encode(['success' => false, 'error' => 'Migration is not running']);
    die();
}

Option::set($moduleId, 'migration_stop', 'pause');

echo json_encode(['success' => true, 'message' => 'Pause signal sent']);
