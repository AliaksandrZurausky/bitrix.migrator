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

$planJson = $_POST['plan'] ?? '';
if (empty($planJson)) {
    echo json_encode(['success' => false, 'error' => 'Empty plan']);
    die();
}

$plan = json_decode($planJson, true);
if (!is_array($plan)) {
    echo json_encode(['success' => false, 'error' => 'Invalid plan JSON']);
    die();
}

Option::set('bitrix_migrator', 'migration_plan', json_encode($plan, JSON_UNESCAPED_UNICODE));
Option::set('bitrix_migrator', 'plan_saved_at', (string)time());

echo json_encode(['success' => true]);
