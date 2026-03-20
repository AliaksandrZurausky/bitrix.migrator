<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use BitrixMigrator\Integration\CloudAPI;
use BitrixMigrator\Service\TaskMigrationService;

header('Content-Type: application/json');

set_time_limit(0);
ignore_user_abort(true);

if (!check_bitrix_sessid()) {
    echo json_encode(['success' => false, 'error' => 'Invalid sessid']);
    die();
}

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$moduleId = 'bitrix_migrator';

$cloudWebhookUrl = Option::get($moduleId, 'cloud_webhook_url', '');
$boxWebhookUrl   = Option::get($moduleId, 'box_webhook_url', '');

if (empty($cloudWebhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Cloud webhook URL not configured']);
    die();
}

if (empty($boxWebhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Box webhook URL not configured']);
    die();
}

// Check if already running
$currentStatus = Option::get($moduleId, 'migration_status', 'idle');
if ($currentStatus === 'running') {
    echo json_encode(['success' => false, 'error' => 'Migration already running']);
    die();
}

$cloudAPI = new CloudAPI($cloudWebhookUrl);
$boxAPI   = new CloudAPI($boxWebhookUrl);

$planJson = Option::get($moduleId, 'migration_plan', '');
$plan     = $planJson ? json_decode($planJson, true) : [];

// Determine what to migrate
$migrateType = $_POST['type'] ?? 'tasks';

// Send response immediately, continue in background
echo json_encode(['success' => true, 'message' => 'Migration started']);

if (function_exists('fastcgi_finish_request')) {
    fastcgi_finish_request();
} else {
    ob_end_flush();
    flush();
}

// Run migration
try {
    if ($migrateType === 'tasks') {
        $service = new TaskMigrationService($cloudAPI, $boxAPI, $plan);
        $service->migrate();
    }
} catch (\Exception $e) {
    Option::set($moduleId, 'migration_status', 'error');
    Option::set($moduleId, 'migration_message', $e->getMessage());
}
