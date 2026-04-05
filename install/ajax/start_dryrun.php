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

use Bitrix\Main\Application;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use BitrixMigrator\Service\DryRunService;

header('Content-Type: application/json');
set_time_limit(300);

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$request = Application::getInstance()->getContext()->getRequest();

if (!check_bitrix_sessid()) {
    echo json_encode(['success' => false, 'error' => 'Invalid session']);
    die();
}

$cloudWebhook = Option::get('bitrix_migrator', 'cloud_webhook_url', '');
if (empty($cloudWebhook)) {
    echo json_encode(['success' => false, 'error' => 'Cloud webhook not configured']);
    die();
}

Option::set('bitrix_migrator', 'dryrun_status', 'running');
Option::set('bitrix_migrator', 'dryrun_result_json', '');

try {
    $data = DryRunService::analyze();
    echo json_encode(['success' => true, 'data' => $data]);
} catch (Exception $e) {
    Option::set('bitrix_migrator', 'dryrun_status', 'error');
    Option::set('bitrix_migrator', 'dryrun_error', $e->getMessage());
    echo json_encode(['success' => false, 'error' => $e->getMessage()]);
}
