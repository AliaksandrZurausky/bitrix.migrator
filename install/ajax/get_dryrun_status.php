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

$status = Option::get('bitrix_migrator', 'dryrun_status', 'idle');

if ($status === 'completed') {
    $resultJson = Option::get('bitrix_migrator', 'dryrun_result_json', '');
    if ($resultJson) {
        $data = json_decode($resultJson, true);
        echo json_encode(['success' => true, 'status' => 'completed', 'data' => $data]);
    } else {
        echo json_encode(['success' => true, 'status' => 'idle', 'data' => null]);
    }
} elseif ($status === 'error') {
    $error = Option::get('bitrix_migrator', 'dryrun_error', 'Unknown error');
    echo json_encode(['success' => false, 'status' => 'error', 'error' => $error]);
} elseif ($status === 'running') {
    echo json_encode(['success' => true, 'status' => 'running', 'data' => null]);
} else {
    echo json_encode(['success' => true, 'status' => 'idle', 'data' => null]);
}
