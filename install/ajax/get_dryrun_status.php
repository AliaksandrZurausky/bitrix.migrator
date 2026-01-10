<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('NOT_CHECK_PERMISSIONS', true);

require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Loader;
use Bitrix\Main\Application;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Type\DateTime;

header('Content-Type: application/json');

$request = Application::getInstance()->getContext()->getRequest();
$response = ['success' => false];

if (!check_bitrix_sessid() || !$request->isPost()) {
    $response['error'] = 'Invalid request';
    echo json_encode($response);
    die();
}

if (!Loader::includeModule('bitrix_migrator')) {
    $response['error'] = 'Module not loaded';
    echo json_encode($response);
    die();
}

$status = Option::get('bitrix_migrator', 'dryrun_status', 'idle');
$progress = (int)Option::get('bitrix_migrator', 'dryrun_progress', 0);
$error = Option::get('bitrix_migrator', 'dryrun_error', '');

$response['success'] = true;
$response['status'] = $status;
$response['progress'] = $progress;
$response['error'] = $error;

// If completed, get results
if ($status === 'completed') {
    $resultsJson = Option::get('bitrix_migrator', 'dryrun_results', '');
    if ($resultsJson) {
        $response['results'] = json_decode($resultsJson, true);
    }
}

echo json_encode($response);
