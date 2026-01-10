<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('NOT_CHECK_PERMISSIONS', true);

require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Loader;
use Bitrix\Main\Application;
use Bitrix\Main\Config\Option;

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

$entities = $request->getPost('entities');
$batchSize = (int)$request->getPost('batchSize');
$priority = $request->getPost('priority');

if (empty($entities) || !is_array($entities)) {
    $response['error'] = 'No entities selected';
    echo json_encode($response);
    die();
}

if ($batchSize < 10 || $batchSize > 500) {
    $response['error'] = 'Invalid batch size';
    echo json_encode($response);
    die();
}

// Save plan
$plan = [
    'entities' => $entities,
    'batchSize' => $batchSize,
    'priority' => $priority ?: $entities,
];

Option::set('bitrix_migrator', 'migration_plan', json_encode($plan));

$response['success'] = true;
echo json_encode($response);
