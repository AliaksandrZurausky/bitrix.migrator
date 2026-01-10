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

// Check if dry run already in progress
$status = Option::get('bitrix_migrator', 'dryrun_status', 'idle');
if ($status === 'running') {
    $response['error'] = 'Dry run already in progress';
    echo json_encode($response);
    die();
}

// Set status to running
Option::set('bitrix_migrator', 'dryrun_status', 'running');
Option::set('bitrix_migrator', 'dryrun_started_at', (new DateTime())->toString());
Option::set('bitrix_migrator', 'dryrun_progress', '0');

// Add agent
\CAgent::AddAgent(
    '\\BitrixMigrator\\Agent\\DryRunAgent::run();',
    'bitrix_migrator',
    'N',
    60,
    '',
    'Y',
    '',
    1
);

$response['success'] = true;
echo json_encode($response);
