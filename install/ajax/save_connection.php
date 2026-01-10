<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Application;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

header('Content-Type: application/json');

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$request = Application::getInstance()->getContext()->getRequest();

if (!check_bitrix_sessid()) {
    echo json_encode(['success' => false, 'error' => 'Invalid session']);
    die();
}

$webhookUrl = trim($request->getPost('webhookUrl'));

if (empty($webhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Empty webhook URL']);
    die();
}

if (!filter_var($webhookUrl, FILTER_VALIDATE_URL)) {
    echo json_encode(['success' => false, 'error' => 'Invalid URL format']);
    die();
}

if (!preg_match('#^https?://[^/]+/rest/\d+/[a-zA-Z0-9]+/?$#', $webhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Invalid webhook format. Expected: https://portal.bitrix24.ru/rest/1/abc123/']);
    die();
}

Option::set('bitrix_migrator', 'webhook_url', rtrim($webhookUrl, '/'));

echo json_encode(['success' => true]);