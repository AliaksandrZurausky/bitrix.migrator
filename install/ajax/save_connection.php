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

$cloudWebhookUrl = trim($request->getPost('cloud_webhook_url'));
$boxWebhookUrl = trim($request->getPost('box_webhook_url'));

if (empty($cloudWebhookUrl) && empty($boxWebhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'At least one webhook URL required']);
    die();
}

if (!empty($cloudWebhookUrl)) {
    if (!filter_var($cloudWebhookUrl, FILTER_VALIDATE_URL)) {
        echo json_encode(['success' => false, 'error' => 'Invalid cloud webhook URL format']);
        die();
    }
    if (!preg_match('#^https?://[^/]+/rest/\d+/[a-zA-Z0-9]+/?$#', $cloudWebhookUrl)) {
        echo json_encode(['success' => false, 'error' => 'Invalid cloud webhook format. Expected: https://portal.bitrix24.ru/rest/1/abc123/']);
        die();
    }
    Option::set('bitrix_migrator', 'cloud_webhook_url', rtrim($cloudWebhookUrl, '/'));
}

if (!empty($boxWebhookUrl)) {
    if (!filter_var($boxWebhookUrl, FILTER_VALIDATE_URL)) {
        echo json_encode(['success' => false, 'error' => 'Invalid box webhook URL format']);
        die();
    }
    if (!preg_match('#^https?://[^/]+/rest/\d+/[a-zA-Z0-9]+/?$#', $boxWebhookUrl)) {
        echo json_encode(['success' => false, 'error' => 'Invalid box webhook format. Expected: https://portal.bitrix24.ru/rest/1/abc123/']);
        die();
    }
    Option::set('bitrix_migrator', 'box_webhook_url', rtrim($boxWebhookUrl, '/'));
}

echo json_encode(['success' => true]);
