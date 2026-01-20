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

$type = trim($request->getPost('type'));

if (!in_array($type, ['cloud', 'box'], true)) {
    echo json_encode(['success' => false, 'error' => 'Invalid type parameter']);
    die();
}

// Get webhook URL from settings
$webhookUrl = Option::get('bitrix_migrator', $type . '_webhook_url', '');

if (empty($webhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Webhook URL not configured']);
    die();
}

$apiUrl = rtrim($webhookUrl, '/') . '/user.current.json';

try {
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $apiUrl);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_TIMEOUT, 30);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($error) {
        throw new Exception('cURL error: ' . $error);
    }

    if ($httpCode !== 200) {
        throw new Exception('HTTP error: ' . $httpCode);
    }

    $data = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new Exception('JSON decode error: ' . json_last_error_msg());
    }

    if (isset($data['result']) && isset($data['result']['ID'])) {
        Option::set('bitrix_migrator', 'connection_status_' . $type, 'success');
        echo json_encode(['success' => true, 'user' => $data['result']]);
    } else {
        Option::set('bitrix_migrator', 'connection_status_' . $type, 'error');
        $errorMsg = $data['error_description'] ?? 'Unknown error';
        echo json_encode(['success' => false, 'error' => $errorMsg]);
    }
} catch (Exception $e) {
    Option::set('bitrix_migrator', 'connection_status_' . $type, 'error');
    echo json_encode(['success' => false, 'error' => $e->getMessage()]);}
