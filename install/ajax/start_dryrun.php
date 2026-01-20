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

$cloudWebhook = Option::get('bitrix_migrator', 'cloud_webhook_url', '');

if (empty($cloudWebhook)) {
    echo json_encode(['success' => false, 'error' => 'Cloud webhook not configured']);
    die();
}

// Fetch departments from cloud
try {
    $apiUrl = rtrim($cloudWebhook, '/') . '/department.get.json';
    
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $apiUrl);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_TIMEOUT, 60);
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
        throw new Exception('HTTP error code: ' . $httpCode);
    }

    $data = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new Exception('JSON decode error: ' . json_last_error_msg());
    }

    if (!isset($data['result'])) {
        throw new Exception('Invalid API response: ' . ($data['error_description'] ?? 'Unknown error'));
    }

    $departments = $data['result'];

    // Save results
    $migrationPlan = [
        'departments' => $departments,
        'timestamp' => time(),
        'stats' => [
            'total_departments' => count($departments)
        ]
    ];

    Option::set('bitrix_migrator', 'migration_plan', json_encode($migrationPlan, JSON_UNESCAPED_UNICODE));
    Option::set('bitrix_migrator', 'dryrun_status', 'completed');

    echo json_encode([
        'success' => true,
        'data' => $migrationPlan
    ]);

} catch (Exception $e) {
    Option::set('bitrix_migrator', 'dryrun_status', 'error');
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage()
    ]);}
