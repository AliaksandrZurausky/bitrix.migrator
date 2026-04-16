#!/usr/bin/env php
<?php
/**
 * CLI worker for post-migration repair (field/requisite/binding refresh).
 *
 * Usage:  php repair.php [document_root]
 */

set_time_limit(0);
ini_set('memory_limit', '2G');

$autoRoot = dirname(__DIR__, 5);
$documentRoot = $argv[1] ?? $autoRoot;

if (!is_file($documentRoot . '/bitrix/modules/main/include/prolog_before.php')) {
    if ($documentRoot !== $autoRoot && is_file($autoRoot . '/bitrix/modules/main/include/prolog_before.php')) {
        $documentRoot = $autoRoot;
    } else {
        fwrite(STDERR, "ERROR: Invalid document root: $documentRoot\n");
        exit(1);
    }
}

$_SERVER['DOCUMENT_ROOT']  = $documentRoot;
$_SERVER['SERVER_NAME']    = 'localhost';
$_SERVER['HTTP_HOST']      = 'localhost';
$_SERVER['REQUEST_URI']    = '/';
$_SERVER['SERVER_PORT']    = '80';
$_SERVER['HTTPS']          = '';

define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);
define('NOT_CHECK_PERMISSIONS', true);
define('BX_NO_ACCELERATOR_RESET', true);

require_once($documentRoot . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use BitrixMigrator\Integration\CloudAPI;
use BitrixMigrator\Service\RepairService;

$moduleId = 'bitrix_migrator';

if (!Loader::includeModule($moduleId)) {
    fwrite(STDERR, "Module bitrix_migrator not loaded\n");
    exit(1);
}

$pid = getmypid();
Option::set($moduleId, 'repair_pid', (string)$pid);
Option::set($moduleId, 'repair_stop', '0');
Option::set($moduleId, 'repair_status', 'running');

$logDir = $documentRoot . '/local/logs';
if (!is_dir($logDir)) {
    @mkdir($logDir, 0775, true);
}
$logFile = $logDir . '/bitrix_migrator_repair_' . date('Y-m-d_H-i-s') . '.log';
Option::set($moduleId, 'repair_log_file', $logFile);

function cliLog($message, $logFile) {
    $line = date('Y-m-d H:i:s') . ' ' . $message . "\n";
    @file_put_contents($logFile, $line, FILE_APPEND);
    fwrite(STDOUT, $line);
}

cliLog("Repair worker started (PID=$pid, root=$documentRoot)", $logFile);

$cloudWebhookUrl = Option::get($moduleId, 'cloud_webhook_url', '');
$boxWebhookUrl   = Option::get($moduleId, 'box_webhook_url', '');

if (empty($cloudWebhookUrl) || empty($boxWebhookUrl)) {
    $error = 'Webhook URLs not configured';
    cliLog("ERROR: $error", $logFile);
    Option::set($moduleId, 'repair_status', 'error');
    Option::set($moduleId, 'repair_message', $error);
    Option::set($moduleId, 'repair_pid', '');
    exit(1);
}

$cloudAPI = new CloudAPI($cloudWebhookUrl);
$boxAPI   = new CloudAPI($boxWebhookUrl);

// Selected types from UI
$typesJson = Option::get($moduleId, 'repair_types', '[]');
$selectedTypes = json_decode($typesJson, true) ?: [];
cliLog("Repair types: " . implode(', ', $selectedTypes), $logFile);

register_shutdown_function(function () use ($moduleId, $logFile) {
    $err = error_get_last();
    if ($err && in_array($err['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR], true)) {
        $msg = "PHP FATAL [{$err['type']}]: {$err['message']} in {$err['file']}:{$err['line']}";
        @file_put_contents($logFile, date('Y-m-d H:i:s') . ' ' . $msg . "\n", FILE_APPEND);
        fwrite(STDERR, $msg . "\n");
        Option::set($moduleId, 'repair_status', 'error');
        Option::set($moduleId, 'repair_message', $msg);
        Option::set($moduleId, 'repair_pid', '');
    }
});

try {
    $service = new RepairService($cloudAPI, $boxAPI);
    $service->setLogFile($logFile);
    $service->repair($selectedTypes);
} catch (\Throwable $e) {
    $error = $e->getMessage();
    cliLog("FATAL ERROR: $error", $logFile);
    Option::set($moduleId, 'repair_status', 'error');
    Option::set($moduleId, 'repair_message', $error);
}

Option::set($moduleId, 'repair_pid', '');
cliLog("Repair worker finished", $logFile);
