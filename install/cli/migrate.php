#!/usr/bin/env php
<?php
/**
 * CLI worker for background migration.
 * Runs outside PHP-FPM — does not block web workers.
 *
 * Usage:  php migrate.php [document_root] [full|tasks]
 *
 * If document_root is omitted, it is auto-detected from this file's location:
 *   .../local/modules/bitrix_migrator/install/cli/migrate.php
 *   => document_root = 5 levels up
 */

set_time_limit(0);
ini_set('memory_limit', '512M');

// Auto-detect document root from this file's path
$autoRoot = dirname(__DIR__, 5); // install/cli -> install -> bitrix_migrator -> modules -> local -> ROOT

$documentRoot = $argv[1] ?? $autoRoot;
$migrateType  = $argv[2] ?? 'full';

// Verify the document root is valid
if (!is_file($documentRoot . '/bitrix/modules/main/include/prolog_before.php')) {
    // Try auto-detected path as fallback
    if ($documentRoot !== $autoRoot && is_file($autoRoot . '/bitrix/modules/main/include/prolog_before.php')) {
        $documentRoot = $autoRoot;
    } else {
        fwrite(STDERR, "ERROR: Invalid document root: $documentRoot\n");
        fwrite(STDERR, "Bitrix core not found at: $documentRoot/bitrix/modules/main/include/prolog_before.php\n");
        exit(1);
    }
}

// Emulate server environment for Bitrix core
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
use BitrixMigrator\Service\MigrationService;
use BitrixMigrator\Service\TaskMigrationService;

$moduleId = 'bitrix_migrator';

if (!Loader::includeModule($moduleId)) {
    fwrite(STDERR, "Module bitrix_migrator not loaded\n");
    exit(1);
}

// Save PID so the UI can stop this process
$pid = getmypid();
Option::set($moduleId, 'migration_pid', (string)$pid);
Option::set($moduleId, 'migration_stop', '0');

// Log file
$logDir = $documentRoot . '/local/logs';
if (!is_dir($logDir)) {
    @mkdir($logDir, 0775, true);
}
$logFile = $logDir . '/bitrix_migrator_' . date('Y-m-d_H-i-s') . '.log';
Option::set($moduleId, 'migration_log_file', $logFile);

function cliLog($message, $logFile) {
    $line = date('Y-m-d H:i:s') . ' ' . $message . "\n";
    @file_put_contents($logFile, $line, FILE_APPEND);
    fwrite(STDOUT, $line);
}

cliLog("Migration worker started (PID=$pid, type=$migrateType, root=$documentRoot)", $logFile);

$cloudWebhookUrl = Option::get($moduleId, 'cloud_webhook_url', '');
$boxWebhookUrl   = Option::get($moduleId, 'box_webhook_url', '');

if (empty($cloudWebhookUrl) || empty($boxWebhookUrl)) {
    $error = 'Webhook URLs not configured';
    cliLog("ERROR: $error", $logFile);
    Option::set($moduleId, 'migration_status', 'error');
    Option::set($moduleId, 'migration_message', $error);
    Option::set($moduleId, 'migration_pid', '');
    exit(1);
}

$cloudAPI = new CloudAPI($cloudWebhookUrl);
$boxAPI   = new CloudAPI($boxWebhookUrl);

$planJson = Option::get($moduleId, 'migration_plan', '');
$plan     = $planJson ? json_decode($planJson, true) : [];

try {
    if ($migrateType === 'tasks') {
        $service = new TaskMigrationService($cloudAPI, $boxAPI, $plan);
        $service->setLogFile($logFile);
        $service->migrate();
    } else {
        $service = new MigrationService($cloudAPI, $boxAPI, $plan);
        $service->setLogFile($logFile);
        $service->migrate();
    }
} catch (\Exception $e) {
    cliLog("FATAL: " . $e->getMessage(), $logFile);
    Option::set($moduleId, 'migration_status', 'error');
    Option::set($moduleId, 'migration_message', $e->getMessage());
}

// Cleanup PID
Option::set($moduleId, 'migration_pid', '');
cliLog("Migration worker finished", $logFile);
