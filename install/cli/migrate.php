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
ini_set('memory_limit', '2G');

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

// Inject test-run scope (if set by ajax/start_migration.php)
$scopeJson = Option::get($moduleId, 'migration_scope', '');
if (!empty($scopeJson)) {
    $scope = json_decode($scopeJson, true);
    if (is_array($scope) && !empty($scope['entity_type']) && !empty($scope['entity_ids'])) {
        $plan['scope'] = $scope;
        cliLog("Scoped run: type={$scope['entity_type']} ids=" . implode(',', (array)$scope['entity_ids']), $logFile);
    }
}

// Catch fatal PHP errors (out of memory, class not found, etc.) that bypass try/catch
register_shutdown_function(function () use ($moduleId, $logFile) {
    $err = error_get_last();
    if ($err && in_array($err['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR], true)) {
        $msg = "PHP FATAL [{$err['type']}]: {$err['message']} in {$err['file']}:{$err['line']}";
        @file_put_contents($logFile, date('Y-m-d H:i:s') . ' ' . $msg . "\n", FILE_APPEND);
        fwrite(STDERR, $msg . "\n");
        Option::set($moduleId, 'migration_status', 'error');
        Option::set($moduleId, 'migration_message', $msg);
        Option::set($moduleId, 'migration_pid', '');
    }
});

// Incremental migration: inject flag into plan settings
if ($migrateType === 'incremental') {
    if (!isset($plan['settings'])) $plan['settings'] = [];
    $plan['settings']['incremental'] = true;
}

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
} catch (\Throwable $e) {
    cliLog("FATAL: " . $e->getMessage() . " in " . $e->getFile() . ":" . $e->getLine(), $logFile);
    Option::set($moduleId, 'migration_status', 'error');
    Option::set($moduleId, 'migration_message', $e->getMessage());
}

// Cleanup PID
Option::set($moduleId, 'migration_pid', '');
cliLog("Migration worker finished", $logFile);

// Batched execution respawn: a phase signalled it has hit per-process batch
// limit. Spawn a fresh worker so the OS reclaims memory held by Bitrix's
// eval'd ORM classes (unclearable mid-process). The new worker skips
// already-completed phases and resumes via phase_cursor_*.
if (Option::get($moduleId, 'migration_respawn', '0') === '1') {
    $respawnStdout = $logDir . '/migrator_stdout_' . date('Y-m-d_H-i-s') . '_respawn.log';
    $respawnCmd = 'nohup ' . escapeshellarg(PHP_BINARY)
        . ' -d memory_limit=2G'
        . ' ' . escapeshellarg(__FILE__)
        . ' ' . escapeshellarg($documentRoot)
        . ' ' . escapeshellarg($migrateType)
        . ' > ' . escapeshellarg($respawnStdout) . ' 2>&1 & echo $!';
    $newPid = trim((string)shell_exec($respawnCmd));

    if ($newPid && is_numeric($newPid)) {
        Option::set($moduleId, 'migration_respawn', '0');
        Option::set($moduleId, 'migration_pid', $newPid);
        cliLog("Respawned worker with PID=$newPid", $logFile);
    } else {
        Option::set($moduleId, 'migration_status', 'error');
        Option::set($moduleId, 'migration_message', 'Respawn failed: could not spawn new worker');
        cliLog("FATAL: failed to respawn worker (shell_exec returned '$newPid')", $logFile);
    }
    exit(0);
}
