<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

global $USER;
if (!$USER->IsAdmin()) {
    echo json_encode(['success' => false, 'error' => 'Access denied']);
    die();
}

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

header('Content-Type: application/json');

if (!check_bitrix_sessid()) {
    echo json_encode(['success' => false, 'error' => 'Invalid sessid']);
    die();
}

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$moduleId = 'bitrix_migrator';

$cloudWebhookUrl = Option::get($moduleId, 'cloud_webhook_url', '');
$boxWebhookUrl   = Option::get($moduleId, 'box_webhook_url', '');

if (empty($cloudWebhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Cloud webhook URL not configured']);
    die();
}

if (empty($boxWebhookUrl)) {
    echo json_encode(['success' => false, 'error' => 'Box webhook URL not configured']);
    die();
}

// Check if already running
$currentStatus = Option::get($moduleId, 'migration_status', 'idle');
if ($currentStatus === 'running') {
    echo json_encode(['success' => false, 'error' => 'Migration already running']);
    die();
}

$migrateType = $_POST['type'] ?? 'full';
if (!in_array($migrateType, ['full', 'tasks', 'incremental'], true)) {
    $migrateType = 'full';
}

// Test (scoped) migration: single company/contact/task and everything bound to it
$scopeJson = '';
$scopeType = $_POST['scope_type'] ?? '';
$scopeIdsRaw = $_POST['scope_ids'] ?? '';
if (in_array($scopeType, ['company', 'contact', 'task'], true) && $scopeIdsRaw !== '') {
    $scopeIds = array_values(array_filter(array_map('intval', explode(',', $scopeIdsRaw))));
    if (!empty($scopeIds)) {
        $scopeJson = json_encode([
            'entity_type'  => $scopeType,
            'entity_ids'   => $scopeIds,
            'skip_prereqs' => !empty($_POST['scope_skip_prereqs']) && $_POST['scope_skip_prereqs'] !== '0',
        ]);
    }
}

// Reset state
Option::set($moduleId, 'migration_status', 'running');
Option::set($moduleId, 'migration_message', 'Запуск миграции...');
Option::set($moduleId, 'migration_stats', '{}');
Option::set($moduleId, 'migration_log', '[]');
Option::set($moduleId, 'migration_phases', '{}');
Option::set($moduleId, 'migration_progress', '{}');
Option::set($moduleId, 'migration_stop', '0');
Option::set($moduleId, 'migration_pid', '');

// Reset respawn / batched-execution state from any previous run
Option::set($moduleId, 'migration_respawn', '0');
Option::set($moduleId, 'pipeline_map_cache', '');
Option::set($moduleId, 'stage_map_cache', '');
Option::set($moduleId, 'uf_field_schema', '');
Option::set($moduleId, 'uf_enum_map', '');
Option::set($moduleId, 'smart_process_map_cache', '');
Option::set($moduleId, 'smart_items_by_type', '');
// Test migration scope — either the explicit JSON or cleared for full run
Option::set($moduleId, 'migration_scope', $scopeJson);
foreach (['departments','users','crm_fields','pipelines','currencies','companies','contacts','leads','deals','invoices','requisites','smart_processes','workgroups','timeline','tasks'] as $ph) {
    Option::set($moduleId, 'phase_cursor_' . $ph, '0');
}

// Path to CLI worker — resolve from module directory, not from ajax copy
$documentRoot = $_SERVER['DOCUMENT_ROOT'];
$workerPath = $documentRoot . '/local/modules/bitrix_migrator/install/cli/migrate.php';
// PHP_BINARY may point to php-fpm which can't run scripts — find CLI binary
$phpBinary = null;
$candidates = [
    '/usr/bin/php',
    '/usr/local/bin/php',
    '/usr/bin/php8.2',
    '/usr/bin/php8.1',
    '/usr/bin/php8.0',
    '/usr/bin/php7.4',
];
// Check if PHP_BINARY is actually CLI (not fpm)
if (PHP_BINARY && !preg_match('/fpm/', PHP_BINARY)) {
    $phpBinary = PHP_BINARY;
} else {
    foreach ($candidates as $path) {
        if (is_file($path) && is_executable($path)) {
            $phpBinary = $path;
            break;
        }
    }
}
// Last resort: try 'php' from PATH
if (!$phpBinary) {
    $fromPath = trim(shell_exec('which php 2>/dev/null') ?: '');
    if ($fromPath && is_executable($fromPath)) {
        $phpBinary = $fromPath;
    }
}
if (!$phpBinary) {
    Option::set($moduleId, 'migration_status', 'error');
    Option::set($moduleId, 'migration_message', 'PHP CLI binary not found');
    echo json_encode(['success' => false, 'error' => 'PHP CLI binary not found. Install php-cli package.']);
    die();
}

if (!$workerPath || !file_exists($workerPath)) {
    Option::set($moduleId, 'migration_status', 'error');
    Option::set($moduleId, 'migration_message', 'CLI worker not found');
    echo json_encode(['success' => false, 'error' => 'CLI worker not found at expected path']);
    die();
}

// Launch CLI process in background
// Output goes to log file, nohup keeps it running after parent exits
$logDir = $documentRoot . '/local/logs';
if (!is_dir($logDir)) {
    @mkdir($logDir, 0775, true);
}
$stdoutLog = $logDir . '/migrator_stdout_' . date('Y-m-d_H-i-s') . '.log';

$command = 'nohup ' . escapeshellarg($phpBinary)
    . ' -d memory_limit=4G'
    . ' ' . escapeshellarg($workerPath)
    . ' ' . escapeshellarg($documentRoot)
    . ' ' . escapeshellarg($migrateType)
    . ' > ' . escapeshellarg($stdoutLog) . ' 2>&1 & echo $!';

$pid = trim(shell_exec($command));

if ($pid && is_numeric($pid)) {
    Option::set($moduleId, 'migration_pid', $pid);
    echo json_encode([
        'success' => true,
        'message' => 'Migration started in background',
        'pid' => (int)$pid,
    ]);
} else {
    // Fallback: try exec()
    $command2 = escapeshellarg($phpBinary)
        . ' -d memory_limit=4G'
        . ' ' . escapeshellarg($workerPath)
        . ' ' . escapeshellarg($documentRoot)
        . ' ' . escapeshellarg($migrateType)
        . ' > ' . escapeshellarg($stdoutLog) . ' 2>&1 &';
    exec($command2);

    echo json_encode([
        'success' => true,
        'message' => 'Migration started (exec fallback)',
    ]);
}
