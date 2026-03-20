<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

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

// Reset state
Option::set($moduleId, 'migration_status', 'running');
Option::set($moduleId, 'migration_message', 'Запуск миграции...');
Option::set($moduleId, 'migration_stats', '{}');
Option::set($moduleId, 'migration_log', '[]');
Option::set($moduleId, 'migration_phases', '{}');
Option::set($moduleId, 'migration_progress', '{}');
Option::set($moduleId, 'migration_stop', '0');
Option::set($moduleId, 'migration_pid', '');

// Path to CLI worker — resolve from module directory, not from ajax copy
$documentRoot = $_SERVER['DOCUMENT_ROOT'];
$workerPath = $documentRoot . '/local/modules/bitrix_migrator/install/cli/migrate.php';
$phpBinary = PHP_BINARY ?: '/usr/bin/php';

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
