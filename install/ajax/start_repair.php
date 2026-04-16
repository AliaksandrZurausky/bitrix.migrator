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

// Check if already running
$currentStatus = Option::get($moduleId, 'repair_status', 'idle');
if ($currentStatus === 'running') {
    echo json_encode(['success' => false, 'error' => 'Repair already running']);
    die();
}

// Selected types from POST
$selectedTypes = $_POST['types'] ?? [];
if (is_string($selectedTypes)) {
    $selectedTypes = json_decode($selectedTypes, true) ?: [];
}
$allowed = [
    'rebuild_mappings_companies', 'rebuild_mappings_contacts', 'rebuild_mappings_deals', 'rebuild_mappings_leads',
    'companies', 'contacts', 'deals', 'leads',
    'requisites_companies', 'requisites_contacts',
    'bindings_companies', 'bindings',
];
$selectedTypes = array_values(array_intersect($selectedTypes, $allowed));

if (empty($selectedTypes)) {
    echo json_encode(['success' => false, 'error' => 'No types selected']);
    die();
}

// Save selected types for CLI worker
Option::set($moduleId, 'repair_types', json_encode($selectedTypes));
Option::set($moduleId, 'repair_status', 'starting');
Option::set($moduleId, 'repair_stop', '0');
Option::set($moduleId, 'repair_progress', '');
Option::set($moduleId, 'repair_message', '');

// Find PHP CLI binary (same approach as start_migration.php)
$documentRoot = $_SERVER['DOCUMENT_ROOT'];
$workerPath = $documentRoot . '/local/modules/bitrix_migrator/install/cli/repair.php';

$phpBinary = null;
$candidates = ['/usr/bin/php', '/usr/local/bin/php', '/usr/bin/php8.2', '/usr/bin/php8.1', '/usr/bin/php8.0'];
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
if (!$phpBinary) {
    $fromPath = trim(shell_exec('which php 2>/dev/null') ?: '');
    if ($fromPath && is_executable($fromPath)) $phpBinary = $fromPath;
}
if (!$phpBinary) {
    Option::set($moduleId, 'repair_status', 'error');
    echo json_encode(['success' => false, 'error' => 'PHP CLI binary not found']);
    die();
}
if (!file_exists($workerPath)) {
    Option::set($moduleId, 'repair_status', 'error');
    echo json_encode(['success' => false, 'error' => 'Repair CLI worker not found']);
    die();
}

$logDir = $documentRoot . '/local/logs';
if (!is_dir($logDir)) @mkdir($logDir, 0775, true);
$stdoutLog = $logDir . '/repair_stdout_' . date('Y-m-d_H-i-s') . '.log';

// Launch CLI process in background via nohup
// All arguments are escaped via escapeshellarg to prevent injection
$command = 'nohup ' . escapeshellarg($phpBinary)
    . ' -d memory_limit=2G'
    . ' ' . escapeshellarg($workerPath)
    . ' ' . escapeshellarg($documentRoot)
    . ' > ' . escapeshellarg($stdoutLog) . ' 2>&1 & echo $!';

$pid = trim(shell_exec($command));

if ($pid && is_numeric($pid)) {
    Option::set($moduleId, 'repair_pid', $pid);
    echo json_encode(['success' => true, 'pid' => (int)$pid, 'types' => $selectedTypes]);
} else {
    Option::set($moduleId, 'repair_status', 'error');
    echo json_encode(['success' => false, 'error' => 'Failed to launch repair process']);
}
