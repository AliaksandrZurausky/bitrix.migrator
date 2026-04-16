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

if (!Loader::includeModule('bitrix_migrator')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$moduleId = 'bitrix_migrator';

$status   = Option::get($moduleId, 'repair_status', 'idle');
$message  = Option::get($moduleId, 'repair_message', '');
$progress = json_decode(Option::get($moduleId, 'repair_progress', '{}'), true);
$pid      = (int)Option::get($moduleId, 'repair_pid', '0');
$logFile  = Option::get($moduleId, 'repair_log_file', '');

// Check if process is alive
if ($status === 'running' && $pid > 0) {
    if (function_exists('posix_kill') && !posix_kill($pid, 0)) {
        $status = 'error';
        $message = 'Процесс дозаполнения завершился неожиданно (PID ' . $pid . ')';
        Option::set($moduleId, 'repair_status', $status);
        Option::set($moduleId, 'repair_message', $message);
        Option::set($moduleId, 'repair_pid', '');
    }
}

// Read last N lines from log file for real-time display
$logLines = [];
if ($logFile && is_file($logFile)) {
    $lines = file($logFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    if ($lines) {
        $logLines = array_slice($lines, -100);
    }
}

echo json_encode([
    'success'  => true,
    'status'   => $status,
    'message'  => $message,
    'progress' => $progress,
    'pid'      => $pid,
    'log_file' => $logFile,
    'log'      => $logLines,
]);
