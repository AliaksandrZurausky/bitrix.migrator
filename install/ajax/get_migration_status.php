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

$status   = Option::get($moduleId, 'migration_status', 'idle');
$message  = Option::get($moduleId, 'migration_message', '');
$stats    = json_decode(Option::get($moduleId, 'migration_stats', '{}'), true);
$log      = json_decode(Option::get($moduleId, 'migration_log', '[]'), true);
$phases   = json_decode(Option::get($moduleId, 'migration_phases', '{}'), true);
$progress = json_decode(Option::get($moduleId, 'migration_progress', '{}'), true);
$pid      = (int)Option::get($moduleId, 'migration_pid', '0');
$logFile  = Option::get($moduleId, 'migration_log_file', '');

// Check if process is actually alive (if status says running but process died)
if ($status === 'running' && $pid > 0) {
    if (function_exists('posix_kill')) {
        $alive = posix_kill($pid, 0); // Signal 0 = check if process exists
        if (!$alive) {
            $status = 'error';
            $message = 'Процесс миграции завершился неожиданно (PID ' . $pid . ')';
            Option::set($moduleId, 'migration_status', $status);
            Option::set($moduleId, 'migration_message', $message);
            Option::set($moduleId, 'migration_pid', '');
        }
    }
}

// If status is "stopping" and process is dead, update to "stopped"
if ($status === 'stopping' && $pid > 0) {
    if (function_exists('posix_kill') && !posix_kill($pid, 0)) {
        $status = 'stopped';
        Option::set($moduleId, 'migration_status', 'stopped');
        Option::set($moduleId, 'migration_message', 'Миграция остановлена');
        Option::set($moduleId, 'migration_pid', '');
    }
}

echo json_encode([
    'success'  => true,
    'status'   => $status,
    'message'  => $message,
    'stats'    => $stats,
    'log'      => $log,
    'phases'   => $phases,
    'progress' => $progress,
    'pid'      => $pid,
    'log_file' => $logFile,
]);
