<?php

namespace BitrixMigrator\Agent;

use Bitrix\Main\Loader;
use BitrixMigrator\Service\StateService;
use BitrixMigrator\Service\QueueService;
use BitrixMigrator\Service\LogService;

class MigratorAgent
{
    public static function run()
    {
        try {
            if (!Loader::includeModule('bitrix_migrator')) {
                return "\\BitrixMigrator\\Agent\\MigratorAgent::run();";
            }

            // Check if paused
            if (StateService::isPaused()) {
                LogService::debug('Agent paused');
                return "\\BitrixMigrator\\Agent\\MigratorAgent::run();";
            }

            // Get current state
            $state = StateService::getState();

            // Process next queue task
            $task = QueueService::getNextTask();
            if ($task) {
                self::processTask($task);
            } else {
                LogService::debug('No tasks in queue');
            }
        } catch (\Exception $e) {
            LogService::error('Agent error: ' . $e->getMessage(), 'AGENT');
        }

        return "\\BitrixMigrator\\Agent\\MigratorAgent::run();";
    }

    private static function processTask($task)
    {
        $taskId = $task['ID'];
        $entityType = $task['UF_ENTITY_TYPE'];
        $cloudId = $task['UF_CLOUD_ID'];

        try {
            // Mark as in progress
            QueueService::updateTaskStatus($taskId, 'IN_PROGRESS');

            LogService::debug(
                "Processing task: $entityType #$cloudId",
                "QUEUE_TASK",
                ['task_id' => $taskId, 'entity_type' => $entityType]
            );

            // TODO: Implement actual migration logic for each entity type
            // For now, just mark as done
            QueueService::updateTaskStatus($taskId, 'DONE');
            LogService::info(
                "Task completed: $entityType #$cloudId",
                "QUEUE_TASK",
                ['task_id' => $taskId]
            );
        } catch (\Exception $e) {
            QueueService::updateTaskStatus($taskId, 'ERROR', null, $e->getMessage());
            LogService::error(
                "Task error: " . $e->getMessage(),
                "QUEUE_TASK",
                ['task_id' => $taskId, 'entity_type' => $entityType]
            );
        }
    }
}
