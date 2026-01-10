<?php

namespace BitrixMigrator\Service;

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use Bitrix\Main\Type\DateTime;

class QueueService
{
    private static $hlblockId = null;
    private static $entity = null;

    public static function getHLBlockId()
    {
        if (self::$hlblockId === null) {
            self::$hlblockId = (int) Option::get('bitrix_migrator', 'queue_hlblock_id');
        }
        return self::$hlblockId;
    }

    private static function getEntity()
    {
        if (self::$entity === null) {
            if (!Loader::includeModule('highloadblock')) {
                throw new \Exception('HighloadBlock module not loaded');
            }

            $hlblockId = self::getHLBlockId();
            if (!$hlblockId) {
                throw new \Exception('MigratorQueue HL-block not found');
            }

            $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getById($hlblockId)->fetch();
            if (!$hlblock) {
                throw new \Exception('MigratorQueue HL-block not found by ID');
            }

            self::$entity = \Bitrix\Highloadblock\HighloadBlockTable::compileEntity($hlblock);
        }
        return self::$entity;
    }

    /**
     * Add task to queue
     */
    public static function addTask($entityType, $cloudId, $localId = null, $priority = 0, $dependsOn = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::add([
            'UF_ENTITY_TYPE' => $entityType,
            'UF_CLOUD_ID' => (string) $cloudId,
            'UF_LOCAL_ID' => $localId ? (int) $localId : null,
            'UF_STATUS' => 'NEW',
            'UF_RETRIES' => 0,
            'UF_LAST_ERROR' => '',
            'UF_PRIORITY' => (int) $priority,
            'UF_DEPENDS_ON' => $dependsOn ?: '',
            'UF_UPDATED_AT' => new DateTime(),
        ]);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to add queue task: ' . implode(', ', $result->getErrorMessages()));
        }

        return $result->getId();
    }

    /**
     * Get next pending task
     */
    public static function getNextTask()
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        return $entityDataClass::getList([
            'filter' => ['=UF_STATUS' => 'NEW'],
            'order' => ['UF_PRIORITY' => 'DESC', 'ID' => 'ASC'],
            'limit' => 1,
        ])->fetch();
    }

    /**
     * Update task status
     */
    public static function updateTaskStatus($taskId, $status, $localId = null, $errorMsg = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $updateData = [
            'UF_STATUS' => $status,
            'UF_UPDATED_AT' => new DateTime(),
        ];

        if ($localId !== null) {
            $updateData['UF_LOCAL_ID'] = (int) $localId;
        }
        if ($errorMsg !== null) {
            $updateData['UF_LAST_ERROR'] = $errorMsg;
        }
        if ($status === 'ERROR') {
            $task = $entityDataClass::getByPrimary($taskId)->fetch();
            if ($task) {
                $updateData['UF_RETRIES'] = (int) $task['UF_RETRIES'] + 1;
            }
        }

        $result = $entityDataClass::update($taskId, $updateData);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to update task: ' . implode(', ', $result->getErrorMessages()));
        }

        return $taskId;
    }

    /**
     * Get queue statistics
     */
    public static function getStats()
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::getList([
            'select' => ['UF_STATUS'],
            'runtime' => [
                new \Bitrix\Main\Entity\ExpressionField(
                    'CNT',
                    'COUNT(*)',
                    []
                )
            ]
        ]);

        $stats = [
            'total' => 0,
            'new' => 0,
            'in_progress' => 0,
            'done' => 0,
            'error' => 0,
            'skipped' => 0,
        ];

        while ($row = $result->fetch()) {
            $status = $row['UF_STATUS'];
            $count = (int) $row['CNT'];
            $stats['total'] += $count;

            switch ($status) {
                case 'NEW':
                    $stats['new'] = $count;
                    break;
                case 'IN_PROGRESS':
                    $stats['in_progress'] = $count;
                    break;
                case 'DONE':
                    $stats['done'] = $count;
                    break;
                case 'ERROR':
                    $stats['error'] = $count;
                    break;
                case 'SKIPPED':
                    $stats['skipped'] = $count;
                    break;
            }
        }

        return $stats;
    }

    /**
     * Clear queue
     */
    public static function clearQueue()
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::getList([
            'select' => ['ID'],
        ]);

        while ($row = $result->fetch()) {
            try {
                $entityDataClass::delete($row['ID']);
            } catch (\Exception $e) {
                // Continue on error
            }
        }
    }
}
