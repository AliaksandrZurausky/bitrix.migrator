<?php

namespace BitrixMigrator\Service;

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use Bitrix\Main\Type\DateTime;

class LogService
{
    private static $hlblockId = null;
    private static $entity = null;

    const LEVEL_DEBUG = 'DEBUG';
    const LEVEL_INFO = 'INFO';
    const LEVEL_WARN = 'WARN';
    const LEVEL_ERROR = 'ERROR';

    public static function getHLBlockId()
    {
        if (self::$hlblockId === null) {
            self::$hlblockId = (int) Option::get('bitrix_migrator', 'logs_hlblock_id');
            
            // If not found in Option, search by name
            if (!self::$hlblockId) {
                if (!Loader::includeModule('highloadblock')) {
                    throw new \Exception('HighloadBlock module not loaded');
                }
                
                $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
                    'filter' => ['=NAME' => 'MigratorLogs']
                ])->fetch();
                
                if ($hlblock) {
                    self::$hlblockId = $hlblock['ID'];
                    Option::set('bitrix_migrator', 'logs_hlblock_id', self::$hlblockId);
                }
            }
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
                throw new \Exception('MigratorLogs HL-block not found (0)');
            }

            $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getById($hlblockId)->fetch();
            if (!$hlblock) {
                throw new \Exception('MigratorLogs HL-block not found by ID');
            }

            self::$entity = \Bitrix\Highloadblock\HighloadBlockTable::compileEntity($hlblock);
        }
        return self::$entity;
    }

    /**
     * Add log entry
     */
    public static function addLog($message, $level = self::LEVEL_INFO, $context = null, $payload = null)
    {
        try {
            $entity = self::getEntity();
            $entityDataClass = $entity->getDataClass();

            $result = $entityDataClass::add([
                'UF_LEVEL' => $level,
                'UF_MESSAGE' => substr($message, 0, 5000), // Limit message length
                'UF_CONTEXT' => $context ? substr($context, 0, 500) : '',
                'UF_PAYLOAD' => $payload ? json_encode($payload) : '',
                'UF_CREATED_AT' => new DateTime(),
            ]);

            if (!$result->isSuccess()) {
                // Silently fail for logging to avoid recursion
                return false;
            }

            return $result->getId();
        } catch (\Exception $e) {
            // Silently fail
            return false;
        }
    }

    /**
     * Log with INFO level
     */
    public static function info($message, $context = null, $payload = null)
    {
        return self::addLog($message, self::LEVEL_INFO, $context, $payload);
    }

    /**
     * Log with DEBUG level
     */
    public static function debug($message, $context = null, $payload = null)
    {
        return self::addLog($message, self::LEVEL_DEBUG, $context, $payload);
    }

    /**
     * Log with WARN level
     */
    public static function warn($message, $context = null, $payload = null)
    {
        return self::addLog($message, self::LEVEL_WARN, $context, $payload);
    }

    /**
     * Log with ERROR level
     */
    public static function error($message, $context = null, $payload = null)
    {
        return self::addLog($message, self::LEVEL_ERROR, $context, $payload);
    }

    /**
     * Get logs
     */
    public static function getLogs($limit = 50, $offset = 0, $level = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $filter = [];
        if ($level) {
            $filter['=UF_LEVEL'] = $level;
        }

        $result = $entityDataClass::getList([
            'filter' => $filter,
            'order' => ['ID' => 'DESC'],
            'limit' => $limit,
            'offset' => $offset,
        ]);

        $logs = [];
        while ($row = $result->fetch()) {
            $logs[] = $row;
        }

        return $logs;
    }

    /**
     * Get logs count
     */
    public static function getCount($level = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $filter = [];
        if ($level) {
            $filter['=UF_LEVEL'] = $level;
        }

        $result = $entityDataClass::getList([
            'filter' => $filter,
            'select' => ['CNT' => 'ID'],
            'count_total' => true,
        ]);

        return $result->getCount();
    }

    /**
     * Clear logs
     */
    public static function clearLogs($level = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $filter = [];
        if ($level) {
            $filter['=UF_LEVEL'] = $level;
        }

        $result = $entityDataClass::getList([
            'filter' => $filter,
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
