<?php

namespace BitrixMigrator\Service;

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

class StateService
{
    private static $hlblockId = null;
    private static $entity = null;

    public static function getHLBlockId()
    {
        if (self::$hlblockId === null) {
            self::$hlblockId = (int) Option::get('bitrix_migrator', 'state_hlblock_id');
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
                throw new \Exception('MigratorState HL-block not found');
            }

            $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getById($hlblockId)->fetch();
            if (!$hlblock) {
                throw new \Exception('MigratorState HL-block not found by ID');
            }

            self::$entity = \Bitrix\Highloadblock\HighloadBlockTable::compileEntity($hlblock);
        }
        return self::$entity;
    }

    /**
     * Get current state or create default
     */
    public static function getState()
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::getList([
            'limit' => 1,
            'order' => ['ID' => 'DESC']
        ]);

        if ($state = $result->fetch()) {
            return $state;
        }

        // Create default state
        return self::initializeState();
    }

    /**
     * Initialize default state
     */
    public static function initializeState()
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::add([
            'UF_STATUS' => 'NEW',
            'UF_CURRENT_PHASE' => 'INIT',
            'UF_PAUSE_FLAG' => false,
            'UF_SETTINGS_JSON' => '{}',
            'UF_LAST_ERROR' => '',
            'UF_STARTED_AT' => null,
            'UF_UPDATED_AT' => new \Bitrix\Main\Type\DateTime(),
        ]);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to create initial state: ' . implode(', ', $result->getErrorMessages()));
        }

        return $entityDataClass::getByPrimary($result->getId())->fetch();
    }

    /**
     * Update state
     */
    public static function setState($status = null, $phase = null, $settingsJson = null, $lastError = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $state = self::getState();
        $stateId = $state['ID'];

        $updateData = [
            'UF_UPDATED_AT' => new \Bitrix\Main\Type\DateTime(),
        ];

        if ($status !== null) {
            $updateData['UF_STATUS'] = $status;
        }
        if ($phase !== null) {
            $updateData['UF_CURRENT_PHASE'] = $phase;
        }
        if ($settingsJson !== null) {
            $updateData['UF_SETTINGS_JSON'] = is_string($settingsJson) ? $settingsJson : json_encode($settingsJson);
        }
        if ($lastError !== null) {
            $updateData['UF_LAST_ERROR'] = $lastError;
        }

        $result = $entityDataClass::update($stateId, $updateData);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to update state: ' . implode(', ', $result->getErrorMessages()));
        }

        return $entityDataClass::getByPrimary($stateId)->fetch();
    }

    /**
     * Set pause flag
     */
    public static function setPauseFlag($flag = true)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $state = self::getState();
        $stateId = $state['ID'];

        $result = $entityDataClass::update($stateId, [
            'UF_PAUSE_FLAG' => $flag,
            'UF_UPDATED_AT' => new \Bitrix\Main\Type\DateTime(),
        ]);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to set pause flag: ' . implode(', ', $result->getErrorMessages()));
        }

        return $flag;
    }

    /**
     * Get pause flag
     */
    public static function isPaused()
    {
        $state = self::getState();
        return (bool) $state['UF_PAUSE_FLAG'];
    }
}
