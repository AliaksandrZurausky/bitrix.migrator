<?php

namespace BitrixMigrator\Service;

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

class MapService
{
    private static $hlblockId = null;
    private static $entity = null;

    public static function getHLBlockId()
    {
        if (self::$hlblockId === null) {
            self::$hlblockId = (int) Option::get('bitrix_migrator', 'map_hlblock_id');
            
            // If not found in Option, search by name
            if (!self::$hlblockId) {
                if (!Loader::includeModule('highloadblock')) {
                    throw new \Exception('HighloadBlock module not loaded');
                }
                
                $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
                    'filter' => ['=NAME' => 'MigratorMap']
                ])->fetch();
                
                if ($hlblock) {
                    self::$hlblockId = $hlblock['ID'];
                    Option::set('bitrix_migrator', 'map_hlblock_id', self::$hlblockId);
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
                throw new \Exception('MigratorMap HL-block not found (0)');
            }

            $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getById($hlblockId)->fetch();
            if (!$hlblock) {
                throw new \Exception('MigratorMap HL-block not found by ID');
            }

            self::$entity = \Bitrix\Highloadblock\HighloadBlockTable::compileEntity($hlblock);
        }
        return self::$entity;
    }

    /**
     * Add mapping cloud ID → local ID
     */
    public static function addMap($entityType, $cloudId, $localId, $cloudUrl = null, $metaJson = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        // Check if already exists
        $existing = $entityDataClass::getList([
            'filter' => [
                '=UF_ENTITY_TYPE' => $entityType,
                '=UF_CLOUD_ID' => (string) $cloudId,
            ]
        ])->fetch();

        if ($existing) {
            // Update existing
            return self::updateMap($existing['ID'], $localId, $cloudUrl, $metaJson);
        }

        $result = $entityDataClass::add([
            'UF_ENTITY_TYPE' => $entityType,
            'UF_CLOUD_ID' => (string) $cloudId,
            'UF_LOCAL_ID' => (string) $localId,
            'UF_CLOUD_URL' => $cloudUrl ?: '',
            'UF_META_JSON' => is_array($metaJson) ? json_encode($metaJson) : ($metaJson ?: ''),
        ]);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to add map: ' . implode(', ', $result->getErrorMessages()));
        }

        return $result->getId();
    }

    /**
     * Get local ID by cloud ID
     */
    public static function getLocalId($entityType, $cloudId)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::getList([
            'filter' => [
                '=UF_ENTITY_TYPE' => $entityType,
                '=UF_CLOUD_ID' => (string) $cloudId,
            ]
        ])->fetch();

        return $result ? $result['UF_LOCAL_ID'] : null;
    }

    /**
     * Get cloud ID by local ID
     */
    public static function getCloudId($entityType, $localId)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $result = $entityDataClass::getList([
            'filter' => [
                '=UF_ENTITY_TYPE' => $entityType,
                '=UF_LOCAL_ID' => (string) $localId,
            ]
        ])->fetch();

        return $result ? $result['UF_CLOUD_ID'] : null;
    }

    /**
     * Get full mapping by cloud ID
     */
    public static function getMap($entityType, $cloudId)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        return $entityDataClass::getList([
            'filter' => [
                '=UF_ENTITY_TYPE' => $entityType,
                '=UF_CLOUD_ID' => (string) $cloudId,
            ]
        ])->fetch();
    }

    /**
     * Update mapping
     */
    public static function updateMap($mapId, $localId = null, $cloudUrl = null, $metaJson = null)
    {
        $entity = self::getEntity();
        $entityDataClass = $entity->getDataClass();

        $updateData = [];
        if ($localId !== null) {
            $updateData['UF_LOCAL_ID'] = (string) $localId;
        }
        if ($cloudUrl !== null) {
            $updateData['UF_CLOUD_URL'] = $cloudUrl;
        }
        if ($metaJson !== null) {
            $updateData['UF_META_JSON'] = is_array($metaJson) ? json_encode($metaJson) : $metaJson;
        }

        if (empty($updateData)) {
            return $mapId;
        }

        $result = $entityDataClass::update($mapId, $updateData);

        if (!$result->isSuccess()) {
            throw new \Exception('Failed to update map: ' . implode(', ', $result->getErrorMessages()));
        }

        return $mapId;
    }

    /**
     * Check if mapping exists
     */
    public static function exists($entityType, $cloudId)
    {
        return self::getMap($entityType, $cloudId) !== null;
    }
}
