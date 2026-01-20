<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Integration\CloudAPI;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use Bitrix\Highloadblock\HighloadBlockTable;

class DryRunService
{
    /**
     * Simplified analyze: only get departments from cloud
     */
    public static function analyze()
    {
        if (!Loader::includeModule('bitrix_migrator') || !Loader::includeModule('highloadblock')) {
            throw new \Exception('Required modules not loaded');
        }

        $cloudWebhookUrl = Option::get('bitrix_migrator', 'cloud_webhook_url', '');
        if (empty($cloudWebhookUrl)) {
            throw new \Exception('Cloud webhook URL not configured');
        }

        $cloudAPI = new CloudAPI($cloudWebhookUrl);
        $departments = $cloudAPI->getDepartments();

        $data = [
            'departments' => $departments,
            'count' => count($departments)
        ];

        // Save to HL-block
        $hlblockId = Option::get('bitrix_migrator', 'dryrun_hlblock_id', 0);
        if (!$hlblockId) {
            throw new \Exception('DryRun HL block not found');
        }

        $hlblock = HighloadBlockTable::getById($hlblockId)->fetch();
        if (!$hlblock) {
            throw new \Exception('HL block not found');
        }

        $entity = HighloadBlockTable::compileEntity($hlblock);
        $entityClass = $entity->getDataClass();

        // Delete old records
        $old = $entityClass::getList(['select' => ['ID']])->fetchAll();
        foreach ($old as $row) {
            $entityClass::delete($row['ID']);
        }

        // Add new record
        $entityClass::add([
            'UF_DATA_JSON' => json_encode($data, JSON_UNESCAPED_UNICODE),
            'UF_CREATED_AT' => new \Bitrix\Main\Type\DateTime()
        ]);

        return $data;
    }
}
