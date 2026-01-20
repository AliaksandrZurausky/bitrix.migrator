<?php

namespace BitrixMigrator\Agent;

use BitrixMigrator\Service\DryRunService;
use Bitrix\Main\Config\Option;

class DryRunAgent
{
    public static function run()
    {
        try {
            DryRunService::analyze();
            Option::set('bitrix_migrator', 'dryrun_status', 'completed');
        } catch (\Exception $e) {
            Option::set('bitrix_migrator', 'dryrun_status', 'error');
            Option::set('bitrix_migrator', 'dryrun_error', $e->getMessage());
        }

        return '';
    }
}
