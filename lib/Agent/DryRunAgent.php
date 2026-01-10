<?php

namespace BitrixMigrator\Agent;

use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use BitrixMigrator\Service\DryRunService;

class DryRunAgent
{
    /**
     * Run dry run process
     */
    public static function run()
    {
        if (!Loader::includeModule('bitrix_migrator')) {
            return '';
        }

        try {
            $status = Option::get('bitrix_migrator', 'dryrun_status', 'idle');
            
            if ($status !== 'running') {
                return '';
            }

            $service = new DryRunService();
            $results = $service->analyze();

            // Save results
            Option::set('bitrix_migrator', 'dryrun_results', json_encode($results));
            Option::set('bitrix_migrator', 'dryrun_status', 'completed');
            Option::set('bitrix_migrator', 'dryrun_progress', '100');
            
            // Agent completed, return empty string to stop
            return '';
            
        } catch (\Exception $e) {
            Option::set('bitrix_migrator', 'dryrun_status', 'error');
            Option::set('bitrix_migrator', 'dryrun_error', $e->getMessage());
            return '';
        }
    }
}
