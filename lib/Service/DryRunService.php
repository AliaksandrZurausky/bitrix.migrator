<?php

namespace BitrixMigrator\Service;

use Bitrix\Main\Config\Option;
use BitrixMigrator\Integration\CloudAPI;

class DryRunService
{
    private $api;

    public function __construct()
    {
        $webhookUrl = Option::get('bitrix_migrator', 'webhook_url', '');
        $this->api = new CloudAPI($webhookUrl);
    }

    /**
     * Analyze data from cloud
     */
    public function analyze()
    {
        $entities = [
            'users' => 'Пользователи',
            'crm.company' => 'Компании',
            'crm.contact' => 'Контакты',
            'crm.deal' => 'Сделки',
            'crm.lead' => 'Лиды',
            'tasks.task' => 'Задачи',
        ];

        $results = [];
        $progress = 0;
        $step = 100 / count($entities);

        foreach ($entities as $entityKey => $entityName) {
            try {
                $count = $this->getEntityCount($entityKey);
                $results[] = [
                    'key' => $entityKey,
                    'name' => $entityName,
                    'count' => $count,
                    'status' => $count > 0 ? 'ready' : 'empty',
                ];
            } catch (\Exception $e) {
                $results[] = [
                    'key' => $entityKey,
                    'name' => $entityName,
                    'count' => 0,
                    'status' => 'error',
                    'error' => $e->getMessage(),
                ];
            }

            $progress += $step;
            Option::set('bitrix_migrator', 'dryrun_progress', (int)$progress);
        }

        return $results;
    }

    /**
     * Get entity count from cloud
     */
    private function getEntityCount($entity)
    {
        switch ($entity) {
            case 'users':
                return $this->api->getUsersCount();
            case 'crm.company':
                return $this->api->getCompaniesCount();
            case 'crm.contact':
                return $this->api->getContactsCount();
            case 'crm.deal':
                return $this->api->getDealsCount();
            case 'crm.lead':
                return $this->api->getLeadsCount();
            case 'tasks.task':
                return $this->api->getTasksCount();
            default:
                return 0;
        }
    }
}
