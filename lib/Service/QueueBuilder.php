<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Repository\Hl\QueueRepository;
use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Integration\Cloud\Api\DealApi;
use BitrixMigrator\Integration\Cloud\Api\ContactApi;
use BitrixMigrator\Integration\Cloud\Api\CompanyApi;
use BitrixMigrator\Integration\Cloud\Api\LeadApi;
use BitrixMigrator\Domain\Enum\EntityType;
use BitrixMigrator\Domain\Enum\TaskType;
use Bitrix\Main\Type\DateTime;

final class QueueBuilder
{
    private QueueRepository $queueRepo;
    private RestClient $cloudClient;

    public function __construct(QueueRepository $queueRepo, RestClient $cloudClient)
    {
        $this->queueRepo = $queueRepo;
        $this->cloudClient = $cloudClient;
    }

    public function buildForDeals(array $filter = []): int
    {
        return $this->buildForEntity(EntityType::DEAL, new DealApi($this->cloudClient), $filter);
    }

    public function buildForContacts(array $filter = []): int
    {
        return $this->buildForEntity(EntityType::CONTACT, new ContactApi($this->cloudClient), $filter);
    }

    public function buildForCompanies(array $filter = []): int
    {
        return $this->buildForEntity(EntityType::COMPANY, new CompanyApi($this->cloudClient), $filter);
    }

    public function buildForLeads(array $filter = []): int
    {
        return $this->buildForEntity(EntityType::LEAD, new LeadApi($this->cloudClient), $filter);
    }

    private function buildForEntity(string $entityType, $api, array $filter): int
    {
        $count = 0;
        $start = 0;
        $hasMore = true;

        while ($hasMore) {
            $result = $api->getList($filter, $start);
            $items = $result ?? [];

            if (empty($items)) {
                $hasMore = false;
                break;
            }

            foreach ($items as $item) {
                $this->addEntityTask($entityType, $item['ID']);
                $count++;
            }

            $start += 50;

            // Простая проверка: если меньше 50 элементов - это последняя страница
            if (count($items) < 50) {
                $hasMore = false;
            }
        }

        return $count;
    }

    public function addEntityTask(string $entityType, string $cloudId): int
    {
        return $this->queueRepo->add([
            'UF_TYPE' => TaskType::ENTITY_CREATE,
            'UF_ENTITY_TYPE' => $entityType,
            'UF_CLOUD_ID' => $cloudId,
            'UF_STEP' => 'CREATE',
            'UF_STATUS' => 'NEW',
            'UF_NEXT_RUN_AT' => new DateTime(),
        ]);
    }

    public function addTimelineTask(string $entityType, string $cloudId, int $boxId): int
    {
        return $this->queueRepo->add([
            'UF_TYPE' => TaskType::TIMELINE_IMPORT,
            'UF_ENTITY_TYPE' => $entityType,
            'UF_CLOUD_ID' => $cloudId,
            'UF_BOX_ID' => $boxId,
            'UF_STEP' => 'IMPORT',
            'UF_STATUS' => 'NEW',
            'UF_NEXT_RUN_AT' => new DateTime(),
        ]);
    }
}
