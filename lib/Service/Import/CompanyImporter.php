<?php

namespace BitrixMigrator\Service\Import;

use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Integration\Cloud\Api\CompanyApi;
use BitrixMigrator\Writer\Box\CrmWriter;
use BitrixMigrator\Repository\Hl\MapRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
use BitrixMigrator\Service\QueueBuilder;
use BitrixMigrator\Domain\Enum\EntityType;

final class CompanyImporter
{
    private RestClient $cloudClient;
    private CrmWriter $crmWriter;
    private MapRepository $mapRepo;
    private LogRepository $logRepo;
    private QueueBuilder $queueBuilder;

    public function __construct(
        RestClient $cloudClient,
        CrmWriter $crmWriter,
        MapRepository $mapRepo,
        LogRepository $logRepo,
        QueueBuilder $queueBuilder
    ) {
        $this->cloudClient = $cloudClient;
        $this->crmWriter = $crmWriter;
        $this->mapRepo = $mapRepo;
        $this->logRepo = $logRepo;
        $this->queueBuilder = $queueBuilder;
    }

    public function import(array $task): void
    {
        $cloudId = $task['UF_CLOUD_ID'];

        if ($this->mapRepo->exists(EntityType::COMPANY, $cloudId)) {
            return;
        }

        $companyApi = new CompanyApi($this->cloudClient);
        $cloudCompany = $companyApi->get((int)$cloudId);

        if (empty($cloudCompany)) {
            throw new \RuntimeException('Company not found in cloud: ' . $cloudId);
        }

        $fields = $this->crmWriter->normalizeFields($cloudCompany, EntityType::COMPANY);
        $boxId = $this->crmWriter->createCompany($fields);

        $this->mapRepo->add(EntityType::COMPANY, $cloudId, $boxId);
        $this->queueBuilder->addTimelineTask(EntityType::COMPANY, $cloudId, $boxId);

        $this->logRepo->info('Company migrated successfully', 'CompanyImporter', [
            'cloud_id' => $cloudId,
            'box_id' => $boxId,
        ]);
    }
}
