<?php

namespace BitrixMigrator\Service\Import;

use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Integration\Cloud\Api\LeadApi;
use BitrixMigrator\Writer\Box\CrmWriter;
use BitrixMigrator\Repository\Hl\MapRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
use BitrixMigrator\Service\QueueBuilder;
use BitrixMigrator\Domain\Enum\EntityType;

final class LeadImporter
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

        if ($this->mapRepo->exists(EntityType::LEAD, $cloudId)) {
            return;
        }

        $leadApi = new LeadApi($this->cloudClient);
        $cloudLead = $leadApi->get((int)$cloudId);

        if (empty($cloudLead)) {
            throw new \RuntimeException('Lead not found in cloud: ' . $cloudId);
        }

        $fields = $this->crmWriter->normalizeFields($cloudLead, EntityType::LEAD);
        $boxId = $this->crmWriter->createLead($fields);

        $this->mapRepo->add(EntityType::LEAD, $cloudId, $boxId);
        $this->queueBuilder->addTimelineTask(EntityType::LEAD, $cloudId, $boxId);

        $this->logRepo->info('Lead migrated successfully', 'LeadImporter', [
            'cloud_id' => $cloudId,
            'box_id' => $boxId,
        ]);
    }
}
