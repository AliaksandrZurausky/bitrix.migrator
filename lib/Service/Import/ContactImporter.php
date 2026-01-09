<?php

namespace BitrixMigrator\Service\Import;

use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Integration\Cloud\Api\ContactApi;
use BitrixMigrator\Writer\Box\CrmWriter;
use BitrixMigrator\Repository\Hl\MapRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
use BitrixMigrator\Service\QueueBuilder;
use BitrixMigrator\Domain\Enum\EntityType;

final class ContactImporter
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

        if ($this->mapRepo->exists(EntityType::CONTACT, $cloudId)) {
            return;
        }

        $contactApi = new ContactApi($this->cloudClient);
        $cloudContact = $contactApi->get((int)$cloudId);

        if (empty($cloudContact)) {
            throw new \RuntimeException('Contact not found in cloud: ' . $cloudId);
        }

        $fields = $this->crmWriter->normalizeFields($cloudContact, EntityType::CONTACT);
        $boxId = $this->crmWriter->createContact($fields);

        $this->mapRepo->add(EntityType::CONTACT, $cloudId, $boxId);
        $this->queueBuilder->addTimelineTask(EntityType::CONTACT, $cloudId, $boxId);

        $this->logRepo->info('Contact migrated successfully', 'ContactImporter', [
            'cloud_id' => $cloudId,
            'box_id' => $boxId,
        ]);
    }
}
