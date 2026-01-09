<?php

namespace BitrixMigrator\Service\Import;

use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Integration\Cloud\Api\DealApi;
use BitrixMigrator\Writer\Box\CrmWriter;
use BitrixMigrator\Repository\Hl\MapRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
use BitrixMigrator\Service\QueueBuilder;
use BitrixMigrator\Domain\Enum\EntityType;

final class DealImporter
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

        // Проверка: уже перенесена?
        if ($this->mapRepo->exists(EntityType::DEAL, $cloudId)) {
            $this->logRepo->info('Deal already migrated, skipping', 'DealImporter', [
                'cloud_id' => $cloudId,
            ]);
            return;
        }

        // Получение данных из облака
        $dealApi = new DealApi($this->cloudClient);
        $cloudDeal = $dealApi->get((int)$cloudId);

        if (empty($cloudDeal)) {
            throw new \RuntimeException('Deal not found in cloud: ' . $cloudId);
        }

        // Нормализация полей
        $fields = $this->crmWriter->normalizeFields($cloudDeal, EntityType::DEAL);

        // Создание в коробке
        $boxId = $this->crmWriter->createDeal($fields);

        // Сохранение маппинга
        $this->mapRepo->add(EntityType::DEAL, $cloudId, $boxId);

        // Добавление задачи на перенос таймлайна
        $this->queueBuilder->addTimelineTask(EntityType::DEAL, $cloudId, $boxId);

        $this->logRepo->info('Deal migrated successfully', 'DealImporter', [
            'cloud_id' => $cloudId,
            'box_id' => $boxId,
        ]);
    }
}
