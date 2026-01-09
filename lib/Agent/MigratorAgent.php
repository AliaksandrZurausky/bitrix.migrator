<?php

namespace BitrixMigrator\Agent;

use Bitrix\Main\Loader;
use Bitrix\Main\Config\Option;
use BitrixMigrator\Config\Module;
use BitrixMigrator\Repository\Hl\QueueRepository;
use BitrixMigrator\Repository\Hl\MapRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Writer\Box\CrmWriter;
use BitrixMigrator\Writer\Box\TimelineWriter;
use BitrixMigrator\Service\TaskRunner;
use BitrixMigrator\Service\QueueBuilder;
use BitrixMigrator\Service\Import\DealImporter;
use BitrixMigrator\Service\Import\ContactImporter;
use BitrixMigrator\Service\Import\CompanyImporter;
use BitrixMigrator\Service\Import\LeadImporter;
use BitrixMigrator\Service\Import\TimelineImporter;
use BitrixMigrator\Domain\Enum\TaskType;

final class MigratorAgent
{
    /**
     * Агент миграции.
     *
     * Контракт агентов Битрикса: метод должен вернуть строку вызова самого себя,
     * чтобы агент продолжил выполняться, или пустую строку для самоудаления.
     */
    public static function run(): string
    {
        try {
            if (!Loader::includeModule(Module::ID)) {
                return "\\" . __CLASS__ . "::run();";
            }

            // Проверка флага включения миграции
            $enabled = Option::get(Module::ID, 'MIGRATION_ENABLED', 'N');
            if ($enabled !== 'Y') {
                return "\\" . __CLASS__ . "::run();";
            }

            // Инициализация зависимостей
            $queueRepo = new QueueRepository();
            $mapRepo = new MapRepository();
            $logRepo = new LogRepository();

            $cloudClient = new RestClient();
            $crmWriter = new CrmWriter();
            $timelineWriter = new TimelineWriter();
            $queueBuilder = new QueueBuilder($queueRepo, $cloudClient);

            // Импортеры
            $importers = [
                TaskType::ENTITY_CREATE => new DealImporter($cloudClient, $crmWriter, $mapRepo, $logRepo, $queueBuilder),
                TaskType::TIMELINE_IMPORT => new TimelineImporter($cloudClient, $timelineWriter, $logRepo),
            ];

            // Поддержка других сущностей (для Contact/Company/Lead используется тот же тип ENTITY_CREATE)
            // Логика разделения по entityType делается внутри импортера либо через factory

            $taskRunner = new TaskRunner($queueRepo, $logRepo, $importers);

            // Выполнение пачки
            $batchSize = (int)Option::get(Module::ID, 'BATCH_SIZE', 50);
            $taskRunner->runBatch($batchSize);
        } catch (\Exception $e) {
            // Логирование ошибки агента
            if (class_exists('BitrixMigrator\Repository\Hl\LogRepository')) {
                try {
                    $logRepo = new LogRepository();
                    $logRepo->error('Agent execution failed: ' . $e->getMessage(), 'MigratorAgent');
                } catch (\Exception $logException) {
                    // Игнорируем, чтобы не ломать агент
                }
            }
        }

        return "\\" . __CLASS__ . "::run();";
    }
}
