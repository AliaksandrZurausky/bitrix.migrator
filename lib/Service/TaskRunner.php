<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Repository\Hl\QueueRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
use BitrixMigrator\Domain\Enum\TaskType;
use BitrixMigrator\Service\Import\DealImporter;
use BitrixMigrator\Service\Import\ContactImporter;
use BitrixMigrator\Service\Import\CompanyImporter;
use BitrixMigrator\Service\Import\LeadImporter;
use BitrixMigrator\Service\Import\TimelineImporter;

final class TaskRunner
{
    private QueueRepository $queueRepo;
    private LogRepository $logRepo;

    private array $importers = [];

    public function __construct(
        QueueRepository $queueRepo,
        LogRepository $logRepo,
        array $importers = []
    ) {
        $this->queueRepo = $queueRepo;
        $this->logRepo = $logRepo;
        $this->importers = $importers;
    }

    public function runBatch(int $limit = 50): int
    {
        $tasks = $this->queueRepo->getBatch($limit);

        if (empty($tasks)) {
            return 0;
        }

        $processed = 0;

        foreach ($tasks as $task) {
            try {
                $this->queueRepo->markAsRunning($task['ID']);
                $this->processTask($task);
                $this->queueRepo->markAsDone($task['ID']);
                $processed++;
            } catch (\Exception $e) {
                $retryCount = (int)($task['UF_RETRY'] ?? 0) + 1;

                if ($retryCount >= 3) {
                    $this->queueRepo->markAsError($task['ID'], $e->getMessage());
                    $this->logRepo->error('Task failed after retries', 'TaskRunner', [
                        'task_id' => $task['ID'],
                        'error' => $e->getMessage(),
                    ]);
                } else {
                    $this->queueRepo->markAsRetry($task['ID'], $e->getMessage(), $retryCount);
                }
            }
        }

        return $processed;
    }

    private function processTask(array $task): void
    {
        $taskType = $task['UF_TYPE'];

        if (!isset($this->importers[$taskType])) {
            throw new \RuntimeException('No importer found for task type: ' . $taskType);
        }

        $importer = $this->importers[$taskType];
        $importer->import($task);
    }
}
