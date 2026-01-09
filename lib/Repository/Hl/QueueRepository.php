<?php

namespace BitrixMigrator\Repository\Hl;

use Bitrix\Main\Type\DateTime;
use BitrixMigrator\Config\HlConfig;

final class QueueRepository extends BaseHlRepository
{
    public function __construct()
    {
        parent::__construct(HlConfig::QUEUE_OPTION_KEY);
    }

    public function add(array $data): int
    {
        $data['UF_CREATED_AT'] = new DateTime();
        $data['UF_UPDATED_AT'] = new DateTime();
        $data['UF_STATUS'] = $data['UF_STATUS'] ?? 'NEW';
        $data['UF_RETRY'] = $data['UF_RETRY'] ?? 0;

        $result = $this->dataClass::add($data);
        if (!$result->isSuccess()) {
            throw new \RuntimeException('Failed to add queue task: ' . implode(', ', $result->getErrorMessages()));
        }

        return $result->getId();
    }

    public function update(int $id, array $data): void
    {
        $data['UF_UPDATED_AT'] = new DateTime();

        $result = $this->dataClass::update($id, $data);
        if (!$result->isSuccess()) {
            throw new \RuntimeException('Failed to update queue task: ' . implode(', ', $result->getErrorMessages()));
        }
    }

    public function getBatch(int $limit = 50): array
    {
        $result = $this->dataClass::getList([
            'filter' => [
                'UF_STATUS' => ['NEW', 'RETRY'],
                '<=UF_NEXT_RUN_AT' => new DateTime(),
            ],
            'order' => ['ID' => 'ASC'],
            'limit' => $limit,
        ]);

        $tasks = [];
        while ($row = $result->fetch()) {
            $tasks[] = $row;
        }

        return $tasks;
    }

    public function markAsRunning(int $id): void
    {
        $this->update($id, ['UF_STATUS' => 'RUNNING']);
    }

    public function markAsDone(int $id): void
    {
        $this->update($id, ['UF_STATUS' => 'DONE']);
    }

    public function markAsError(int $id, string $error): void
    {
        $this->update($id, [
            'UF_STATUS' => 'ERROR',
            'UF_ERROR' => $error,
        ]);
    }

    public function markAsRetry(int $id, string $error, int $retryCount): void
    {
        $nextRunAt = new DateTime();
        $nextRunAt->add('+' . (60 * $retryCount) . ' seconds');

        $this->update($id, [
            'UF_STATUS' => 'RETRY',
            'UF_ERROR' => $error,
            'UF_RETRY' => $retryCount,
            'UF_NEXT_RUN_AT' => $nextRunAt,
        ]);
    }
}
