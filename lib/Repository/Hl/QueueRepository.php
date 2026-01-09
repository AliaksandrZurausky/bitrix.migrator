<?php

namespace BitrixMigrator\Repository\Hl;

use BitrixMigrator\Config\HlConfig;
use Bitrix\Main\ORM\Query\Query;

final class QueueRepository extends BaseHlRepository
{
    protected function getHlTable(): string
    {
        return HlConfig::QUEUE_TABLE;
    }

    /**
     * Добавить новую задачу в очередь
     */
    public function add(array $data): int
    {
        $entity = $this->getEntity();
        $result = $entity->add($data);

        if (!$result->isSuccess()) {
            throw new \RuntimeException(
                'Failed to add task: ' . implode(', ', $result->getErrorMessages())
            );
        }

        return $result->getId();
    }

    /**
     * Получить порцию задач со статусом NEW/RETRY
     */
    public function getBatch(int $limit = 50): array
    {
        $entity = $this->getEntity();

        $query = $entity::query()
            ->where('UF_STATUS', 'IN', ['NEW', 'RETRY'])
            ->where('UF_NEXT_RUN_AT', '<=', new \Bitrix\Main\Type\DateTime())
            ->setOrder(['ID' => 'ASC'])
            ->setLimit($limit);

        return $query->fetchAll();
    }

    /**
     * Пометить задачу как выполняющаяся
     */
    public function markAsRunning(int $id): void
    {
        $this->update($id, [
            'UF_STATUS' => 'RUNNING',
        ]);
    }

    /**
     * Пометить задачу как завершённую
     */
    public function markAsDone(int $id): void
    {
        $this->update($id, [
            'UF_STATUS' => 'DONE',
        ]);
    }

    /**
     * Пометить задачу как ошибку
     */
    public function markAsError(int $id, string $error): void
    {
        $this->update($id, [
            'UF_STATUS' => 'ERROR',
            'UF_ERROR' => $error,
        ]);
    }

    /**
     * Пометить для повторной попытки
     */
    public function markAsRetry(int $id, string $error, int $retryCount): void
    {
        $nextRun = new \Bitrix\Main\Type\DateTime();
        $nextRun->add('PT' . (60 * $retryCount) . 'S'); // Экспоненциальный бэкофф

        $this->update($id, [
            'UF_STATUS' => 'RETRY',
            'UF_ERROR' => $error,
            'UF_RETRY' => $retryCount,
            'UF_NEXT_RUN_AT' => $nextRun,
        ]);
    }

    /**
     * Получить общее количество задач
     */
    public function getTotal(): int
    {
        $entity = $this->getEntity();
        return $entity::query()
            ->countTotal(true)
            ->exec()
            ->getCount();
    }

    /**
     * Получить количество завершённых задач
     */
    public function getCompleted(): int
    {
        $entity = $this->getEntity();
        return $entity::query()
            ->where('UF_STATUS', 'DONE')
            ->countTotal(true)
            ->exec()
            ->getCount();
    }

    /**
     * Получить количество задач в очереди
     */
    public function getPending(): int
    {
        $entity = $this->getEntity();
        return $entity::query()
            ->where('UF_STATUS', 'IN', ['NEW', 'RETRY', 'RUNNING'])
            ->countTotal(true)
            ->exec()
            ->getCount();
    }

    /**
     * Получить количество задач с ошибками
     */
    public function getErrors(): int
    {
        $entity = $this->getEntity();
        return $entity::query()
            ->where('UF_STATUS', 'ERROR')
            ->countTotal(true)
            ->exec()
            ->getCount();
    }

    /**
     * Получить статистику по типам сущностей
     */
    public function getStatsByEntityType(): array
    {
        $entity = $this->getEntity();
        
        // Примечание: это упрощённая реализация
        // В реальности может потребоваться более сложный SQL
        $rows = $entity::query()
            ->setSelect(['UF_ENTITY_TYPE', 'COUNT' => new \Bitrix\Main\ORM\Query\Expression('COUNT(*)')])
            ->setGroup(['UF_ENTITY_TYPE'])
            ->fetchAll();
        
        $stats = [];
        foreach ($rows as $row) {
            $stats[$row['UF_ENTITY_TYPE']] = $row['COUNT'];
        }
        
        return $stats;
    }

    /**
     * Очистить завершённые задачи старше N дней
     */
    public function cleanupCompleted(int $days = 7): int
    {
        $entity = $this->getEntity();
        $date = new \Bitrix\Main\Type\DateTime();
        $date->sub('P' . $days . 'D');

        $query = $entity::query()
            ->where('UF_STATUS', 'DONE')
            ->where('DATE_CREATE', '<', $date);

        $count = 0;
        foreach ($query->fetchAll() as $row) {
            try {
                $this->delete($row['ID']);
                $count++;
            } catch (\Exception $e) {
                // Игнорируем ошибки удаления
            }
        }

        return $count;
    }
}
