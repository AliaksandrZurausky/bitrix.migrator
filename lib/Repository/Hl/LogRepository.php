<?php

namespace BitrixMigrator\Repository\Hl;

use BitrixMigrator\Config\HlConfig;

final class LogRepository extends BaseHlRepository
{
    protected function getHlTable(): string
    {
        return HlConfig::LOG_TABLE;
    }

    /**
     * Простое логирование
     */
    public function log(string $level, string $message, string $scope = '', ?string $entity = null, ?string $cloudId = null, ?int $boxId = null): int
    {
        $context = [
            'scope' => $scope,
            'timestamp' => date('Y-m-d H:i:s'),
        ];

        return $this->add([
            'UF_LEVEL' => $level,
            'UF_MESSAGE' => $message,
            'UF_SCOPE' => $scope,
            'UF_ENTITY' => $entity,
            'UF_CLOUD_ID' => $cloudId,
            'UF_BOX_ID' => $boxId,
            'UF_CONTEXT' => json_encode($context),
        ]);
    }

    /**
     * Логирование информации
     */
    public function info(string $message, string $scope = '', ?array $context = null): int
    {
        return $this->log('INFO', $message, $scope);
    }

    /**
     * Логирование предупреждения
     */
    public function warning(string $message, string $scope = '', ?array $context = null): int
    {
        return $this->log('WARNING', $message, $scope);
    }

    /**
     * Логирование ошибки
     */
    public function error(string $message, string $scope = '', ?array $context = null): int
    {
        return $this->log('ERROR', $message, $scope);
    }

    /**
     * Получить логи для вывода
     */
    public function getLogs(int $limit = 50, int $offset = 0, ?string $level = null): array
    {
        $entity = $this->getEntity();

        $query = $entity::query()
            ->setOrder(['ID' => 'DESC'])
            ->setLimit($limit)
            ->setOffset($offset);

        if ($level) {
            // Поддержка множественных валюей (ERROR,WARNING)
            $levels = array_map('trim', explode(',', $level));
            $query->where('UF_LEVEL', 'IN', $levels);
        }

        return $query->fetchAll();
    }

    /**
     * Получить количество логов
     */
    public function getLogsCount(?string $level = null): int
    {
        $entity = $this->getEntity();

        $query = $entity::query()
            ->countTotal(true);

        if ($level) {
            $levels = array_map('trim', explode(',', $level));
            $query->where('UF_LEVEL', 'IN', $levels);
        }

        return $query->exec()->getCount();
    }

    /**
     * Очистить старые логи (>Н дней)
     */
    public function cleanup(int $days = 30): int
    {
        $entity = $this->getEntity();
        $date = new \Bitrix\Main\Type\DateTime();
        $date->sub('P' . $days . 'D');

        $query = $entity::query()
            ->where('DATE_CREATE', '<', $date);

        $count = 0;
        foreach ($query->fetchAll() as $row) {
            try {
                $this->delete($row['ID']);
                $count++;
            } catch (\Exception $e) {
                // Игнорируем
            }
        }

        return $count;
    }

    /**
     * Получить логи сразу для админа
     */
    public function add(array $data): int
    {
        $entity = $this->getEntity();
        $result = $entity->add($data);

        if (!$result->isSuccess()) {
            throw new \RuntimeException(
                'Failed to add log: ' . implode(', ', $result->getErrorMessages())
            );
        }

        return $result->getId();
    }
}
