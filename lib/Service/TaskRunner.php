<?php

namespace BitrixMigrator\Service;

final class TaskRunner
{
    /**
     * Выполнить одну пачку задач.
     *
     * @return int Количество успешно обработанных задач.
     */
    public function runBatch(int $limit = 50): int
    {
        // На этапе каркаса возвращаем 0.
        return 0;
    }
}
