<?php

namespace BitrixMigrator\Service;

class QueueService
{
    public static function getStats()
    {
        // TODO: Получить статистику очереди
        return [
            'total' => 0,
            'completed' => 0,
            'pending' => 0,
            'errors' => 0,
        ];
    }
}
