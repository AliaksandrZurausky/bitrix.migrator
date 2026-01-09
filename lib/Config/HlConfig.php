<?php

namespace BitrixMigrator\Config;

final class HlConfig
{
    public const QUEUE_NAME = 'BitrixMigratorQueue';
    public const MAP_NAME = 'BitrixMigratorMap';
    public const LOG_NAME = 'BitrixMigratorLog';

    public const QUEUE_OPTION_KEY = 'HL_BitrixMigratorQueue';
    public const MAP_OPTION_KEY = 'HL_BitrixMigratorMap';
    public const LOG_OPTION_KEY = 'HL_BitrixMigratorLog';

    private function __construct()
    {
    }
}
