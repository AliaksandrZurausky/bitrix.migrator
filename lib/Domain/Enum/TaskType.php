<?php

namespace BitrixMigrator\Domain\Enum;

final class TaskType
{
    public const ENTITY_CREATE = 'ENTITY_CREATE';
    public const TIMELINE_IMPORT = 'TIMELINE_IMPORT';
    public const FILES_IMPORT = 'FILES_IMPORT';

    private function __construct()
    {
    }
}
