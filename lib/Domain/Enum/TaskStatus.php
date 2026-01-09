<?php

namespace BitrixMigrator\Domain\Enum;

final class TaskStatus
{
    public const NEW = 'NEW';
    public const RUNNING = 'RUNNING';
    public const DONE = 'DONE';
    public const ERROR = 'ERROR';
    public const RETRY = 'RETRY';
    public const SKIP = 'SKIP';

    private function __construct()
    {
    }
}
