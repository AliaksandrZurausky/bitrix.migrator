<?php

namespace BitrixMigrator\Domain\Enum;

final class EntityType
{
    public const DEAL = 'DEAL';
    public const CONTACT = 'CONTACT';
    public const COMPANY = 'COMPANY';
    public const LEAD = 'LEAD';

    private function __construct()
    {
    }
}
