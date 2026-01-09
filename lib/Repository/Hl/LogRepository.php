<?php

namespace BitrixMigrator\Repository\Hl;

use Bitrix\Main\Type\DateTime;
use BitrixMigrator\Config\HlConfig;

final class LogRepository extends BaseHlRepository
{
    public function __construct()
    {
        parent::__construct(HlConfig::LOG_OPTION_KEY);
    }

    public function log(string $level, string $message, ?string $scope = null, ?array $context = null): void
    {
        $data = [
            'UF_LEVEL' => $level,
            'UF_MESSAGE' => $message,
            'UF_SCOPE' => $scope,
            'UF_CONTEXT' => $context ? json_encode($context, JSON_UNESCAPED_UNICODE) : null,
            'UF_ENTITY' => $context['entity'] ?? null,
            'UF_CLOUD_ID' => $context['cloud_id'] ?? null,
            'UF_BOX_ID' => $context['box_id'] ?? null,
            'UF_CREATED_AT' => new DateTime(),
        ];

        $this->dataClass::add($data);
    }

    public function info(string $message, ?string $scope = null, ?array $context = null): void
    {
        $this->log('INFO', $message, $scope, $context);
    }

    public function error(string $message, ?string $scope = null, ?array $context = null): void
    {
        $this->log('ERROR', $message, $scope, $context);
    }

    public function warning(string $message, ?string $scope = null, ?array $context = null): void
    {
        $this->log('WARNING', $message, $scope, $context);
    }
}
