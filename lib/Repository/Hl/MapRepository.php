<?php

namespace BitrixMigrator\Repository\Hl;

use Bitrix\Main\Type\DateTime;
use BitrixMigrator\Config\HlConfig;

final class MapRepository extends BaseHlRepository
{
    public function __construct()
    {
        parent::__construct(HlConfig::MAP_OPTION_KEY);
    }

    public function add(string $entity, string $cloudId, int $boxId, ?string $hash = null, ?string $meta = null): int
    {
        $data = [
            'UF_ENTITY' => $entity,
            'UF_CLOUD_ID' => $cloudId,
            'UF_BOX_ID' => $boxId,
            'UF_HASH' => $hash,
            'UF_META' => $meta,
            'UF_CREATED_AT' => new DateTime(),
        ];

        $result = $this->dataClass::add($data);
        if (!$result->isSuccess()) {
            throw new \RuntimeException('Failed to add mapping: ' . implode(', ', $result->getErrorMessages()));
        }

        return $result->getId();
    }

    public function findBoxId(string $entity, string $cloudId): ?int
    {
        $result = $this->dataClass::getList([
            'filter' => [
                'UF_ENTITY' => $entity,
                'UF_CLOUD_ID' => $cloudId,
            ],
            'limit' => 1,
        ])->fetch();

        return $result ? (int)$result['UF_BOX_ID'] : null;
    }

    public function exists(string $entity, string $cloudId): bool
    {
        return $this->findBoxId($entity, $cloudId) !== null;
    }
}
