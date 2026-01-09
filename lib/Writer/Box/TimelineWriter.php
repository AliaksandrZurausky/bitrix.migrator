<?php

namespace BitrixMigrator\Writer\Box;

use Bitrix\Main\Loader;
use Bitrix\Crm\Timeline\CommentEntry;
use Bitrix\Crm\ActivityTable;

final class TimelineWriter
{
    public function __construct()
    {
        Loader::includeModule('crm');
    }

    public function createComment(
        int $entityTypeId,
        int $entityId,
        string $text,
        int $authorId,
        ?\DateTime $createdAt = null
    ): int {
        $bindings = [[
            'ENTITY_TYPE_ID' => $entityTypeId,
            'ENTITY_ID' => $entityId,
        ]];

        $settings = [
            'TEXT' => $text,
            'AUTHOR_ID' => $authorId,
            'BINDINGS' => $bindings,
        ];

        if ($createdAt) {
            // Попытка установить дату (может не работать в зависимости от версии)
            $settings['CREATED'] = $createdAt->format('d.m.Y H:i:s');
        }

        $id = CommentEntry::create($settings);

        if (!$id) {
            throw new \RuntimeException('Failed to create timeline comment');
        }

        return $id;
    }

    public function createActivity(
        int $entityTypeId,
        int $entityId,
        array $activityData
    ): int {
        $fields = [
            'OWNER_TYPE_ID' => $entityTypeId,
            'OWNER_ID' => $entityId,
            'TYPE_ID' => $activityData['TYPE_ID'] ?? \CCrmActivityType::Call,
            'SUBJECT' => $activityData['SUBJECT'] ?? '',
            'DESCRIPTION' => $activityData['DESCRIPTION'] ?? '',
            'DESCRIPTION_TYPE' => $activityData['DESCRIPTION_TYPE'] ?? \CCrmContentType::PlainText,
            'DIRECTION' => $activityData['DIRECTION'] ?? \CCrmActivityDirection::Incoming,
            'RESPONSIBLE_ID' => $activityData['RESPONSIBLE_ID'] ?? 1,
            'COMPLETED' => $activityData['COMPLETED'] ?? 'Y',
            'STATUS' => $activityData['STATUS'] ?? \CCrmActivityStatus::Completed,
        ];

        if (isset($activityData['START_TIME'])) {
            $fields['START_TIME'] = $activityData['START_TIME'];
        }

        if (isset($activityData['END_TIME'])) {
            $fields['END_TIME'] = $activityData['END_TIME'];
        }

        $result = (new \CCrmActivity(false))->Add($fields);

        if (!$result) {
            global $APPLICATION;
            throw new \RuntimeException('Failed to create activity: ' . $APPLICATION->GetException()->GetString());
        }

        return (int)$result;
    }

    public function getEntityTypeIdByName(string $entityTypeName): int
    {
        $map = [
            'DEAL' => \CCrmOwnerType::Deal,
            'CONTACT' => \CCrmOwnerType::Contact,
            'COMPANY' => \CCrmOwnerType::Company,
            'LEAD' => \CCrmOwnerType::Lead,
        ];

        return $map[$entityTypeName] ?? 0;
    }
}
