<?php

namespace BitrixMigrator\Writer\Box;

use Bitrix\Main\Loader;
use Bitrix\Crm\DealTable;
use Bitrix\Crm\ContactTable;
use Bitrix\Crm\CompanyTable;
use Bitrix\Crm\LeadTable;

final class CrmWriter
{
    public function __construct()
    {
        Loader::includeModule('crm');
    }

    public function createDeal(array $fields): int
    {
        $result = (new \CCrmDeal(false))->Add($fields);

        if (!$result) {
            global $APPLICATION;
            throw new \RuntimeException('Failed to create deal: ' . $APPLICATION->GetException()->GetString());
        }

        return (int)$result;
    }

    public function createContact(array $fields): int
    {
        $result = (new \CCrmContact(false))->Add($fields);

        if (!$result) {
            global $APPLICATION;
            throw new \RuntimeException('Failed to create contact: ' . $APPLICATION->GetException()->GetString());
        }

        return (int)$result;
    }

    public function createCompany(array $fields): int
    {
        $result = (new \CCrmCompany(false))->Add($fields);

        if (!$result) {
            global $APPLICATION;
            throw new \RuntimeException('Failed to create company: ' . $APPLICATION->GetException()->GetString());
        }

        return (int)$result;
    }

    public function createLead(array $fields): int
    {
        $result = (new \CCrmLead(false))->Add($fields);

        if (!$result) {
            global $APPLICATION;
            throw new \RuntimeException('Failed to create lead: ' . $APPLICATION->GetException()->GetString());
        }

        return (int)$result;
    }

    public function normalizeFields(array $cloudFields, string $entityType): array
    {
        $normalized = [];

        // Базовые поля
        $baseFields = ['TITLE', 'COMMENTS', 'ASSIGNED_BY_ID', 'OPENED', 'CURRENCY_ID'];
        foreach ($baseFields as $field) {
            if (isset($cloudFields[$field])) {
                $normalized[$field] = $cloudFields[$field];
            }
        }

        // UF-поля
        foreach ($cloudFields as $key => $value) {
            if (strpos($key, 'UF_') === 0) {
                $normalized[$key] = $value;
            }
        }

        // Специфичные поля по типам
        switch ($entityType) {
            case 'DEAL':
                $specificFields = ['OPPORTUNITY', 'STAGE_ID', 'TYPE_ID', 'PROBABILITY'];
                break;
            case 'CONTACT':
                $specificFields = ['NAME', 'LAST_NAME', 'SECOND_NAME', 'POST', 'BIRTHDATE'];
                break;
            case 'COMPANY':
                $specificFields = ['COMPANY_TYPE', 'INDUSTRY', 'EMPLOYEES', 'REVENUE'];
                break;
            case 'LEAD':
                $specificFields = ['NAME', 'LAST_NAME', 'SECOND_NAME', 'STATUS_ID', 'SOURCE_ID'];
                break;
            default:
                $specificFields = [];
        }

        foreach ($specificFields as $field) {
            if (isset($cloudFields[$field])) {
                $normalized[$field] = $cloudFields[$field];
            }
        }

        return $normalized;
    }
}
