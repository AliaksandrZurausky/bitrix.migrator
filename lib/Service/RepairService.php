<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Integration\CloudAPI;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

class RepairService
{
    private $cloudAPI;
    private $boxAPI;
    private $moduleId = 'bitrix_migrator';
    private $logFile = null;
    private $errorLogFile = null;

    private $ufFieldSchema = [];
    private $ufEnumMap = [];
    private $userMapCache = [];
    private $companyMapCache = [];
    private $contactMapCache = [];
    private $dealMapCache = [];
    private $leadMapCache = [];
    private $pipelineMapCache = [];
    private $stageMapCache = [];

    public function __construct(CloudAPI $cloudAPI, CloudAPI $boxAPI)
    {
        $this->cloudAPI = $cloudAPI;
        $this->boxAPI = $boxAPI;
    }

    public function setLogFile(string $logFile): void
    {
        $this->logFile = $logFile;
        $this->errorLogFile = preg_replace('/\.log$/', '-errors.log', $logFile);
    }

    public function addLog(string $message): void
    {
        $line = date('H:i:s') . ' ' . $message;
        if ($this->logFile) {
            @file_put_contents($this->logFile, date('Y-m-d H:i:s') . ' ' . $message . "\n", FILE_APPEND);
        }
        Option::set($this->moduleId, 'repair_message', $line);
    }

    // ------------------------------------------------------------------
    // Main entry point
    // ------------------------------------------------------------------

    public function repair(array $selectedTypes): void
    {
        Option::set($this->moduleId, 'repair_status', 'running');
        $this->addLog('=== Начало дозаполнения ===');

        try {
            $this->restoreCaches();

            // Rebuild mappings run without pre-counted totals (cloud count unknown)
            $rebuildTypes = array_filter($selectedTypes, fn($t) => strpos($t, 'rebuild_mappings_') === 0);
            $repairTypes = array_diff($selectedTypes, $rebuildTypes);

            // Phase 1: Rebuild mappings (if requested)
            foreach ($rebuildTypes as $type) {
                $this->checkStop();
                $entityType = str_replace('rebuild_mappings_', '', $type);
                $entityType = rtrim($entityType, 's'); // companies → company
                $this->rebuildMappings($entityType);
            }

            // Restore caches after potential mapping rebuild
            if (!empty($rebuildTypes)) {
                $this->restoreCaches();
            }

            // Phase 2: Repair operations
            $total = 0;
            $done = 0;
            foreach ($repairTypes as $type) {
                if (in_array($type, ['companies', 'contacts', 'deals', 'leads'])) {
                    $total += count(MapService::getAllMappings(rtrim($type, 's')));
                } elseif ($type === 'requisites_companies') {
                    $total += count(MapService::getAllMappings('company'));
                } elseif ($type === 'requisites_contacts') {
                    $total += count(MapService::getAllMappings('contact'));
                } elseif ($type === 'bindings') {
                    $total += count(MapService::getAllMappings('deal'));
                    $total += count(MapService::getAllMappings('contact'));
                } elseif ($type === 'bindings_companies') {
                    $total += count(MapService::getAllMappings('company'));
                }
            }
            if ($total > 0) $this->addLog("Элементов для обработки: $total");

            foreach ($repairTypes as $type) {
                $this->checkStop();
                switch ($type) {
                    case 'companies':
                    case 'contacts':
                    case 'deals':
                    case 'leads':
                        $done += $this->repairFields(rtrim($type, 's'), $done, $total);
                        break;
                    case 'requisites_companies':
                        $done += $this->repairRequisites($done, $total, 'company');
                        break;
                    case 'requisites_contacts':
                        $done += $this->repairRequisites($done, $total, 'contact');
                        break;
                    case 'bindings':
                        $done += $this->repairBindings($done, $total);
                        break;
                    case 'bindings_companies':
                        $done += $this->repairCompanyBindings($done, $total);
                        break;
                }
            }

            $this->addLog('=== Дозаполнение завершено ===');
            Option::set($this->moduleId, 'repair_status', 'completed');
        } catch (\Throwable $e) {
            $this->addLog('ОШИБКА: ' . $e->getMessage());
            Option::set($this->moduleId, 'repair_status', 'error');
            Option::set($this->moduleId, 'repair_message', $e->getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Repair: CRM fields (company, contact, deal, lead)
    // ------------------------------------------------------------------

    private function repairFields(string $entityType, int $offset, int $total): int
    {
        $mappings = MapService::getAllMappings($entityType);
        $this->addLog("--- Дозаполнение полей: $entityType (" . count($mappings) . " шт) ---");

        $apiGetMethod = 'crm.' . $entityType . '.get';
        $processed = 0;
        $updated = 0;
        $errors = 0;

        foreach ($mappings as $cloudId => $boxId) {
            $this->checkStop();
            $processed++;
            $this->saveProgress($offset + $processed, $total);
            if ($processed % 20 === 0) {
                $this->addLog("  $entityType: обработано $processed/" . count($mappings) . " (обновлено=$updated, ошибок=$errors)");
            }

            try {
                $this->rateLimit();
                $cloud = $this->cloudAPI->request($apiGetMethod, ['id' => (int)$cloudId]);
                $entity = $cloud['result'] ?? null;
                if (!$entity) continue;

                $fields = $this->buildFieldsForRepair($entityType, $entity);
                if (empty($fields)) continue;

                $updateMethod = 'update' . ucfirst($entityType);
                BoxD7Service::$updateMethod((int)$boxId, $fields);
                $updated++;
            } catch (\Throwable $e) {
                $errors++;
                if ($errors <= 10) {
                    $this->addLog("  Ошибка $entityType cloud#$cloudId: " . $e->getMessage());
                }
            }
        }

        $this->addLog("$entityType: обновлено=$updated, ошибок=$errors из " . count($mappings));
        return $processed;
    }

    private function buildFieldsForRepair(string $entityType, array $entity): array
    {
        $fields = [];

        // Copy standard fields based on entity type
        $fieldSets = [
            'company' => ['TITLE', 'COMPANY_TYPE', 'INDUSTRY', 'EMPLOYEES', 'REVENUE', 'CURRENCY_ID',
                          'COMMENTS', 'OPENED', 'IS_MY_COMPANY', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                          'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                          'SOURCE_ID', 'SOURCE_DESCRIPTION', 'BANKING_DETAILS'],
            'contact' => ['NAME', 'LAST_NAME', 'SECOND_NAME', 'POST', 'TYPE_ID', 'SOURCE_ID',
                          'SOURCE_DESCRIPTION', 'COMMENTS', 'OPENED', 'EXPORT',
                          'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                          'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                          'BIRTHDATE', 'HONORIFIC'],
            'deal'    => ['TITLE', 'TYPE_ID', 'PROBABILITY', 'CURRENCY_ID', 'OPPORTUNITY',
                          'TAX_VALUE', 'COMMENTS', 'OPENED', 'CLOSED', 'BEGINDATE', 'CLOSEDATE',
                          'SOURCE_ID', 'SOURCE_DESCRIPTION', 'ADDITIONAL_INFO'],
            'lead'    => ['TITLE', 'NAME', 'LAST_NAME', 'SECOND_NAME', 'STATUS_ID',
                          'SOURCE_ID', 'SOURCE_DESCRIPTION', 'CURRENCY_ID', 'OPPORTUNITY', 'COMMENTS', 'OPENED',
                          'COMPANY_TITLE', 'POST', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                          'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY'],
        ];

        $copyKeys = $fieldSets[$entityType] ?? [];
        foreach ($copyKeys as $k) {
            if (isset($entity[$k]) && $entity[$k] !== '') {
                $fields[$k] = $entity[$k];
            }
        }

        // Map ASSIGNED_BY_ID
        if (!empty($entity['ASSIGNED_BY_ID'])) {
            $boxUser = $this->userMapCache[(int)$entity['ASSIGNED_BY_ID']] ?? null;
            if ($boxUser) $fields['ASSIGNED_BY_ID'] = $boxUser;
        }

        // UF fields
        $this->copyUfFields($entityType, $entity, $fields);

        // Multifields (PHONE, EMAIL)
        if (in_array($entityType, ['company', 'contact', 'lead'])) {
            $fm = $this->packMultifields($entity);
            if (!empty($fm)) $fields['FM'] = $fm;
        }

        return $fields;
    }

    // ------------------------------------------------------------------
    // Repair: Requisites
    // ------------------------------------------------------------------

    private function repairRequisites(int $offset, int $total, string $scope = 'all'): int
    {
        $presetMap = $this->buildRequisitePresetMap();
        $this->addLog("  Маппинг пресетов: " . json_encode($presetMap));

        $entityMap = [];
        if ($scope === 'all' || $scope === 'company') {
            $entityMap[4] = ['name' => 'company', 'mappings' => MapService::getAllMappings('company')];
        }
        if ($scope === 'all' || $scope === 'contact') {
            $entityMap[3] = ['name' => 'contact', 'mappings' => MapService::getAllMappings('contact')];
        }

        $processed = 0;
        $totalReq = 0;
        $totalBank = 0;
        $errors = 0;

        foreach ($entityMap as $typeId => $cfg) {
            $this->addLog("Реквизиты: обработка {$cfg['name']} (" . count($cfg['mappings']) . " шт)...");

            foreach ($cfg['mappings'] as $cloudEntityId => $boxEntityId) {
                $this->checkStop();
                $processed++;
                $this->saveProgress($offset + $processed, $total);

                try {
                    $this->rateLimit();
                    $requisites = $this->cloudAPI->getRequisites($typeId, (int)$cloudEntityId);
                    if (empty($requisites)) continue;

                    // Check which requisites already exist on box for this entity
                    $existingReqs = [];
                    try {
                        $requisiteObj = new \Bitrix\Crm\EntityRequisite();
                        $dbRes = $requisiteObj->getList([
                            'filter' => ['ENTITY_TYPE_ID' => $typeId, 'ENTITY_ID' => (int)$boxEntityId],
                            'select' => ['ID', 'PRESET_ID', 'NAME'],
                        ]);
                        while ($row = $dbRes->fetch()) {
                            $existingReqs[] = $row;
                        }
                    } catch (\Throwable $e) {}

                    foreach ($requisites as $req) {
                        $this->rateLimit();
                        $cloudReqId = (int)$req['ID'];
                        $cloudPresetId = (int)($req['PRESET_ID'] ?? 0);
                        unset($req['ID'], $req['DATE_CREATE'], $req['DATE_MODIFY'],
                              $req['CREATED_BY_ID'], $req['MODIFY_BY_ID']);
                        $req['ENTITY_TYPE_ID'] = $typeId;
                        $req['ENTITY_ID'] = (int)$boxEntityId;

                        // Remap PRESET_ID
                        $boxPresetId = 0;
                        if ($cloudPresetId > 0 && isset($presetMap[$cloudPresetId])) {
                            $req['PRESET_ID'] = $presetMap[$cloudPresetId];
                            $boxPresetId = $presetMap[$cloudPresetId];
                        }

                        // Skip if requisite with same PRESET_ID already exists
                        $alreadyExists = false;
                        foreach ($existingReqs as $er) {
                            if ((int)$er['PRESET_ID'] === $boxPresetId) {
                                $alreadyExists = true;
                                break;
                            }
                        }
                        if ($alreadyExists) continue;

                        try {
                            $requisiteObj = new \Bitrix\Crm\EntityRequisite();
                            $result = $requisiteObj->add($req);
                            if ($result->isSuccess()) {
                                $boxReqId = $result->getId();
                                $totalReq++;

                                // Addresses
                                $this->repairRequisiteAddresses($cloudReqId, $boxReqId);

                                // Bank details
                                $this->repairBankDetails($cloudReqId, $boxReqId, $boxPresetId, $totalBank, $errors);
                            } else {
                                $errors++;
                                $this->addLog("  Ошибка реквизита ({$cfg['name']} box#$boxEntityId): "
                                    . implode('; ', $result->getErrorMessages()));
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            $this->addLog("  Ошибка реквизита ({$cfg['name']} box#$boxEntityId): " . $e->getMessage());
                        }
                    }
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка получения реквизитов ({$cfg['name']} cloud#$cloudEntityId): " . $e->getMessage());
                }
            }
        }

        $this->addLog("Реквизиты: создано=$totalReq, банковских=$totalBank, ошибок=$errors");
        return $processed;
    }

    private function repairRequisiteAddresses(int $cloudReqId, int $boxReqId): void
    {
        try {
            $addresses = $this->cloudAPI->getRequisiteAddresses($cloudReqId);
            foreach ($addresses as $addr) {
                $typeIdAddr = (int)($addr['TYPE_ID'] ?? 1);
                $addrFields = [
                    'ADDRESS_1'    => $addr['ADDRESS_1'] ?? '',
                    'ADDRESS_2'    => $addr['ADDRESS_2'] ?? '',
                    'CITY'         => $addr['CITY'] ?? '',
                    'POSTAL_CODE'  => $addr['POSTAL_CODE'] ?? '',
                    'REGION'       => $addr['REGION'] ?? '',
                    'PROVINCE'     => $addr['PROVINCE'] ?? '',
                    'COUNTRY'      => $addr['COUNTRY'] ?? '',
                    'COUNTRY_CODE' => $addr['COUNTRY_CODE'] ?? '',
                ];
                try {
                    \Bitrix\Crm\EntityAddress::register(
                        \CCrmOwnerType::Requisite,
                        $boxReqId,
                        $typeIdAddr,
                        $addrFields
                    );
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка адреса реквизита #$boxReqId: " . $e->getMessage());
                }
            }
        } catch (\Throwable $e) { /* non-critical */ }
    }

    private function repairBankDetails(int $cloudReqId, int $boxReqId, int $boxPresetId, int &$totalBank, int &$errors): void
    {
        try {
            $bankDetails = $this->cloudAPI->getBankDetails($cloudReqId);
            foreach ($bankDetails as $bank) {
                $this->rateLimit();
                unset($bank['ID'], $bank['DATE_CREATE'], $bank['DATE_MODIFY'],
                      $bank['CREATED_BY_ID'], $bank['MODIFY_BY_ID']);
                $bank['ENTITY_ID'] = $boxReqId;
                $bank['ENTITY_TYPE_ID'] = \CCrmOwnerType::Requisite;
                try {
                    $bankObj = new \Bitrix\Crm\EntityBankDetail();
                    $bankResult = $bankObj->add($bank, [
                        'FIELD_CHECK_OPTIONS' => ['PRESET_ID' => $boxPresetId],
                    ]);
                    if ($bankResult->isSuccess()) {
                        $totalBank++;
                    } else {
                        $errors++;
                        $this->addLog("  Ошибка банк.реквизита: " . implode('; ', $bankResult->getErrorMessages()));
                    }
                } catch (\Throwable $e) {
                    $errors++;
                    $this->addLog("  Ошибка банк.реквизита: " . $e->getMessage());
                }
            }
        } catch (\Throwable $e) { /* non-critical */ }
    }

    // ------------------------------------------------------------------
    // Repair: M:N Bindings (deal↔contacts, contact↔companies)
    // ------------------------------------------------------------------

    private function repairBindings(int $offset, int $total): int
    {
        $processed = 0;
        $fixed = 0;

        // Deal ↔ Contacts
        $dealMappings = MapService::getAllMappings('deal');
        $this->addLog("--- Дозаполнение связей: deals (" . count($dealMappings) . " шт) ---");

        foreach ($dealMappings as $cloudDealId => $boxDealId) {
            $this->checkStop();
            $processed++;
            $this->saveProgress($offset + $processed, $total);

            try {
                $this->rateLimit();
                $items = $this->cloudAPI->request('crm.deal.contact.items.get', ['id' => (int)$cloudDealId]);
                $contactItems = $items['result'] ?? [];
                if (empty($contactItems)) continue;

                $boxContactIds = [];
                foreach ($contactItems as $ci) {
                    $cid = (int)($ci['CONTACT_ID'] ?? 0);
                    if ($cid > 0 && isset($this->contactMapCache[$cid])) {
                        $boxContactIds[] = $this->contactMapCache[$cid];
                    }
                }
                if (!empty($boxContactIds)) {
                    BoxD7Service::setDealContacts((int)$boxDealId, $boxContactIds);
                    $fixed++;
                }
            } catch (\Throwable $e) {}
        }

        // Contact ↔ Companies
        $contactMappings = MapService::getAllMappings('contact');
        $this->addLog("--- Дозаполнение связей: contacts (" . count($contactMappings) . " шт) ---");

        foreach ($contactMappings as $cloudContactId => $boxContactId) {
            $this->checkStop();
            $processed++;
            $this->saveProgress($offset + $processed, $total);

            try {
                $this->rateLimit();
                $items = $this->cloudAPI->request('crm.contact.company.items.get', ['id' => (int)$cloudContactId]);
                $companyItems = $items['result'] ?? [];
                if (empty($companyItems)) continue;

                $boxCompanyIds = [];
                foreach ($companyItems as $ci) {
                    $cid = (int)($ci['COMPANY_ID'] ?? 0);
                    if ($cid > 0 && isset($this->companyMapCache[$cid])) {
                        $boxCompanyIds[] = $this->companyMapCache[$cid];
                    }
                }
                if (!empty($boxCompanyIds)) {
                    BoxD7Service::setContactCompanies((int)$boxContactId, $boxCompanyIds);
                    $fixed++;
                }
            } catch (\Throwable $e) {}
        }

        $this->addLog("Связи: исправлено=$fixed");
        return $processed;
    }

    // ------------------------------------------------------------------
    // Repair: Company → Deal/Lead bindings
    // ------------------------------------------------------------------

    private function repairCompanyBindings(int $offset, int $total): int
    {
        $companyMappings = MapService::getAllMappings('company');
        $this->addLog("--- Привязки компаний к сделкам/лидам (" . count($companyMappings) . " шт) ---");

        $processed = 0;
        $fixedDeals = 0;
        $fixedLeads = 0;

        foreach ($companyMappings as $cloudCompanyId => $boxCompanyId) {
            $this->checkStop();
            $processed++;
            $this->saveProgress($offset + $processed, $total);

            // Deals linked to this company in the cloud
            try {
                $this->rateLimit();
                $cloudDeals = $this->cloudAPI->request('crm.deal.list', [
                    'filter' => ['COMPANY_ID' => (int)$cloudCompanyId],
                    'select' => ['ID'],
                ]);
                foreach (($cloudDeals['result'] ?? []) as $d) {
                    $cloudDealId = (int)($d['ID'] ?? 0);
                    if ($cloudDealId > 0 && isset($this->dealMapCache[$cloudDealId])) {
                        $boxDealId = (int)$this->dealMapCache[$cloudDealId];
                        try {
                            BoxD7Service::updateDeal($boxDealId, ['COMPANY_ID' => (int)$boxCompanyId]);
                            $fixedDeals++;
                        } catch (\Throwable $e) {}
                    }
                }
            } catch (\Throwable $e) {}

            // Leads linked to this company in the cloud
            try {
                $this->rateLimit();
                $cloudLeads = $this->cloudAPI->request('crm.lead.list', [
                    'filter' => ['COMPANY_ID' => (int)$cloudCompanyId],
                    'select' => ['ID'],
                ]);
                foreach (($cloudLeads['result'] ?? []) as $l) {
                    $cloudLeadId = (int)($l['ID'] ?? 0);
                    if ($cloudLeadId > 0 && isset($this->leadMapCache[$cloudLeadId])) {
                        $boxLeadId = (int)$this->leadMapCache[$cloudLeadId];
                        try {
                            BoxD7Service::updateLead($boxLeadId, ['COMPANY_ID' => (int)$boxCompanyId]);
                            $fixedLeads++;
                        } catch (\Throwable $e) {}
                    }
                }
            } catch (\Throwable $e) {}

            if ($processed % 20 === 0) {
                $this->addLog("  Обработано $processed/" . count($companyMappings) . " (сделок=$fixedDeals, лидов=$fixedLeads)");
            }
        }

        $this->addLog("Привязки компаний: сделок=$fixedDeals, лидов=$fixedLeads");
        return $processed;
    }

    // ------------------------------------------------------------------
    // Rebuild mappings by TITLE / NAME matching
    // ------------------------------------------------------------------

    private function rebuildMappings(string $entityType): void
    {
        $this->addLog("--- Восстановление маппингов: $entityType ---");
        $conn = \Bitrix\Main\Application::getConnection();

        // Build box index by title/name
        $boxIndex = [];
        switch ($entityType) {
            case 'company':
                $rows = $conn->query("SELECT ID, TITLE FROM b_crm_company")->fetchAll();
                foreach ($rows as $r) {
                    $key = mb_strtolower(trim($r['TITLE'] ?? ''));
                    if ($key !== '') $boxIndex[$key] = (int)$r['ID'];
                }
                $apiMethod = 'crm.company.list';
                $selectFields = ['ID', 'TITLE'];
                break;
            case 'contact':
                $rows = $conn->query("SELECT ID, NAME, LAST_NAME FROM b_crm_contact")->fetchAll();
                foreach ($rows as $r) {
                    $key = mb_strtolower(trim(($r['LAST_NAME'] ?? '') . ' ' . ($r['NAME'] ?? '')));
                    if ($key !== '' && $key !== ' ') $boxIndex[$key] = (int)$r['ID'];
                }
                $apiMethod = 'crm.contact.list';
                $selectFields = ['ID', 'NAME', 'LAST_NAME'];
                break;
            case 'deal':
                $rows = $conn->query("SELECT ID, TITLE FROM b_crm_deal")->fetchAll();
                foreach ($rows as $r) {
                    $key = mb_strtolower(trim($r['TITLE'] ?? ''));
                    if ($key !== '') $boxIndex[$key] = (int)$r['ID'];
                }
                $apiMethod = 'crm.deal.list';
                $selectFields = ['ID', 'TITLE'];
                break;
            case 'lead':
                $rows = $conn->query("SELECT ID, TITLE FROM b_crm_lead")->fetchAll();
                // Leads can have duplicate titles — store arrays, match only unique
                $leadIndex = [];
                foreach ($rows as $r) {
                    $key = mb_strtolower(trim($r['TITLE'] ?? ''));
                    if ($key !== '') $leadIndex[$key][] = (int)$r['ID'];
                }
                foreach ($leadIndex as $key => $ids) {
                    if (count($ids) === 1) $boxIndex[$key] = $ids[0];
                }
                $apiMethod = 'crm.lead.list';
                $selectFields = ['ID', 'TITLE'];
                break;
            default:
                $this->addLog("  Неизвестный тип: $entityType");
                return;
        }

        $this->addLog("  Box $entityType: " . count($boxIndex) . " записей");

        $matched = 0;
        $skipped = 0;
        $notFound = 0;
        $self = $this;

        $this->cloudAPI->fetchBatched($apiMethod, ['select' => $selectFields], function ($batch) use ($self, $entityType, $boxIndex, &$matched, &$skipped, &$notFound) {
            foreach ($batch as $item) {
                $cloudId = (int)($item['ID'] ?? 0);
                if ($cloudId <= 0) continue;

                // Build matching key
                if ($entityType === 'contact') {
                    $key = mb_strtolower(trim(($item['LAST_NAME'] ?? '') . ' ' . ($item['NAME'] ?? '')));
                } else {
                    $key = mb_strtolower(trim($item['TITLE'] ?? ''));
                }
                if ($key === '' || $key === ' ') continue;

                if (!isset($boxIndex[$key])) {
                    $notFound++;
                    continue;
                }

                if (MapService::exists($entityType, $cloudId)) {
                    $skipped++;
                    continue;
                }

                MapService::addMap($entityType, $cloudId, $boxIndex[$key]);
                $matched++;
            }
            $self->addLog("  batch: matched=$matched, existed=$skipped, not_found=$notFound");
        });

        $total = count(MapService::getAllMappings($entityType));
        $this->addLog("  Итого: новых=$matched, было=$skipped, не найдено=$notFound. Всего маппингов $entityType: $total");
    }

    // ------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------

    private function restoreCaches(): void
    {
        // UF schema
        $tmp = json_decode(Option::get($this->moduleId, 'uf_field_schema', ''), true);
        if (is_array($tmp)) $this->ufFieldSchema = $tmp;

        $tmp = json_decode(Option::get($this->moduleId, 'uf_enum_map', ''), true);
        if (is_array($tmp)) $this->ufEnumMap = $tmp;

        // Pipeline/stage maps
        $tmp = json_decode(Option::get($this->moduleId, 'pipeline_map_cache', ''), true);
        if (is_array($tmp)) $this->pipelineMapCache = $tmp;

        $tmp = json_decode(Option::get($this->moduleId, 'stage_map_cache', ''), true);
        if (is_array($tmp)) $this->stageMapCache = $tmp;

        // Entity caches from HL block
        foreach (['company', 'contact', 'deal', 'lead'] as $type) {
            $cache = $type . 'MapCache';
            $mappings = MapService::getAllMappings($type);
            foreach ($mappings as $cloudId => $localId) {
                $this->{$cache}[$cloudId] = $localId;
            }
        }

        // User map
        $mappings = MapService::getAllMappings('user');
        foreach ($mappings as $cloudId => $localId) {
            $this->userMapCache[$cloudId] = $localId;
        }

        $this->addLog("Кэши восстановлены: company=" . count($this->companyMapCache)
            . ", contact=" . count($this->contactMapCache)
            . ", deal=" . count($this->dealMapCache)
            . ", lead=" . count($this->leadMapCache)
            . ", user=" . count($this->userMapCache)
            . ", uf_schema=" . (empty($this->ufFieldSchema) ? '0' : array_sum(array_map('count', $this->ufFieldSchema))));
    }

    private function copyUfFields(string $entityType, array $entity, array &$fields): void
    {
        foreach ($entity as $k => $v) {
            if (strncmp($k, 'UF_CRM_', 7) !== 0) continue;
            if ($v === '' || $v === null) continue;
            $transformed = $this->transformUfValue($entityType, $k, $v);
            if ($transformed !== null) {
                $fields[$k] = $transformed;
            }
        }
    }

    private function transformUfValue(string $entityType, string $fieldName, $cloudValue)
    {
        if ($cloudValue === '' || $cloudValue === null) return null;

        $schema = $this->ufFieldSchema[$entityType][$fieldName] ?? null;
        if (!$schema) return $cloudValue;

        $type = $schema['type'];
        $isMultiple = $schema['multiple'];

        if ($isMultiple && is_array($cloudValue)) {
            $result = [];
            foreach ($cloudValue as $v) {
                $tmp = $this->transformSingleUfValue($entityType, $fieldName, $type, $v);
                if ($tmp !== null) $result[] = $tmp;
            }
            return !empty($result) ? $result : null;
        }

        return $this->transformSingleUfValue($entityType, $fieldName, $type, $cloudValue);
    }

    private function transformSingleUfValue(string $entityType, string $fieldName, string $type, $value)
    {
        switch ($type) {
            case 'enumeration':
                $cloudId = (int)$value;
                return $this->ufEnumMap[$entityType][$fieldName][$cloudId] ?? null;
            case 'date':
                $ts = strtotime((string)$value);
                return $ts ? date('d.m.Y', $ts) : null;
            case 'datetime':
                $ts = strtotime((string)$value);
                return $ts ? date('d.m.Y H:i:s', $ts) : null;
            default:
                return $value;
        }
    }

    private function packMultifields(array $entity): array
    {
        $fm = [];
        foreach (['PHONE', 'EMAIL', 'WEB', 'IM'] as $type) {
            if (empty($entity[$type]) || !is_array($entity[$type])) continue;
            $i = 0;
            foreach ($entity[$type] as $row) {
                if (!is_array($row)) continue;
                $value = trim((string)($row['VALUE'] ?? ''));
                if ($value === '') continue;
                $valueType = (string)($row['VALUE_TYPE'] ?? '');
                if ($valueType === '') {
                    $valueType = ($type === 'EMAIL' || $type === 'PHONE') ? 'WORK' : 'OTHER';
                }
                $fm[$type]['n' . (++$i)] = [
                    'VALUE'      => $value,
                    'VALUE_TYPE' => $valueType,
                ];
            }
        }
        return $fm;
    }

    private function buildRequisitePresetMap(): array
    {
        $map = [];
        try {
            $cloudPresets = $this->cloudAPI->request('crm.requisite.preset.list', [])['result'] ?? [];
        } catch (\Throwable $e) {
            $this->addLog("  Не удалось получить пресеты из облака: " . $e->getMessage());
            return $map;
        }
        if (empty($cloudPresets)) return $map;

        $conn = \Bitrix\Main\Application::getConnection();
        $boxPresets = $conn->query("SELECT ID, NAME, COUNTRY_ID, ENTITY_TYPE_ID, SETTINGS FROM b_crm_preset")->fetchAll();

        $byCountryName = [];
        $byName = [];
        foreach ($boxPresets as $p) {
            $name = mb_strtolower(trim($p['NAME'] ?? ''));
            $country = (int)($p['COUNTRY_ID'] ?? 0);
            $id = (int)$p['ID'];
            $settings = @unserialize($p['SETTINGS'] ?? '');
            $hasFields = is_array($settings) && !empty($settings['FIELDS']);
            if (!$hasFields) continue;

            $key = "$country|$name";
            $byCountryName[$key] = $id;
            if (!isset($byName[$name])) $byName[$name] = $id;
        }

        foreach ($cloudPresets as $cp) {
            $cloudId = (int)($cp['ID'] ?? 0);
            if ($cloudId <= 0) continue;
            $cloudName = mb_strtolower(trim($cp['NAME'] ?? ''));
            $cloudCountry = (int)($cp['COUNTRY_ID'] ?? 0);

            $key = "$cloudCountry|$cloudName";
            if (isset($byCountryName[$key])) {
                $map[$cloudId] = $byCountryName[$key];
                continue;
            }
            if (isset($byName[$cloudName])) {
                $map[$cloudId] = $byName[$cloudName];
                continue;
            }
        }

        return $map;
    }

    private function rateLimit(): void
    {
        usleep(330000); // ~3 requests/sec for REST API
    }

    private function saveProgress(int $current, int $total): void
    {
        Option::set($this->moduleId, 'repair_progress', json_encode([
            'current' => $current,
            'total'   => $total,
        ]));
    }

    private function checkStop(): void
    {
        if (Option::get($this->moduleId, 'repair_stop', '0') === '1') {
            $this->addLog('Дозаполнение остановлено пользователем');
            Option::set($this->moduleId, 'repair_status', 'stopped');
            exit(0);
        }
    }
}
