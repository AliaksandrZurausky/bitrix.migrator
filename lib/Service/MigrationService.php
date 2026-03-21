<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Integration\CloudAPI;
use Bitrix\Main\Config\Option;

class MigrationService
{
    private $cloudAPI;
    private $boxAPI;
    private $plan;
    private $moduleId = 'bitrix_migrator';
    private $logFile = null;

    // Caches
    private $userMapCache = [];      // cloud user ID => box user ID
    private $deptMapCache = [];      // cloud dept ID => box dept ID
    private $groupMapCache = [];     // cloud group ID => box group ID
    private $companyMapCache = [];   // cloud company ID => box company ID
    private $contactMapCache = [];   // cloud contact ID => box contact ID
    private $dealMapCache = [];      // cloud deal ID => box deal ID
    private $leadMapCache = [];      // cloud lead ID => box lead ID
    private $pipelineMapCache = [];  // cloud category ID => box category ID
    private $stageMapCache = [];     // cloud status ID => box status ID

    private $log = [];
    private $opCount = 0;

    // Migration phases in execution order
    const PHASES = [
        'departments',
        'users',
        'crm_fields',
        'pipelines',
        'companies',
        'contacts',
        'deals',
        'leads',
        'timeline',
        'workgroups',
        'smart_processes',
        'tasks',
    ];

    const PHASE_LABELS = [
        'departments'     => 'Подразделения',
        'users'           => 'Пользователи',
        'crm_fields'      => 'CRM пользовательские поля',
        'pipelines'       => 'Воронки сделок',
        'companies'       => 'Компании',
        'contacts'        => 'Контакты',
        'deals'           => 'Сделки',
        'leads'           => 'Лиды',
        'timeline'        => 'Таймлайн и активности',
        'workgroups'      => 'Рабочие группы',
        'smart_processes' => 'Смарт-процессы',
        'tasks'           => 'Задачи',
    ];

    public function __construct(CloudAPI $cloudAPI, CloudAPI $boxAPI, array $plan)
    {
        $this->cloudAPI = $cloudAPI;
        $this->boxAPI = $boxAPI;
        $this->plan = $plan;
    }

    public function setLogFile($path)
    {
        $this->logFile = $path;
    }

    public function migrate()
    {
        $this->setStatus('running', 'Миграция запущена');
        $this->addLog('=== Начало полной миграции ===');

        try {
            foreach (self::PHASES as $phase) {
                $this->checkStop();

                if (!$this->isPhaseEnabled($phase)) {
                    $this->addLog("[$phase] Пропущено (отключено в плане)");
                    $this->savePhaseStatus($phase, 'skipped');
                    continue;
                }

                $this->savePhaseStatus($phase, 'running');
                $this->setStatus('running', self::PHASE_LABELS[$phase] . '...');
                $this->addLog("--- Фаза: " . self::PHASE_LABELS[$phase] . " ---");

                $method = 'migrate' . str_replace('_', '', ucwords($phase, '_'));
                $this->$method();

                $this->savePhaseStatus($phase, 'done');
                $this->addLog("[$phase] Завершено");
            }

            $this->setStatus('completed', 'Миграция завершена успешно');
            $this->addLog('=== Миграция завершена ===');

        } catch (MigrationStoppedException $e) {
            $this->addLog('=== Миграция остановлена пользователем ===');
            $this->setStatus('stopped', 'Миграция остановлена пользователем');
        } catch (\Exception $e) {
            $this->addLog('FATAL ERROR: ' . $e->getMessage());
            $this->setStatus('error', 'Ошибка: ' . $e->getMessage());
        }

        $this->saveResult();
    }

    // =========================================================================
    // Stop check
    // =========================================================================

    private function checkStop()
    {
        $stop = Option::get($this->moduleId, 'migration_stop', '0');
        if ($stop === '1') {
            throw new MigrationStoppedException('Остановлено пользователем');
        }
    }

    // =========================================================================
    // Phase checks
    // =========================================================================

    private function isPhaseEnabled($phase)
    {
        $planKeyMap = [
            'departments'     => 'departments',
            'users'           => 'users',
            'crm_fields'      => 'crm_custom_fields',
            'pipelines'       => 'pipelines',
            'companies'       => 'companies',
            'contacts'        => 'contacts',
            'deals'           => 'deals',
            'leads'           => 'leads',
            'timeline'        => 'timeline',
            'workgroups'      => 'workgroups',
            'smart_processes' => 'smart_processes',
            'tasks'           => 'tasks',
        ];

        $key = $planKeyMap[$phase] ?? $phase;
        if (!isset($this->plan[$key])) return true;
        return ($this->plan[$key]['enabled'] ?? true) !== false;
    }

    private function isItemExcluded($section, $id)
    {
        $excluded = $this->plan[$section]['excluded_ids'] ?? [];
        return in_array((string)$id, $excluded);
    }

    // =========================================================================
    // Phase 1: Departments
    // =========================================================================

    private function migrateDepartments()
    {
        $cloudDepts = $this->cloudAPI->getDepartments();
        $boxDepts = $this->boxAPI->getDepartments();

        // --- Step 1: Delete all non-root departments on box ---
        // Find the root department (PARENT=0 or no PARENT)
        $rootDeptId = null;
        foreach ($boxDepts as $d) {
            if (empty($d['PARENT']) || (int)$d['PARENT'] === 0) {
                $rootDeptId = (int)$d['ID'];
                break;
            }
        }

        // Sort by depth descending (deepest first) so children are deleted before parents
        $nonRootDepts = array_filter($boxDepts, function ($d) use ($rootDeptId) {
            return (int)$d['ID'] !== $rootDeptId;
        });

        // Build depth map for proper deletion order
        $deptById = [];
        foreach ($boxDepts as $d) {
            $deptById[(int)$d['ID']] = $d;
        }
        $depthOf = function ($id) use ($deptById, &$depthOf) {
            if (!isset($deptById[$id]) || empty($deptById[$id]['PARENT']) || (int)$deptById[$id]['PARENT'] === 0) {
                return 0;
            }
            return 1 + $depthOf((int)$deptById[$id]['PARENT']);
        };

        // Sort deepest first
        usort($nonRootDepts, function ($a, $b) use ($depthOf) {
            return $depthOf((int)$b['ID']) - $depthOf((int)$a['ID']);
        });

        $deleted = 0;
        foreach ($nonRootDepts as $d) {
            $this->rateLimit();
            try {
                $this->boxAPI->deleteDepartment((int)$d['ID']);
                $deleted++;
            } catch (\Exception $e) {
                $this->addLog("  Ошибка удаления подразделения '{$d['NAME']}' (ID={$d['ID']}): " . $e->getMessage());
            }
        }
        $this->addLog("Удалено подразделений на box: $deleted");

        // --- Step 2: Find the cloud root department ---
        $cloudRootId = null;
        foreach ($cloudDepts as $d) {
            if (empty($d['PARENT']) || (int)$d['PARENT'] === 0) {
                $cloudRootId = (int)$d['ID'];
                break;
            }
        }

        // Map cloud root → box root
        if ($cloudRootId && $rootDeptId) {
            $this->deptMapCache[$cloudRootId] = $rootDeptId;

            // Rename box root to match cloud root name
            $cloudRootName = null;
            foreach ($cloudDepts as $d) {
                if ((int)$d['ID'] === $cloudRootId) {
                    $cloudRootName = trim($d['NAME']);
                    break;
                }
            }
            if ($cloudRootName) {
                try {
                    $this->boxAPI->updateDepartment($rootDeptId, ['NAME' => $cloudRootName]);
                } catch (\Exception $e) {
                    $this->addLog("  Не удалось переименовать корневой отдел: " . $e->getMessage());
                }
            }
        }

        // --- Step 3: Create cloud structure on box ---
        // Sort by parent (top-level first so parents exist before children)
        usort($cloudDepts, function ($a, $b) {
            return ((int)($a['PARENT'] ?? 0)) - ((int)($b['PARENT'] ?? 0));
        });

        $total = count($cloudDepts);
        $created = 0;

        foreach ($cloudDepts as $i => $dept) {
            $this->rateLimit();
            $this->savePhaseProgress('departments', $i + 1, $total);

            $cloudId = (int)$dept['ID'];

            // Skip root — already mapped
            if ($cloudId === $cloudRootId) {
                continue;
            }

            $name = trim($dept['NAME']);

            $fields = ['NAME' => $name];
            if (!empty($dept['PARENT']) && isset($this->deptMapCache[$dept['PARENT']])) {
                $fields['PARENT'] = $this->deptMapCache[$dept['PARENT']];
            } elseif ($rootDeptId) {
                // If parent not in map, attach to box root
                $fields['PARENT'] = $rootDeptId;
            }

            try {
                $newId = $this->boxAPI->addDepartment($fields);
                if ($newId) {
                    $this->deptMapCache[$cloudId] = $newId;
                    $created++;
                }
            } catch (\Exception $e) {
                $this->addLog("  Ошибка создания подразделения '{$name}': " . $e->getMessage());
            }
        }

        $this->addLog("Подразделения: удалено=$deleted, создано=$created из $total");
        $this->saveStats(['departments' => ['total' => $total, 'deleted' => $deleted, 'created' => $created]]);
    }

    // =========================================================================
    // Phase 2: Users
    // =========================================================================

    private function migrateUsers()
    {
        $this->buildUserMapCache();

        $cloudUsers = $this->cloudAPI->getUsers();

        // Separate new users and existing users that need department update
        $newUsers = [];
        $existingToUpdate = [];

        foreach ($cloudUsers as $u) {
            if (($u['ACTIVE'] ?? 'Y') !== 'Y') continue;
            $cloudId = (int)$u['ID'];
            if ($this->isItemExcluded('users', $cloudId)) continue;

            if (isset($this->userMapCache[$cloudId])) {
                // User already exists — update departments and position
                $existingToUpdate[] = $u;
            } else {
                $newUsers[] = $u;
            }
        }

        $total = count($newUsers);
        $invited = 0;
        $updated = 0;
        $errors = 0;

        // --- Create new users ---
        foreach ($newUsers as $i => $u) {
            $this->rateLimit();
            $this->savePhaseProgress('users', $i + 1, $total + count($existingToUpdate));

            try {
                $boxDeptIds = $this->mapDepartmentIds($u['UF_DEPARTMENT'] ?? []);

                $fields = [
                    'EMAIL' => $u['EMAIL'],
                    'NAME' => $u['NAME'] ?? '',
                    'LAST_NAME' => $u['LAST_NAME'] ?? '',
                    'SECOND_NAME' => $u['SECOND_NAME'] ?? '',
                    'WORK_POSITION' => $u['WORK_POSITION'] ?? '',
                    'PERSONAL_PHONE' => $u['PERSONAL_PHONE'] ?? '',
                    'PERSONAL_MOBILE' => $u['PERSONAL_MOBILE'] ?? '',
                    'WORK_PHONE' => $u['WORK_PHONE'] ?? '',
                    'ACTIVE' => true,
                    'EXTRANET' => 'N',
                    'MESSAGE_TYPE' => 'N', // Do not send invitation email
                ];

                if (!empty($boxDeptIds)) {
                    $fields['UF_DEPARTMENT'] = $boxDeptIds;
                }

                // Personal info
                if (!empty($u['PERSONAL_GENDER'])) $fields['PERSONAL_GENDER'] = $u['PERSONAL_GENDER'];
                if (!empty($u['PERSONAL_BIRTHDAY'])) $fields['PERSONAL_BIRTHDAY'] = $u['PERSONAL_BIRTHDAY'];
                if (!empty($u['PERSONAL_PHOTO'])) $fields['PERSONAL_PHOTO'] = $u['PERSONAL_PHOTO'];

                $newId = $this->boxAPI->inviteUser($fields);
                if ($newId) {
                    $this->userMapCache[(int)$u['ID']] = $newId;
                    $invited++;
                }
            } catch (\Exception $e) {
                $errors++;
                $this->addLog("  Ошибка создания пользователя {$u['EMAIL']}: " . $e->getMessage());
            }
        }

        // --- Update existing users (departments, position) ---
        foreach ($existingToUpdate as $j => $u) {
            $this->rateLimit();
            $this->savePhaseProgress('users', $total + $j + 1, $total + count($existingToUpdate));

            $cloudId = (int)$u['ID'];
            $boxUserId = $this->userMapCache[$cloudId];

            try {
                $boxDeptIds = $this->mapDepartmentIds($u['UF_DEPARTMENT'] ?? []);
                $updateFields = [];

                if (!empty($boxDeptIds)) {
                    $updateFields['UF_DEPARTMENT'] = $boxDeptIds;
                }
                if (!empty($u['WORK_POSITION'])) {
                    $updateFields['WORK_POSITION'] = $u['WORK_POSITION'];
                }

                if (!empty($updateFields)) {
                    $this->boxAPI->updateUser($boxUserId, $updateFields);
                    $updated++;
                }
            } catch (\Exception $e) {
                $this->addLog("  Ошибка обновления пользователя {$u['EMAIL']}: " . $e->getMessage());
            }
        }

        $matched = count($this->userMapCache) - $invited;
        $this->addLog("Пользователи: совпали=$matched, создано=$invited, обновлено=$updated, ошибок=$errors");
    }

    private function mapDepartmentIds($cloudDeptIds)
    {
        if (!is_array($cloudDeptIds)) $cloudDeptIds = [$cloudDeptIds];
        $boxDeptIds = [];
        foreach ($cloudDeptIds as $did) {
            if (isset($this->deptMapCache[$did])) {
                $boxDeptIds[] = $this->deptMapCache[$did];
            }
        }
        return $boxDeptIds;
    }

    private function buildUserMapCache()
    {
        if (!empty($this->userMapCache)) return;

        $cloudUsers = $this->cloudAPI->getUsers();
        $boxUsers = $this->boxAPI->getUsers();

        $boxByEmail = [];
        foreach ($boxUsers as $u) {
            if (!empty($u['EMAIL'])) {
                $boxByEmail[mb_strtolower(trim($u['EMAIL']))] = (int)$u['ID'];
            }
        }

        foreach ($cloudUsers as $u) {
            if (($u['ACTIVE'] ?? 'Y') !== 'Y') continue;
            $email = mb_strtolower(trim($u['EMAIL'] ?? ''));
            if ($email && isset($boxByEmail[$email])) {
                $this->userMapCache[(int)$u['ID']] = $boxByEmail[$email];
            }
        }

        $this->addLog("Кэш пользователей: " . count($this->userMapCache) . " совпадений по email");
    }

    private function mapUser($cloudUserId)
    {
        return $this->userMapCache[(int)$cloudUserId] ?? null;
    }

    // =========================================================================
    // Phase 3: CRM Custom Fields
    // =========================================================================

    private function migrateCrmFields()
    {
        $entityTypes = ['deal', 'contact', 'company', 'lead'];
        $totalCreated = 0;
        $totalDeleted = 0;

        // Setting: which entity types should have fields deleted before migration
        // Default: delete all. User can exclude specific entity types.
        $deleteFieldsSettings = $this->plan['delete_userfields'] ?? [];
        // Default is enabled for all types
        $deleteFieldsEnabled = ($deleteFieldsSettings['enabled'] ?? true) === true;

        foreach ($entityTypes as $entityType) {
            $cloudFields = $this->cloudAPI->getUserfields($entityType);
            $boxFields = $this->boxAPI->getUserfields($entityType);

            // --- Step 1: Delete existing userfields on box if enabled for this entity type ---
            $shouldDelete = $deleteFieldsEnabled
                && !in_array($entityType, $deleteFieldsSettings['skip_entities'] ?? []);

            if ($shouldDelete && !empty($boxFields)) {
                $this->addLog("Удаление пользовательских полей ($entityType): " . count($boxFields) . " шт.");
                foreach ($boxFields as $f) {
                    $this->rateLimit();
                    $fId = (int)($f['ID'] ?? 0);
                    $fName = $f['FIELD_NAME'] ?? '';
                    if (!$fId) continue;
                    try {
                        $this->boxAPI->deleteUserfield($entityType, $fId);
                        $totalDeleted++;
                    } catch (\Exception $e) {
                        $this->addLog("  Ошибка удаления поля $fName ($entityType): " . $e->getMessage());
                    }
                }
                // After deletion, no existing fields to skip
                $boxFieldNames = [];
            } else {
                $boxFieldNames = [];
                foreach ($boxFields as $f) {
                    $boxFieldNames[] = $f['FIELD_NAME'] ?? '';
                }
            }

            // --- Step 2: Create fields from cloud ---
            foreach ($cloudFields as $field) {
                $this->rateLimit();
                $fieldName = $field['FIELD_NAME'] ?? '';

                if (isset($this->plan['crm_custom_fields'][$entityType]['excluded_fields'])) {
                    if (in_array($fieldName, $this->plan['crm_custom_fields'][$entityType]['excluded_fields'])) {
                        continue;
                    }
                }

                if (in_array($fieldName, $boxFieldNames)) continue;

                try {
                    $newField = [
                        'FIELD_NAME' => $fieldName,
                        'USER_TYPE_ID' => $field['USER_TYPE_ID'] ?? 'string',
                        'XML_ID' => $field['XML_ID'] ?? $fieldName,
                        'EDIT_FORM_LABEL' => $field['EDIT_FORM_LABEL'] ?? [],
                        'LIST_COLUMN_LABEL' => $field['LIST_COLUMN_LABEL'] ?? [],
                        'SETTINGS' => $field['SETTINGS'] ?? [],
                    ];

                    if (!empty($field['MANDATORY'])) $newField['MANDATORY'] = $field['MANDATORY'];
                    if (!empty($field['MULTIPLE'])) $newField['MULTIPLE'] = $field['MULTIPLE'];
                    if (!empty($field['LIST'])) $newField['LIST'] = $field['LIST'];

                    $this->boxAPI->addUserfield($entityType, $newField);
                    $totalCreated++;
                } catch (\Exception $e) {
                    $this->addLog("  Ошибка создания поля $fieldName ($entityType): " . $e->getMessage());
                }
            }
        }

        $this->addLog("CRM поля: удалено=$totalDeleted, создано=$totalCreated");
    }

    // =========================================================================
    // Phase 4: Pipelines (Deal Categories + Stages)
    // =========================================================================

    private function migratePipelines()
    {
        $cloudCategories = $this->cloudAPI->getDealCategories();
        $boxCategories = $this->boxAPI->getDealCategories();

        $boxCatByName = [];
        foreach ($boxCategories as $c) {
            $boxCatByName[mb_strtolower(trim($c['NAME'] ?? ''))] = $c;
        }

        $this->pipelineMapCache[0] = 0;

        $cloudStagesGrouped = $this->cloudAPI->getAllDealStagesGrouped();
        $boxStatuses = $this->boxAPI->getStatuses();
        $boxStatusByEntityAndId = [];
        foreach ($boxStatuses as $s) {
            $boxStatusByEntityAndId[$s['ENTITY_ID'] . ':' . $s['STATUS_ID']] = $s;
        }

        $this->mapPipelineStages('DEAL_STAGE', $cloudStagesGrouped['DEAL_STAGE'] ?? [], $boxStatusByEntityAndId);

        $created = 0;
        foreach ($cloudCategories as $cat) {
            $this->rateLimit();
            $catId = (int)$cat['ID'];
            if ($catId === 0) continue;

            if ($this->isItemExcluded('pipelines', $catId)) continue;

            $name = trim($cat['NAME'] ?? '');
            $lowerName = mb_strtolower($name);

            if (isset($boxCatByName[$lowerName])) {
                $boxCat = $boxCatByName[$lowerName];
                $this->pipelineMapCache[$catId] = (int)$boxCat['ID'];
            } else {
                try {
                    $newCatId = $this->boxAPI->addDealCategory([
                        'NAME' => $name,
                        'SORT' => $cat['SORT'] ?? 500,
                    ]);
                    if ($newCatId) {
                        $this->pipelineMapCache[$catId] = $newCatId;
                        $created++;
                    }
                } catch (\Exception $e) {
                    $this->addLog("  Ошибка создания воронки '{$name}': " . $e->getMessage());
                    continue;
                }
            }

            $entityId = 'DEAL_STAGE_' . $catId;
            $stages = $cloudStagesGrouped[$entityId] ?? [];
            if (!empty($stages) && isset($this->pipelineMapCache[$catId])) {
                $boxStatuses = $this->boxAPI->getStatuses();
                $boxStatusByEntityAndId = [];
                foreach ($boxStatuses as $s) {
                    $boxStatusByEntityAndId[$s['ENTITY_ID'] . ':' . $s['STATUS_ID']] = $s;
                }
                $this->mapPipelineStages($entityId, $stages, $boxStatusByEntityAndId);
            }
        }

        $this->addLog("Воронки: создано $created, всего маппингов=" . count($this->pipelineMapCache));
    }

    private function mapPipelineStages($cloudEntityId, $stages, $boxStatusMap)
    {
        foreach ($stages as $stage) {
            $statusId = $stage['STATUS_ID'] ?? '';
            $boxKey = $cloudEntityId . ':' . $statusId;
            if (isset($boxStatusMap[$boxKey])) {
                $this->stageMapCache[$statusId] = $statusId;
            } else {
                try {
                    $this->rateLimit();
                    $boxEntityId = $cloudEntityId;
                    if (strncmp($cloudEntityId, 'DEAL_STAGE_', 11) === 0) {
                        $cloudCatId = (int)substr($cloudEntityId, 11);
                        if (isset($this->pipelineMapCache[$cloudCatId])) {
                            $boxEntityId = 'DEAL_STAGE_' . $this->pipelineMapCache[$cloudCatId];
                        }
                    }

                    $this->boxAPI->addStatus([
                        'ENTITY_ID' => $boxEntityId,
                        'STATUS_ID' => $statusId,
                        'NAME' => $stage['NAME'] ?? $statusId,
                        'SORT' => $stage['SORT'] ?? 10,
                        'COLOR' => $stage['COLOR'] ?? '',
                    ]);
                    $this->stageMapCache[$statusId] = $statusId;
                } catch (\Exception $e) {
                    $this->stageMapCache[$statusId] = $statusId;
                }
            }
        }
    }

    // =========================================================================
    // Phase 5: Companies
    // =========================================================================

    private function migrateCompanies()
    {
        $cloudCompanies = $this->cloudAPI->getCompanies(['*', 'UF_*', 'PHONE', 'EMAIL']);
        $total = count($cloudCompanies);
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;

        $dupSettings = $this->plan['duplicate_settings']['companies'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['TITLE'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        foreach ($cloudCompanies as $i => $company) {
            $this->rateLimit();
            $this->savePhaseProgress('companies', $i + 1, $total);
            $cloudId = (int)$company['ID'];

            $existing = $this->findDuplicate('company', $company, $matchBy);
            if ($existing) {
                $this->companyMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $this->buildCompanyFields($company);
                        $this->boxAPI->updateCompany((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Exception $e) {
                        $this->addLog("  Ошибка обновления компании #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $this->buildCompanyFields($company);
                $newId = $this->boxAPI->addCompany($fields);
                if ($newId) {
                    $this->companyMapCache[$cloudId] = $newId;
                    $created++;
                }
            } catch (\Exception $e) {
                $errors++;
                $this->addLog("  Ошибка создания компании #{$cloudId}: " . $e->getMessage());
            }
        }

        $this->addLog("Компании: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['companies' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildCompanyFields($company)
    {
        $fields = [];
        $copy = ['TITLE', 'COMPANY_TYPE', 'INDUSTRY', 'REVENUE', 'CURRENCY_ID',
                 'COMMENTS', 'OPENED', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                 'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY'];
        foreach ($copy as $k) {
            if (isset($company[$k]) && $company[$k] !== '') $fields[$k] = $company[$k];
        }

        if (!empty($company['ASSIGNED_BY_ID'])) {
            $boxUser = $this->mapUser($company['ASSIGNED_BY_ID']);
            if ($boxUser) $fields['ASSIGNED_BY_ID'] = $boxUser;
        }

        if (!empty($company['PHONE'])) $fields['PHONE'] = $company['PHONE'];
        if (!empty($company['EMAIL'])) $fields['EMAIL'] = $company['EMAIL'];
        if (!empty($company['WEB'])) $fields['WEB'] = $company['WEB'];
        if (!empty($company['IM'])) $fields['IM'] = $company['IM'];

        foreach ($company as $k => $v) {
            if (strncmp($k, 'UF_CRM_', 7) === 0 && $v !== '' && $v !== null) {
                $fields[$k] = $v;
            }
        }

        return $fields;
    }

    // =========================================================================
    // Phase 6: Contacts
    // =========================================================================

    private function migrateContacts()
    {
        $cloudContacts = $this->cloudAPI->getContacts(['*', 'UF_*', 'PHONE', 'EMAIL']);
        $total = count($cloudContacts);
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;

        $dupSettings = $this->plan['duplicate_settings']['contacts'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['EMAIL'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        foreach ($cloudContacts as $i => $contact) {
            $this->rateLimit();
            $this->savePhaseProgress('contacts', $i + 1, $total);
            $cloudId = (int)$contact['ID'];

            $existing = $this->findDuplicate('contact', $contact, $matchBy);
            if ($existing) {
                $this->contactMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $this->buildContactFields($contact);
                        $this->boxAPI->updateContact((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Exception $e) {
                        $this->addLog("  Ошибка обновления контакта #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $this->buildContactFields($contact);
                $newId = $this->boxAPI->addContact($fields);
                if ($newId) {
                    $this->contactMapCache[$cloudId] = $newId;
                    $created++;

                    $this->linkContactCompanies($cloudId, $newId, $contact);
                }
            } catch (\Exception $e) {
                $errors++;
                $this->addLog("  Ошибка создания контакта #{$cloudId}: " . $e->getMessage());
            }
        }

        $this->addLog("Контакты: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['contacts' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildContactFields($contact)
    {
        $fields = [];
        $copy = ['NAME', 'LAST_NAME', 'SECOND_NAME', 'POST', 'TYPE_ID', 'SOURCE_ID',
                 'COMMENTS', 'OPENED', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                 'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                 'BIRTHDATE', 'HONORIFIC'];
        foreach ($copy as $k) {
            if (isset($contact[$k]) && $contact[$k] !== '') $fields[$k] = $contact[$k];
        }

        if (!empty($contact['ASSIGNED_BY_ID'])) {
            $boxUser = $this->mapUser($contact['ASSIGNED_BY_ID']);
            if ($boxUser) $fields['ASSIGNED_BY_ID'] = $boxUser;
        }

        if (!empty($contact['COMPANY_ID']) && isset($this->companyMapCache[(int)$contact['COMPANY_ID']])) {
            $fields['COMPANY_ID'] = $this->companyMapCache[(int)$contact['COMPANY_ID']];
        }

        if (!empty($contact['PHONE'])) $fields['PHONE'] = $contact['PHONE'];
        if (!empty($contact['EMAIL'])) $fields['EMAIL'] = $contact['EMAIL'];
        if (!empty($contact['WEB'])) $fields['WEB'] = $contact['WEB'];
        if (!empty($contact['IM'])) $fields['IM'] = $contact['IM'];

        foreach ($contact as $k => $v) {
            if (strncmp($k, 'UF_CRM_', 7) === 0 && $v !== '' && $v !== null) {
                $fields[$k] = $v;
            }
        }

        return $fields;
    }

    private function linkContactCompanies($cloudContactId, $boxContactId, $contact)
    {
        try {
            $cloudCompanies = $this->cloudAPI->getContactCompanyItems($cloudContactId);
            if (empty($cloudCompanies)) return;

            $boxItems = [];
            foreach ($cloudCompanies as $item) {
                $cid = (int)($item['COMPANY_ID'] ?? 0);
                if ($cid && isset($this->companyMapCache[$cid])) {
                    $boxItems[] = ['COMPANY_ID' => $this->companyMapCache[$cid]];
                }
            }

            if (!empty($boxItems)) {
                $this->boxAPI->setContactCompanyItems($boxContactId, $boxItems);
            }
        } catch (\Exception $e) {
            // Non-critical
        }
    }

    // =========================================================================
    // Phase 7: Deals
    // =========================================================================

    private function migrateDeals()
    {
        $cloudDeals = $this->cloudAPI->getDeals(['*', 'UF_*']);
        $total = count($cloudDeals);
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;

        $dupSettings = $this->plan['duplicate_settings']['deals'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['TITLE'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        foreach ($cloudDeals as $i => $deal) {
            $this->rateLimit();
            $this->savePhaseProgress('deals', $i + 1, $total);
            $cloudId = (int)$deal['ID'];

            $existing = $this->findDuplicate('deal', $deal, $matchBy);
            if ($existing) {
                $this->dealMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $this->buildDealFields($deal);
                        $this->boxAPI->updateDeal((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Exception $e) {
                        $this->addLog("  Ошибка обновления сделки #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $this->buildDealFields($deal);
                $newId = $this->boxAPI->addDeal($fields);
                if ($newId) {
                    $this->dealMapCache[$cloudId] = $newId;
                    $created++;

                    $this->linkDealContacts($cloudId, $newId);
                }
            } catch (\Exception $e) {
                $errors++;
                $this->addLog("  Ошибка создания сделки #{$cloudId}: " . $e->getMessage());
            }
        }

        $this->addLog("Сделки: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['deals' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildDealFields($deal)
    {
        $fields = [];
        $copy = ['TITLE', 'TYPE_ID', 'PROBABILITY', 'CURRENCY_ID', 'OPPORTUNITY',
                 'TAX_VALUE', 'COMMENTS', 'OPENED', 'CLOSED', 'BEGINDATE', 'CLOSEDATE',
                 'SOURCE_ID', 'SOURCE_DESCRIPTION', 'ADDITIONAL_INFO'];
        foreach ($copy as $k) {
            if (isset($deal[$k]) && $deal[$k] !== '') $fields[$k] = $deal[$k];
        }

        $catId = (int)($deal['CATEGORY_ID'] ?? 0);
        if (isset($this->pipelineMapCache[$catId])) {
            $fields['CATEGORY_ID'] = $this->pipelineMapCache[$catId];
        }

        if (!empty($deal['STAGE_ID'])) {
            $fields['STAGE_ID'] = $deal['STAGE_ID'];
        }

        if (!empty($deal['ASSIGNED_BY_ID'])) {
            $boxUser = $this->mapUser($deal['ASSIGNED_BY_ID']);
            if ($boxUser) $fields['ASSIGNED_BY_ID'] = $boxUser;
        }

        if (!empty($deal['COMPANY_ID']) && isset($this->companyMapCache[(int)$deal['COMPANY_ID']])) {
            $fields['COMPANY_ID'] = $this->companyMapCache[(int)$deal['COMPANY_ID']];
        }

        if (!empty($deal['CONTACT_ID']) && isset($this->contactMapCache[(int)$deal['CONTACT_ID']])) {
            $fields['CONTACT_ID'] = $this->contactMapCache[(int)$deal['CONTACT_ID']];
        }

        foreach ($deal as $k => $v) {
            if (strncmp($k, 'UF_CRM_', 7) === 0 && $v !== '' && $v !== null) {
                $fields[$k] = $v;
            }
        }

        return $fields;
    }

    private function linkDealContacts($cloudDealId, $boxDealId)
    {
        try {
            $cloudContacts = $this->cloudAPI->getDealContactItems($cloudDealId);
            if (empty($cloudContacts)) return;

            $boxItems = [];
            foreach ($cloudContacts as $item) {
                $cid = (int)($item['CONTACT_ID'] ?? 0);
                if ($cid && isset($this->contactMapCache[$cid])) {
                    $boxItems[] = ['CONTACT_ID' => $this->contactMapCache[$cid]];
                }
            }

            if (!empty($boxItems)) {
                $this->boxAPI->setDealContactItems($boxDealId, $boxItems);
            }
        } catch (\Exception $e) {
            // Non-critical
        }
    }

    // =========================================================================
    // Phase 8: Leads
    // =========================================================================

    private function migrateLeads()
    {
        $cloudLeads = $this->cloudAPI->getLeads(['*', 'UF_*', 'PHONE', 'EMAIL']);
        $total = count($cloudLeads);
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;

        $dupSettings = $this->plan['duplicate_settings']['leads'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['TITLE'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        foreach ($cloudLeads as $i => $lead) {
            $this->rateLimit();
            $this->savePhaseProgress('leads', $i + 1, $total);
            $cloudId = (int)$lead['ID'];

            $existing = $this->findDuplicate('lead', $lead, $matchBy);
            if ($existing) {
                $this->leadMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $this->buildLeadFields($lead);
                        $this->boxAPI->updateLead((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Exception $e) {
                        $this->addLog("  Ошибка обновления лида #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $this->buildLeadFields($lead);
                $newId = $this->boxAPI->addLead($fields);
                if ($newId) {
                    $this->leadMapCache[$cloudId] = $newId;
                    $created++;
                }
            } catch (\Exception $e) {
                $errors++;
                $this->addLog("  Ошибка создания лида #{$cloudId}: " . $e->getMessage());
            }
        }

        $this->addLog("Лиды: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['leads' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildLeadFields($lead)
    {
        $fields = [];
        $copy = ['TITLE', 'NAME', 'LAST_NAME', 'SECOND_NAME', 'STATUS_ID', 'SOURCE_ID',
                 'SOURCE_DESCRIPTION', 'CURRENCY_ID', 'OPPORTUNITY', 'COMMENTS', 'OPENED',
                 'COMPANY_TITLE', 'POST', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                 'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                 'BIRTHDATE', 'HONORIFIC'];
        foreach ($copy as $k) {
            if (isset($lead[$k]) && $lead[$k] !== '') $fields[$k] = $lead[$k];
        }

        if (!empty($lead['ASSIGNED_BY_ID'])) {
            $boxUser = $this->mapUser($lead['ASSIGNED_BY_ID']);
            if ($boxUser) $fields['ASSIGNED_BY_ID'] = $boxUser;
        }

        if (!empty($lead['COMPANY_ID']) && isset($this->companyMapCache[(int)$lead['COMPANY_ID']])) {
            $fields['COMPANY_ID'] = $this->companyMapCache[(int)$lead['COMPANY_ID']];
        }
        if (!empty($lead['CONTACT_ID']) && isset($this->contactMapCache[(int)$lead['CONTACT_ID']])) {
            $fields['CONTACT_ID'] = $this->contactMapCache[(int)$lead['CONTACT_ID']];
        }

        if (!empty($lead['PHONE'])) $fields['PHONE'] = $lead['PHONE'];
        if (!empty($lead['EMAIL'])) $fields['EMAIL'] = $lead['EMAIL'];

        foreach ($lead as $k => $v) {
            if (strncmp($k, 'UF_CRM_', 7) === 0 && $v !== '' && $v !== null) {
                $fields[$k] = $v;
            }
        }

        return $fields;
    }

    // =========================================================================
    // Phase 9: Timeline (Activities & Comments)
    // =========================================================================

    private function migrateTimeline()
    {
        // CRM entity type IDs: Lead=1, Deal=2, Contact=3, Company=4
        $entityTypes = [
            'company' => ['typeId' => 4, 'cache' => &$this->companyMapCache],
            'contact' => ['typeId' => 3, 'cache' => &$this->contactMapCache],
            'deal'    => ['typeId' => 2, 'cache' => &$this->dealMapCache],
            'lead'    => ['typeId' => 1, 'cache' => &$this->leadMapCache],
        ];

        $totalActivities = 0;
        $totalComments = 0;
        $errors = 0;

        // Count total entities for progress
        $totalEntities = 0;
        foreach ($entityTypes as $config) {
            $totalEntities += count($config['cache']);
        }

        $processedEntities = 0;

        foreach ($entityTypes as $entityName => $config) {
            $typeId = $config['typeId'];
            $cache = $config['cache'];

            $this->addLog("Таймлайн: обработка {$entityName} (" . count($cache) . " шт)...");

            foreach ($cache as $cloudId => $boxId) {
                $this->rateLimit();
                $processedEntities++;
                $this->savePhaseProgress('timeline', $processedEntities, $totalEntities);

                // Migrate activities
                try {
                    $activities = $this->cloudAPI->getActivities($typeId, $cloudId);
                    foreach ($activities as $activity) {
                        $this->rateLimit();
                        $fields = $this->buildActivityFields($activity, $typeId, $boxId);

                        try {
                            $this->boxAPI->addActivity($fields);
                            $totalActivities++;
                        } catch (\Exception $e) {
                            $errors++;
                            // Skip "already exists" type errors silently
                        }
                    }
                } catch (\Exception $e) {
                    // Getting activities failed — skip entity
                }

                // Migrate timeline comments
                try {
                    $comments = $this->cloudAPI->getTimelineComments($typeId, $cloudId);
                    foreach ($comments as $comment) {
                        $this->rateLimit();
                        $fields = $this->buildTimelineCommentFields($comment, $typeId, $boxId);

                        try {
                            $this->boxAPI->addTimelineComment($fields);
                            $totalComments++;
                        } catch (\Exception $e) {
                            $errors++;
                        }
                    }
                } catch (\Exception $e) {
                    // Non-critical
                }
            }
        }

        $this->addLog("Таймлайн: активностей=$totalActivities, комментариев=$totalComments, ошибок=$errors");
        $this->saveStats(['timeline' => ['activities' => $totalActivities, 'comments' => $totalComments, 'errors' => $errors]]);
    }

    private function buildActivityFields($activity, $ownerTypeId, $boxOwnerId)
    {
        $fields = [
            'OWNER_TYPE_ID' => $ownerTypeId,
            'OWNER_ID' => $boxOwnerId,
            'TYPE_ID' => $activity['TYPE_ID'] ?? 0,
            'SUBJECT' => $activity['SUBJECT'] ?? '',
            'DESCRIPTION' => $activity['DESCRIPTION'] ?? '',
            'DIRECTION' => $activity['DIRECTION'] ?? 0,
            'COMPLETED' => $activity['COMPLETED'] ?? 'N',
            'PRIORITY' => $activity['PRIORITY'] ?? '2',
        ];

        if (!empty($activity['START_TIME'])) $fields['START_TIME'] = $activity['START_TIME'];
        if (!empty($activity['END_TIME'])) $fields['END_TIME'] = $activity['END_TIME'];
        if (!empty($activity['DEADLINE'])) $fields['DEADLINE'] = $activity['DEADLINE'];

        // Map responsible
        if (!empty($activity['RESPONSIBLE_ID'])) {
            $boxUser = $this->mapUser($activity['RESPONSIBLE_ID']);
            if ($boxUser) $fields['RESPONSIBLE_ID'] = $boxUser;
        }

        // Communications (phone numbers, emails linked to activity)
        if (!empty($activity['COMMUNICATIONS']) && is_array($activity['COMMUNICATIONS'])) {
            $fields['COMMUNICATIONS'] = $activity['COMMUNICATIONS'];
        }

        return $fields;
    }

    private function buildTimelineCommentFields($comment, $entityTypeId, $boxEntityId)
    {
        $fields = [
            'ENTITY_TYPE_ID' => $entityTypeId,
            'ENTITY_ID' => $boxEntityId,
            'COMMENT' => $comment['COMMENT'] ?? '',
        ];

        if (!empty($comment['AUTHOR_ID'])) {
            $boxUser = $this->mapUser($comment['AUTHOR_ID']);
            if ($boxUser) $fields['AUTHOR_ID'] = $boxUser;
        }

        if (!empty($comment['CREATED'])) {
            $fields['CREATED'] = $comment['CREATED'];
        }

        return $fields;
    }

    // =========================================================================
    // Phase 10: Workgroups
    // =========================================================================

    private function migrateWorkgroups()
    {
        $cloudGroups = $this->cloudAPI->getWorkgroups();
        $boxGroups = $this->boxAPI->getWorkgroups();

        $boxByName = [];
        foreach ($boxGroups as $g) {
            $boxByName[mb_strtolower(trim($g['NAME'] ?? ''))] = $g;
        }

        $conflictRes = $this->plan['settings']['conflict_resolution'] ?? 'skip';
        $total = count($cloudGroups);
        $matched = 0;
        $created = 0;
        $errors = 0;

        foreach ($cloudGroups as $i => $group) {
            $this->rateLimit();
            $this->savePhaseProgress('workgroups', $i + 1, $total);
            $cloudId = (int)$group['ID'];
            if ($this->isItemExcluded('workgroups', $cloudId)) continue;

            $name = trim($group['NAME'] ?? '');
            $lowerName = mb_strtolower($name);

            if (isset($boxByName[$lowerName])) {
                if ($conflictRes === 'skip') {
                    $this->groupMapCache[$cloudId] = (int)$boxByName[$lowerName]['ID'];
                    $matched++;
                    continue;
                }
            }

            try {
                $fields = [
                    'NAME' => $name,
                    'DESCRIPTION' => $group['DESCRIPTION'] ?? '',
                    'VISIBLE' => $group['VISIBLE'] ?? 'Y',
                    'OPENED' => $group['OPENED'] ?? 'Y',
                    'PROJECT' => $group['PROJECT'] ?? 'N',
                ];

                if (!empty($group['OWNER_ID'])) {
                    $boxOwner = $this->mapUser($group['OWNER_ID']);
                    if ($boxOwner) $fields['OWNER_ID'] = $boxOwner;
                }

                $newId = $this->boxAPI->createWorkgroup($fields);
                if ($newId) {
                    $this->groupMapCache[$cloudId] = $newId;
                    $created++;
                }
            } catch (\Exception $e) {
                $errors++;
                $this->addLog("  Ошибка создания группы '{$name}': " . $e->getMessage());
            }
        }

        $this->addLog("Рабочие группы: совпали=$matched, создано=$created, ошибок=$errors из $total");
    }

    // =========================================================================
    // Phase 11: Smart Processes
    // =========================================================================

    private function migrateSmartProcesses()
    {
        $cloudTypes = $this->cloudAPI->getSmartProcessTypes();
        $boxTypes = $this->boxAPI->getSmartProcessTypes();

        $boxTypeByTitle = [];
        foreach ($boxTypes as $t) {
            $boxTypeByTitle[mb_strtolower(trim($t['title'] ?? ''))] = $t;
        }

        $totalTypes = 0;
        $totalItems = 0;

        foreach ($cloudTypes as $type) {
            $this->rateLimit();
            $cloudEntityTypeId = (int)($type['entityTypeId'] ?? 0);
            $spId = (string)$cloudEntityTypeId;

            if ($this->isItemExcluded('smart_processes', $spId)) continue;

            $title = trim($type['title'] ?? '');
            $lowerTitle = mb_strtolower($title);
            $boxEntityTypeId = null;

            if (isset($boxTypeByTitle[$lowerTitle])) {
                $boxEntityTypeId = (int)$boxTypeByTitle[$lowerTitle]['entityTypeId'];
            } else {
                try {
                    $newType = $this->boxAPI->addSmartProcessType([
                        'title' => $title,
                        'code' => $type['code'] ?? '',
                    ]);
                    if ($newType && !empty($newType['entityTypeId'])) {
                        $boxEntityTypeId = (int)$newType['entityTypeId'];
                        $totalTypes++;
                    }
                } catch (\Exception $e) {
                    $this->addLog("  Ошибка создания смарт-процесса '{$title}': " . $e->getMessage());
                    continue;
                }
            }

            if (!$boxEntityTypeId) continue;

            try {
                $cloudItems = $this->cloudAPI->getSmartProcessItems($cloudEntityTypeId);
                foreach ($cloudItems as $item) {
                    $this->rateLimit();
                    $fields = $this->buildSmartProcessItemFields($item, $spId);

                    try {
                        $this->boxAPI->addSmartProcessItem($boxEntityTypeId, $fields);
                        $totalItems++;
                    } catch (\Exception $e) {
                        $this->addLog("  Ошибка создания записи SP #{$item['id']}: " . $e->getMessage());
                    }
                }
            } catch (\Exception $e) {
                $this->addLog("  Ошибка получения записей SP '{$title}': " . $e->getMessage());
            }
        }

        $this->addLog("Смарт-процессы: типов создано=$totalTypes, записей=$totalItems");
    }

    private function buildSmartProcessItemFields($item, $spId)
    {
        $fields = [];
        $copy = ['title', 'stageId', 'begindate', 'closeDate', 'opened', 'sourceId'];
        foreach ($copy as $k) {
            if (isset($item[$k]) && $item[$k] !== '' && $item[$k] !== null) {
                $fields[$k] = $item[$k];
            }
        }

        if (!empty($item['assignedById'])) {
            $boxUser = $this->mapUser($item['assignedById']);
            if ($boxUser) $fields['assignedById'] = $boxUser;
        }

        if (!empty($item['companyId']) && isset($this->companyMapCache[(int)$item['companyId']])) {
            $fields['companyId'] = $this->companyMapCache[(int)$item['companyId']];
        }
        if (!empty($item['contactId']) && isset($this->contactMapCache[(int)$item['contactId']])) {
            $fields['contactId'] = $this->contactMapCache[(int)$item['contactId']];
        }

        $excludedFields = $this->plan['smart_process_fields'][$spId]['excluded_fields'] ?? [];
        foreach ($item as $k => $v) {
            if ((strncmp($k, 'uf_', 3) === 0 || strncmp($k, 'UF_', 3) === 0) && $v !== '' && $v !== null) {
                if (!in_array($k, $excludedFields)) {
                    $fields[$k] = $v;
                }
            }
        }

        return $fields;
    }

    // =========================================================================
    // Phase 12: Tasks (delegate to TaskMigrationService)
    // =========================================================================

    private function migrateTasks()
    {
        $taskService = new TaskMigrationService($this->cloudAPI, $this->boxAPI, $this->plan);
        $taskService->setUserMapCache($this->userMapCache);
        $taskService->setGroupMapCache($this->groupMapCache);
        if ($this->logFile) {
            $taskService->setLogFile($this->logFile);
        }
        $taskService->migrate();
    }

    // =========================================================================
    // Duplicate detection
    // =========================================================================

    private function findDuplicate($entityType, $entity, $matchBy)
    {
        foreach ($matchBy as $criterion) {
            $found = null;

            switch ($entityType) {
                case 'company':
                    $found = $this->findCompanyDuplicate($entity, $criterion);
                    break;
                case 'contact':
                    $found = $this->findContactDuplicate($entity, $criterion);
                    break;
                case 'deal':
                    if ($criterion === 'TITLE' && !empty($entity['TITLE'])) {
                        $found = $this->boxAPI->findDealByTitle($entity['TITLE']);
                    }
                    break;
                case 'lead':
                    $found = $this->findLeadDuplicate($entity, $criterion);
                    break;
            }

            if ($found) return $found;
        }

        return null;
    }

    private function findCompanyDuplicate($company, $criterion)
    {
        switch ($criterion) {
            case 'TITLE':
                return !empty($company['TITLE'])
                    ? $this->boxAPI->findCompanyByField('TITLE', $company['TITLE'])
                    : null;
            case 'RQ_INN':
                try {
                    $reqs = $this->cloudAPI->getRequisites(4, (int)$company['ID']);
                    foreach ($reqs as $req) {
                        if (!empty($req['RQ_INN'])) {
                            $boxReq = $this->boxAPI->findRequisiteByInn($req['RQ_INN']);
                            if ($boxReq && (int)$boxReq['ENTITY_TYPE_ID'] === 4) {
                                return ['ID' => $boxReq['ENTITY_ID']];
                            }
                        }
                    }
                } catch (\Exception $e) {}
                return null;
            case 'PHONE':
                $phones = $company['PHONE'] ?? [];
                if (is_array($phones)) {
                    foreach ($phones as $p) {
                        $val = $p['VALUE'] ?? '';
                        if ($val) {
                            $found = $this->boxAPI->findCompanyByPhone($val);
                            if ($found) return $found;
                        }
                    }
                }
                return null;
            case 'EMAIL':
                $emails = $company['EMAIL'] ?? [];
                if (is_array($emails)) {
                    foreach ($emails as $e) {
                        $val = $e['VALUE'] ?? '';
                        if ($val) {
                            $found = $this->boxAPI->findCompanyByEmail($val);
                            if ($found) return $found;
                        }
                    }
                }
                return null;
        }
        return null;
    }

    private function findContactDuplicate($contact, $criterion)
    {
        switch ($criterion) {
            case 'EMAIL':
                $emails = $contact['EMAIL'] ?? [];
                if (is_array($emails)) {
                    foreach ($emails as $e) {
                        $val = $e['VALUE'] ?? '';
                        if ($val) {
                            $found = $this->boxAPI->findContactByEmail($val);
                            if ($found) return $found;
                        }
                    }
                }
                return null;
            case 'PHONE':
                $phones = $contact['PHONE'] ?? [];
                if (is_array($phones)) {
                    foreach ($phones as $p) {
                        $val = $p['VALUE'] ?? '';
                        if ($val) {
                            $found = $this->boxAPI->findContactByPhone($val);
                            if ($found) return $found;
                        }
                    }
                }
                return null;
            case 'FULL_NAME':
                $name = $contact['NAME'] ?? '';
                $lastName = $contact['LAST_NAME'] ?? '';
                if ($name && $lastName) {
                    return $this->boxAPI->findContactByName($name, $lastName);
                }
                return null;
        }
        return null;
    }

    private function findLeadDuplicate($lead, $criterion)
    {
        switch ($criterion) {
            case 'TITLE':
                return !empty($lead['TITLE']) ? $this->boxAPI->findLeadByTitle($lead['TITLE']) : null;
            case 'EMAIL':
                $emails = $lead['EMAIL'] ?? [];
                if (is_array($emails)) {
                    foreach ($emails as $e) {
                        $val = $e['VALUE'] ?? '';
                        if ($val) {
                            $found = $this->boxAPI->findLeadByEmail($val);
                            if ($found) return $found;
                        }
                    }
                }
                return null;
            case 'PHONE':
                $phones = $lead['PHONE'] ?? [];
                if (is_array($phones)) {
                    foreach ($phones as $p) {
                        $val = $p['VALUE'] ?? '';
                        if ($val) {
                            $found = $this->boxAPI->findLeadByPhone($val);
                            if ($found) return $found;
                        }
                    }
                }
                return null;
        }
        return null;
    }

    // =========================================================================
    // Rate limiting & progress
    // =========================================================================

    private function rateLimit()
    {
        $this->opCount++;
        usleep(600000); // 600ms between operations (~1.6 req/s, within 2 req/s limit)

        // Check stop flag every operation
        $this->checkStop();

        if ($this->opCount % 100 === 0) {
            $this->addLog("Пауза 10 сек (каждые 100 операций, op={$this->opCount})...");
            sleep(10);
        }
    }

    private function setStatus($status, $message)
    {
        Option::set($this->moduleId, 'migration_status', $status);
        Option::set($this->moduleId, 'migration_message', $message);
        $this->saveLog();
    }

    private function savePhaseStatus($phase, $status)
    {
        $phases = json_decode(Option::get($this->moduleId, 'migration_phases', '{}'), true) ?: [];
        $phases[$phase] = $status;
        Option::set($this->moduleId, 'migration_phases', json_encode($phases));
    }

    private function savePhaseProgress($phase, $offset, $total)
    {
        // Save every 10th item to avoid excessive DB writes
        if ($offset % 10 === 0 || $offset === $total || $offset === 1) {
            $progress = json_decode(Option::get($this->moduleId, 'migration_progress', '{}'), true) ?: [];
            $progress[$phase] = ['offset' => $offset, 'total' => $total];
            Option::set($this->moduleId, 'migration_progress', json_encode($progress));
        }
    }

    private function saveStats($stats)
    {
        $current = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $merged = array_merge($current, $stats);
        Option::set($this->moduleId, 'migration_stats', json_encode($merged));
    }

    private function addLog($msg)
    {
        $line = date('H:i:s') . ' ' . $msg;
        $this->log[] = $line;

        // Keep last 500 lines
        if (count($this->log) > 500) {
            $this->log = array_slice($this->log, -500);
        }
        $this->saveLog();

        // File log
        if ($this->logFile) {
            @file_put_contents($this->logFile, date('Y-m-d ') . $line . "\n", FILE_APPEND);
        }
    }

    private function saveLog()
    {
        Option::set($this->moduleId, 'migration_log', json_encode($this->log));
    }

    private function saveResult()
    {
        $result = [
            'completed_at' => date('Y-m-d H:i:s'),
            'maps' => [
                'users' => count($this->userMapCache),
                'departments' => count($this->deptMapCache),
                'companies' => count($this->companyMapCache),
                'contacts' => count($this->contactMapCache),
                'deals' => count($this->dealMapCache),
                'leads' => count($this->leadMapCache),
                'groups' => count($this->groupMapCache),
                'pipelines' => count($this->pipelineMapCache),
            ],
        ];
        Option::set($this->moduleId, 'migration_result', json_encode($result));
    }
}
