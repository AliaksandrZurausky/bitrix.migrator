<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Integration\CloudAPI;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

class MigrationService
{
    private $cloudAPI;
    private $boxAPI;
    private $plan;
    private $moduleId = 'bitrix_migrator';
    private $logFile = null;
    private $errorLogFile = null; // separate file for detailed error traces

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
    private $smartProcessMapCache = [];  // cloud entityTypeId => box entityTypeId
    private $smartItemMapCache = [];     // cloud item ID => box item ID
    private $smartItemsByType = [];      // boxEntityTypeId => [cloudItemId => boxItemId, ...]

    private $log = [];
    private $opCount = 0;
    private $saveMapEnabled = false; // persist created IDs to MigratorMap HL block
    private $migrationFolderId = 0;  // disk folder for file uploads

    // Migration phases in execution order
    const PHASES = [
        'departments',
        'users',
        'crm_fields',
        'pipelines',
        'currencies',
        'companies',
        'contacts',
        'leads',
        'deals',
        'requisites',
        'smart_processes',
        'workgroups',
        'timeline',
        'tasks',
    ];

    const PHASE_LABELS = [
        'departments'     => 'Подразделения',
        'users'           => 'Пользователи',
        'crm_fields'      => 'CRM пользовательские поля',
        'pipelines'       => 'Воронки сделок',
        'currencies'      => 'Валюты',
        'companies'       => 'Компании',
        'contacts'        => 'Контакты',
        'leads'           => 'Лиды',
        'deals'           => 'Сделки',
        'requisites'      => 'Реквизиты',
        'smart_processes' => 'Смарт-процессы',
        'workgroups'      => 'Рабочие группы',
        'timeline'        => 'Таймлайн и активности',
        'tasks'           => 'Задачи',
    ];

    public function __construct(CloudAPI $cloudAPI, CloudAPI $boxAPI, array $plan)
    {
        $this->cloudAPI = $cloudAPI;
        $this->boxAPI = $boxAPI;
        $this->plan = $plan;
        $this->saveMapEnabled = !empty($plan['settings']['save_migrated_ids']);
    }

    public function setLogFile($path)
    {
        $this->logFile = $path;
        // Errors-only file: bitrix_migrator_DATE-errors.log
        $this->errorLogFile = preg_replace('/\.log$/', '-errors.log', $path);
    }

    public function migrate()
    {
        $isIncremental = !empty($this->plan['settings']['incremental']);
        $this->setStatus('running', $isIncremental ? 'Инкрементальная миграция запущена' : 'Миграция запущена');
        $this->addLog($isIncremental ? '=== Начало инкрементальной миграции ===' : '=== Начало полной миграции ===');
        if ($isIncremental) {
            $lastTs = Option::get($this->moduleId, 'last_migration_timestamp', '');
            $this->addLog('Фильтр: DATE_CREATE > ' . ($lastTs ?: '(не задан, будет полная выборка)'));
        }

        try {
            // --- Step 0a: Delete previously migrated data via HL block (if enabled) ---
            if (!empty($this->plan['settings']['delete_migrated_data'])) {
                $this->runHlBlockCleanup();
            }

            // --- Step 0b: Run all cleanups first, before any migration ---
            $this->runAllCleanups();

            // --- Step 0c: Build user map (cloud→box by email) — needed by all phases ---
            $this->buildUserMapCache();

            // --- Step 0d: Create migration folder on shared disk for file uploads ---
            try {
                $this->migrationFolderId = BoxD7Service::getOrCreateMigrationFolder('Миграция с облака');
                $this->addLog('Папка миграции на диске: ID ' . $this->migrationFolderId);
            } catch (\Throwable $e) {
                $this->addLog('ВНИМАНИЕ: не удалось создать папку миграции на диске: ' . $e->getMessage());
            }

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

                // Free memory between phases — Bitrix cache engine leaks memory
                gc_collect_cycles();
                try {
                    $app = \Bitrix\Main\Application::getInstance();
                    $app->getManagedCache()->cleanAll();
                    $app->getTaggedCache()->clearByTag('*');
                } catch (\Throwable $e) {}
                $this->addLog('  Память: ' . round(memory_get_usage(true) / 1024 / 1024) . 'MB');
            }

            Option::set($this->moduleId, 'last_migration_timestamp', date('c'));
            $this->setStatus('completed', 'Миграция завершена успешно');
            $this->addLog('=== Миграция завершена ===');

        } catch (MigrationStoppedException $e) {
            $this->addLog('=== Миграция остановлена пользователем ===');
            $this->setStatus('stopped', 'Миграция остановлена пользователем');
        } catch (\Throwable $e) {
            $msg = $e->getMessage() . ' in ' . $e->getFile() . ':' . $e->getLine();
            $this->addLog('FATAL ERROR: ' . $msg);
            $this->setStatus('error', 'Ошибка: ' . $e->getMessage());
        }

        $this->saveResult();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Extract human-readable label text from a string or language-map array.
     * Handles both `"Название"` (string) and `{"ru":"Название","en":"Name"}` (map).
     */
    private static function extractLabel($label): string
    {
        if (is_string($label) && $label !== '') {
            return $label;
        }
        if (is_array($label)) {
            foreach (['ru', 'en', 'de', 'pl', 'ua'] as $lang) {
                if (!empty($label[$lang])) return $label[$lang];
            }
        }
        return '';
    }

    // =========================================================================
    // Stop check
    // =========================================================================

    private $checkStopCounter = 0;

    private function checkStop()
    {
        $this->checkStopCounter++;
        // Periodic memory cleanup every 100 iterations — Bitrix cache engine leaks heavily
        if ($this->checkStopCounter % 100 === 0) {
            gc_collect_cycles();
            try {
                $app = \Bitrix\Main\Application::getInstance();
                $app->getManagedCache()->cleanAll();
                $app->getTaggedCache()->clearByTag('*');
            } catch (\Throwable $e) {}
        }

        $flag = Option::get($this->moduleId, 'migration_stop', '0');
        if ($flag === '1') {
            throw new MigrationStoppedException('Остановлено пользователем');
        }
        // Pause loop — process stays alive, caches preserved
        while ($flag === 'pause') {
            $this->setStatus('paused', 'Миграция на паузе');
            sleep(3);
            $flag = Option::get($this->moduleId, 'migration_stop', '0');
            if ($flag === '1') {
                throw new MigrationStoppedException('Остановлено пользователем');
            }
        }
        // Resumed — restore running status
        if ($flag === '0') {
            $currentStatus = Option::get($this->moduleId, 'migration_status', '');
            if ($currentStatus === 'paused') {
                $this->setStatus('running', 'Миграция продолжена');
            }
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
            'currencies'      => 'currencies',
            'companies'       => 'companies',
            'contacts'        => 'contacts',
            'deals'           => 'deals',
            'leads'           => 'leads',
            'requisites'      => 'requisites',
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
        $depthOf = function ($id, $visited = []) use ($deptById, &$depthOf) {
            if (in_array($id, $visited)) return 0; // cycle detected
            if (!isset($deptById[$id]) || empty($deptById[$id]['PARENT']) || (int)$deptById[$id]['PARENT'] === 0) {
                return 0;
            }
            $visited[] = $id;
            return 1 + $depthOf((int)$deptById[$id]['PARENT'], $visited);
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
            } catch (\Throwable $e) {
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
                } catch (\Throwable $e) {
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
            } catch (\Throwable $e) {
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
            $this->checkStop();
            $this->savePhaseProgress('users', $i + 1, $total + count($existingToUpdate));

            // Skip users without email — user.add requires email
            if (empty(trim($u['EMAIL'] ?? ''))) {
                $this->addLog("  Пропуск пользователя без email: {$u['NAME']} {$u['LAST_NAME']}");
                $errors++;
                continue;
            }

            try {
                $boxDeptIds = $this->mapDepartmentIds($u['UF_DEPARTMENT'] ?? []);

                $fields = [
                    'EMAIL' => trim($u['EMAIL']),
                    'NAME' => trim($u['NAME'] ?? ''),
                    'LAST_NAME' => trim($u['LAST_NAME'] ?? ''),
                    'SECOND_NAME' => trim($u['SECOND_NAME'] ?? ''),
                    'WORK_POSITION' => trim($u['WORK_POSITION'] ?? ''),
                    'PERSONAL_PHONE' => trim($u['PERSONAL_PHONE'] ?? ''),
                    'PERSONAL_MOBILE' => trim($u['PERSONAL_MOBILE'] ?? ''),
                    'WORK_PHONE' => trim($u['WORK_PHONE'] ?? ''),
                ];

                // UF_DEPARTMENT is required for user.add on box — fallback to root dept
                if (!empty($boxDeptIds)) {
                    $fields['UF_DEPARTMENT'] = $boxDeptIds;
                } else {
                    // Assign to root department if no dept mapping available
                    $rootDeptId = reset($this->deptMapCache);
                    if ($rootDeptId) {
                        $fields['UF_DEPARTMENT'] = [$rootDeptId];
                    }
                }

                // Personal info — normalize birthday to YYYY-MM-DD
                if (!empty($u['PERSONAL_GENDER'])) $fields['PERSONAL_GENDER'] = $u['PERSONAL_GENDER'];
                if (!empty($u['PERSONAL_BIRTHDAY'])) {
                    // Strip time/timezone: "1991-10-17T03:00:00+02:00" → "1991-10-17"
                    $bday = $u['PERSONAL_BIRTHDAY'];
                    if (preg_match('/^(\d{4}-\d{2}-\d{2})/', $bday, $m)) {
                        $bday = $m[1];
                    }
                    $fields['PERSONAL_BIRTHDAY'] = $bday;
                }
                if (!empty($u['PERSONAL_PHOTO'])) $fields['PERSONAL_PHOTO'] = $u['PERSONAL_PHOTO'];

                // Preserve fired/inactive status — don't invite inactive users
                $active = $u['ACTIVE'] ?? 'Y';
                $isActive = ($active === 'Y' || $active === true);
                $fields['ACTIVE'] = $isActive ? 'Y' : 'N';

                $sendInvite = $isActive && ($this->plan['settings']['send_invite'] ?? 'N') === 'Y';
                if (!empty($u['DATE_REGISTER'])) $fields['DATE_REGISTER'] = $u['DATE_REGISTER'];
                $newId = BoxD7Service::createUser($fields, $sendInvite);
                if ($newId) {
                    $this->userMapCache[(int)$u['ID']] = $newId;
                    $invited++;
                }
            } catch (\Throwable $e) {
                $errors++;
                $errDetails = $e->getMessage() . ' in ' . $e->getFile() . ':' . $e->getLine();
                $this->addLog("  Ошибка создания пользователя {$u['EMAIL']}: $errDetails");
                // Log full fields for debugging
                $this->addLog("    Отправленные поля: " . json_encode($fields, JSON_UNESCAPED_UNICODE));
            }
        }

        // --- Update existing users (departments, position) ---
        foreach ($existingToUpdate as $j => $u) {
            $this->checkStop();
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
                    BoxD7Service::updateUser($boxUserId, $updateFields);
                    $updated++;
                }
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка обновления пользователя {$u['EMAIL']}: " . $e->getMessage() . ' in ' . $e->getFile() . ':' . $e->getLine());
            }
        }

        $matched = count($this->userMapCache) - $invited;
        $this->addLog("Пользователи: совпали=$matched, создано=$invited, обновлено=$updated, ошибок=$errors");

        // Отключаем email-уведомления всем пользователям на коробке
        try {
            $disabled = BoxD7Service::disableEmailNotificationsForAll();
            $this->addLog("Email-уведомления отключены для $disabled пользователей");
        } catch (\Throwable $e) {
            $this->addLog("Ошибка отключения email-уведомлений: " . $e->getMessage());
        }
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

    /**
     * Check if cleanup is enabled for a given phase in the plan.
     */
    private function isCleanupEnabled($phase)
    {
        return ($this->plan['cleanup'] ?? [])[$phase] ?? false;
    }

    /**
     * Delete only previously migrated entities using IDs stored in MigratorMap HL block.
     * Runs before main cleanup and migration phases.
     */
    private function runHlBlockCleanup(): void
    {
        if (!Loader::includeModule('highloadblock')) {
            $this->addLog('[HL cleanup] Модуль highloadblock не загружен, пропуск');
            return;
        }

        $this->addLog("--- Удаление ранее перенесённых данных (HL блок) ---");
        $this->setStatus('running', 'Удаление ранее перенесённых данных...');

        // Entity type → D7 delete method or API fallback
        // Delete in dependency order: deals first (they reference contacts/companies)
        $entityOrder = ['deal', 'contact', 'company', 'lead'];

        foreach ($entityOrder as $entityType) {
            $this->checkStop();

            try {
                $ids = MapService::getAllLocalIds($entityType);
            } catch (\Throwable $e) {
                $this->addLog("[HL cleanup] Ошибка чтения MigratorMap ($entityType): " . $e->getMessage());
                continue;
            }

            if (empty($ids)) {
                $this->addLog("[HL cleanup] $entityType: нет данных в HL блоке");
                continue;
            }

            $this->addLog("[HL cleanup] $entityType: удаляем " . count($ids) . " записей...");
            $deleted = 0;
            $errors  = 0;

            foreach ($ids as $boxId) {
                $this->checkStop();
                try {
                    switch ($entityType) {
                        case 'company': BoxD7Service::deleteCompany($boxId); break;
                        case 'contact': BoxD7Service::deleteContact($boxId); break;
                        case 'deal':    BoxD7Service::deleteDeal($boxId);    break;
                        case 'lead':    BoxD7Service::deleteLead($boxId);    break;
                    }
                    $deleted++;
                } catch (\Throwable $e) {
                    $errors++;
                }
            }

            // Clear map entries for this entity type
            try {
                MapService::clearByEntityType($entityType);
            } catch (\Throwable $e) {
                $this->addLog("[HL cleanup] Ошибка очистки MigratorMap ($entityType): " . $e->getMessage());
            }

            $this->addLog("[HL cleanup] $entityType: удалено=$deleted, ошибок=$errors");
        }

        $this->addLog("--- HL блок очистка завершена ---");
    }

    /**
     * Save created entity ID to MigratorMap HL block (for future cleanup).
     */
    private function saveToMap(string $entityType, int $cloudId, int $boxId): void
    {
        if (!$this->saveMapEnabled) return;
        try {
            MapService::addMap($entityType, $cloudId, $boxId);
        } catch (\Throwable $e) {
            // Non-critical — don't interrupt migration
        }
    }

    /**
     * Build date filter for incremental migration.
     * Returns empty array for full migration.
     */
    private function getIncrementalFilter(): array
    {
        if (empty($this->plan['settings']['incremental'])) return [];
        $lastTs = Option::get($this->moduleId, 'last_migration_timestamp', '');
        if (empty($lastTs)) return [];
        return ['>DATE_CREATE' => $lastTs];
    }

    /**
     * Build date filter for incremental task migration.
     * Tasks use >CREATED_DATE instead of >DATE_CREATE.
     */
    private function getIncrementalTaskFilter(): array
    {
        if (empty($this->plan['settings']['incremental'])) return [];
        $lastTs = Option::get($this->moduleId, 'last_migration_timestamp', '');
        if (empty($lastTs)) return [];
        return ['>CREATED_DATE' => $lastTs];
    }

    /**
     * Check if entity was already migrated (exists in HL-block map).
     * Uses in-memory cache preloaded by loadExistingMappings or populated during migration.
     * Falls back to direct HL-block lookup if cache miss and saveMapEnabled.
     */
    private function isAlreadyMigrated(string $entityType, int $cloudId, string $cacheProperty): bool
    {
        if (isset($this->{$cacheProperty}[$cloudId])) return true;
        if (!$this->saveMapEnabled) return false;
        try {
            $localId = MapService::getLocalId($entityType, $cloudId);
            if ($localId) {
                $this->{$cacheProperty}[$cloudId] = (int)$localId;
                return true;
            }
        } catch (\Throwable $e) {}
        return false;
    }

    /**
     * In incremental mode, pre-load existing cloud→box mappings from MigratorMap HL block
     * so that timeline/activities/contacts linking still works for previously migrated entities.
     */
    private function loadExistingMappings(string $entityType): void
    {
        try {
            $existingMap = MapService::getAllMappings($entityType);
            $cacheProperty = $entityType . 'MapCache';

            foreach ($existingMap as $cloudId => $localId) {
                if (!isset($this->{$cacheProperty}[$cloudId])) {
                    $this->{$cacheProperty}[$cloudId] = $localId;
                }
            }

            $this->addLog("[incremental] Загружено существующих маппингов $entityType: " . count($existingMap));
        } catch (\Throwable $e) {
            $this->addLog("[incremental] Ошибка загрузки маппингов $entityType: " . $e->getMessage());
        }
    }

    /**
     * Run ALL enabled cleanups upfront before any migration phase starts.
     * Order matters: deals → contacts → companies → leads → pipelines → workgroups → smart_processes
     * (reverse dependency order so linked entities are deleted first)
     */
    private function runAllCleanups()
    {
        $cleanup = $this->plan['cleanup'] ?? [];
        if (empty($cleanup) || !array_filter($cleanup)) {
            return;
        }

        $this->addLog("--- Предварительная очистка данных на box ---");
        $this->setStatus('running', 'Очистка данных перед миграцией...');

        // Delete in dependency order: deals first (reference contacts/companies),
        // then contacts, companies, leads, pipelines, workgroups, smart_processes
        $order = ['tasks', 'deals', 'contacts', 'companies', 'leads', 'pipelines', 'workgroups', 'smart_processes'];

        foreach ($order as $phase) {
            if (empty($cleanup[$phase])) continue;
            $this->checkStop();

            switch ($phase) {
                case 'deals':
                case 'contacts':
                case 'companies':
                case 'leads':
                    $this->cleanupCrmEntity($phase);
                    break;

                case 'pipelines':
                    $this->cleanupPipelines();
                    break;

                case 'workgroups':
                    $this->cleanupWorkgroups();
                    break;

                case 'smart_processes':
                    $this->cleanupSmartProcesses();
                    break;

                case 'tasks':
                    $this->cleanupTasks();
                    break;
            }
        }

        $this->addLog("--- Очистка завершена, начинаем миграцию ---");
    }

    private function cleanupPipelines()
    {
        $this->addLog("Очистка воронок на box...");
        $boxCategories = $this->boxAPI->getDealCategories();
        foreach ($boxCategories as $bc) {
            $bcId = (int)($bc['ID'] ?? 0);
            if ($bcId === 0) continue;
            $this->rateLimit();
            try { $this->boxAPI->deleteDealCategory($bcId); } catch (\Throwable $e) {
                $this->addLog("  Ошибка удаления воронки '{$bc['NAME']}': " . $e->getMessage());
            }
        }
        // Clean non-system stages of default pipeline
        $boxStatuses = $this->boxAPI->getStatuses();
        foreach ($boxStatuses as $s) {
            $eid = $s['ENTITY_ID'] ?? '';
            if (($eid === 'DEAL_STAGE' || strncmp($eid, 'DEAL_STAGE_', 11) === 0)
                && ($s['SYSTEM'] ?? 'N') !== 'Y'
            ) {
                $this->rateLimit();
                try { $this->boxAPI->deleteStatus((int)$s['ID']); } catch (\Throwable $e) {}
            }
        }
        $this->addLog("Очистка воронок выполнена");
    }

    private function cleanupTasks()
    {
        $this->addLog("Очистка задач на box...");
        $allTasks = $this->boxAPI->fetchAll('tasks.task.list', ['select' => ['ID']], 'tasks');
        $total = count($allTasks);
        $deleted = 0;
        $errors = 0;

        foreach ($allTasks as $t) {
            $taskId = (int)($t['id'] ?? $t['ID'] ?? 0);
            if ($taskId <= 0) continue;
            $this->rateLimit();
            $this->checkStop();
            try {
                $this->boxAPI->deleteTask($taskId);
                $deleted++;
            } catch (\Throwable $e) {
                $errors++;
                if ($errors <= 3) {
                    $this->addLog("  Ошибка удаления задачи #$taskId: " . $e->getMessage());
                }
            }
        }

        $this->addLog("Очистка задач: удалено=$deleted, ошибок=$errors из $total");
    }

    private function cleanupWorkgroups()
    {
        $this->addLog("Очистка рабочих групп на box (D7, включая секретные)...");
        $groups = BoxD7Service::getAllWorkgroups();
        $deleted = 0;
        foreach ($groups as $g) {
            $this->checkStop();
            try {
                BoxD7Service::deleteWorkgroup((int)$g['ID']);
                $deleted++;
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка удаления группы #{$g['ID']}: " . $e->getMessage());
            }
        }
        $this->addLog("Очистка рабочих групп: удалено $deleted из " . count($groups));
    }

    private function cleanupSmartProcesses()
    {
        $this->addLog("Очистка смарт-процессов на box...");
        $boxTypes = $this->boxAPI->getSmartProcessTypes();
        foreach ($boxTypes as $t) {
            $etId = (int)($t['entityTypeId'] ?? 0);
            if (!$etId) continue;
            try {
                $items = $this->boxAPI->getSmartProcessItems($etId, ['id']);
                foreach ($items as $item) {
                    $this->rateLimit();
                    try { $this->boxAPI->deleteSmartProcessItem($etId, (int)$item['id']); } catch (\Throwable $e) {}
                }
            } catch (\Throwable $e) {}
            $this->rateLimit();
            try { $this->boxAPI->deleteSmartProcessType((int)$t['id']); } catch (\Throwable $e) {
                $this->addLog("  Не удалось удалить SP '{$t['title']}': " . $e->getMessage());
            }
        }
        $this->addLog("Очистка смарт-процессов выполнена");
    }

    /**
     * Delete all items of a CRM entity type on box via D7 (companies, contacts, deals, leads).
     */
    private function cleanupCrmEntity($entityType)
    {
        $d7Map = [
            'companies' => ['list' => 'crm.company.list', 'd7' => 'deleteCompany'],
            'contacts'  => ['list' => 'crm.contact.list', 'd7' => 'deleteContact'],
            'deals'     => ['list' => 'crm.deal.list',    'd7' => 'deleteDeal'],
            'leads'     => ['list' => 'crm.lead.list',    'd7' => 'deleteLead'],
        ];

        $cfg = $d7Map[$entityType] ?? null;
        if (!$cfg) return;

        $this->addLog("Очистка $entityType на box (D7)...");
        // Fetch IDs via REST (reading), delete via D7 (fast, no rate limit)
        $items = $this->boxAPI->fetchAll($cfg['list'], ['select' => ['ID']]);
        $total = count($items);
        $deleted = 0;

        $errors = 0;
        $firstError = '';
        foreach ($items as $item) {
            $this->checkStop();
            try {
                BoxD7Service::{$cfg['d7']}((int)$item['ID']);
                $deleted++;
            } catch (\Throwable $e) {
                $errors++;
                if ($errors <= 3) {
                    $firstError .= "  ID={$item['ID']}: {$e->getMessage()}\n";
                }
            }
        }

        $this->addLog("Очистка $entityType: удалено=$deleted, ошибок=$errors из $total");
        if ($firstError) {
            $this->addLog("Первые ошибки удаления ($entityType):\n$firstError");
        }
    }

    // =========================================================================
    // Phase 3: CRM Custom Fields
    // =========================================================================

    private function migrateCrmFields()
    {
        $entityTypes = ['deal', 'contact', 'company', 'lead'];
        $totalCreated = 0;
        $totalDeleted = 0;

        // Count total fields across all entity types for progress
        $allCloudFields = [];
        foreach ($entityTypes as $et) {
            $allCloudFields[$et] = $this->cloudAPI->getUserfields($et);
        }
        $grandTotal = 0;
        foreach ($allCloudFields as $fields) {
            $grandTotal += count($fields);
        }
        $progressCounter = 0;

        // Setting: which entity types should have fields deleted before migration
        $deleteFieldsSettings = $this->plan['delete_userfields'] ?? [];
        $deleteFieldsEnabled = ($deleteFieldsSettings['enabled'] ?? false) === true;

        foreach ($entityTypes as $entityType) {
            $cloudFields = $allCloudFields[$entityType];
            $boxFields = BoxD7Service::getUserfields($entityType);

            // Get formLabel from crm.*.fields — more reliable than EDIT_FORM_LABEL in userfield.list
            $fieldSchema = [];
            try {
                $schema = $this->cloudAPI->getFields('crm.' . $entityType . '.fields');
                foreach ($schema as $fname => $fdef) {
                    if (!empty($fdef['formLabel'])) {
                        $fieldSchema[$fname] = $fdef['formLabel'];
                    }
                }
            } catch (\Throwable $e) {
                // non-critical, fall back to EDIT_FORM_LABEL
            }

            // --- Step 1: Delete existing userfields on box if enabled ---
            $shouldDelete = $deleteFieldsEnabled
                && !in_array($entityType, $deleteFieldsSettings['skip_entities'] ?? []);

            if ($shouldDelete && !empty($boxFields)) {
                $this->addLog("Удаление пользовательских полей ($entityType): " . count($boxFields) . " шт.");
                foreach ($boxFields as $f) {
                    $fId = (int)($f['ID'] ?? 0);
                    $fName = $f['FIELD_NAME'] ?? '';
                    if (!$fId) continue;
                    try {
                        BoxD7Service::deleteUserfield($fId);
                        $totalDeleted++;
                    } catch (\Throwable $e) {
                        $this->addLog("  Ошибка удаления поля $fName ($entityType): " . $e->getMessage());
                    }
                }
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
                $progressCounter++;
                $this->savePhaseProgress('crm_fields', $progressCounter, $grandTotal);
                $fieldName = $field['FIELD_NAME'] ?? '';

                if (isset($this->plan['crm_custom_fields'][$entityType]['excluded_fields'])) {
                    if (in_array($fieldName, $this->plan['crm_custom_fields'][$entityType]['excluded_fields'])) {
                        continue;
                    }
                }

                if (in_array($fieldName, $boxFieldNames)) continue;

                // Skip field types that are module-specific and can't be created on box
                $userTypeId = $field['USER_TYPE_ID'] ?? 'string';
                $unmigrateableTypes = ['im_openlines', 'crm_onetomany'];
                if (in_array($userTypeId, $unmigrateableTypes)) {
                    $this->addLog("  Пропуск поля $fieldName (тип '$userTypeId' — открытые линии, требует ручной настройки)");
                    continue;
                }

                try {
                    $editLabel   = $field['EDIT_FORM_LABEL']   ?? [];
                    $listLabel   = $field['LIST_COLUMN_LABEL']  ?? [];
                    $filterLabel = $field['LIST_FILTER_LABEL']  ?? [];

                    // Priority 1: formLabel from crm.*.fields (plain string, always populated)
                    // Priority 2: EDIT_FORM_LABEL / LIST_COLUMN_LABEL (string or language map)
                    // Priority 3: XML_ID / FIELD_NAME as last resort
                    if (!empty($fieldSchema[$fieldName])) {
                        $labelText = $fieldSchema[$fieldName];
                    } else {
                        $labelText = self::extractLabel($editLabel);
                        if ($labelText === '') {
                            $labelText = self::extractLabel($listLabel);
                        }
                        if ($labelText === '') {
                            $labelText = $field['XML_ID'] ?? $fieldName;
                        }
                    }
                    $editLabel = ['ru' => $labelText, 'en' => $labelText];

                    $newField = [
                        'FIELD_NAME' => $fieldName,
                        'USER_TYPE_ID' => $field['USER_TYPE_ID'] ?? 'string',
                        'XML_ID' => $field['XML_ID'] ?? $fieldName,
                        'EDIT_FORM_LABEL' => $editLabel,
                        'LIST_COLUMN_LABEL' => !empty($listLabel) ? $listLabel : $editLabel,
                        'LIST_FILTER_LABEL' => !empty($filterLabel) ? $filterLabel : $editLabel,
                        'SETTINGS' => $field['SETTINGS'] ?? [],
                    ];

                    if (!empty($field['SORT'])) $newField['SORT'] = $field['SORT'];
                    if (!empty($field['SHOW_FILTER'])) $newField['SHOW_FILTER'] = $field['SHOW_FILTER'];
                    if (!empty($field['SHOW_IN_LIST'])) $newField['SHOW_IN_LIST'] = $field['SHOW_IN_LIST'];
                    if (!empty($field['EDIT_IN_LIST'])) $newField['EDIT_IN_LIST'] = $field['EDIT_IN_LIST'];
                    if (!empty($field['IS_SEARCHABLE'])) $newField['IS_SEARCHABLE'] = $field['IS_SEARCHABLE'];
                    if (!empty($field['MANDATORY'])) $newField['MANDATORY'] = $field['MANDATORY'];
                    if (!empty($field['MULTIPLE'])) $newField['MULTIPLE'] = $field['MULTIPLE'];
                    if (!empty($field['LIST'])) $newField['LIST'] = $field['LIST'];

                    $this->boxAPI->addUserfield($entityType, $newField);
                    $totalCreated++;
                } catch (\Throwable $e) {
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

        $cloudStagesGrouped = $this->cloudAPI->getAllDealStagesGrouped();

        // --- Default pipeline (ID=0) ---
        $this->pipelineMapCache[0] = 0;

        // Rename default pipeline on box to match cloud
        $cloudDefaultCat = null;
        foreach ($cloudCategories as $cat) {
            if ((int)($cat['ID'] ?? -1) === 0) {
                $cloudDefaultCat = $cat;
                break;
            }
        }
        // If crm.dealcategory.list doesn't return ID=0, try crm.dealcategory.get
        if (!$cloudDefaultCat) {
            $cloudDefaultCat = $this->cloudAPI->getDealCategoryById(0);
        }
        if ($cloudDefaultCat && !empty($cloudDefaultCat['NAME'])) {
            try {
                $this->boxAPI->updateDealCategory(0, ['NAME' => $cloudDefaultCat['NAME']]);
                $this->addLog("Основная воронка переименована: '{$cloudDefaultCat['NAME']}'");
            } catch (\Throwable $e) {
                $this->addLog("  Не удалось переименовать основную воронку: " . $e->getMessage());
            }
        }

        // Sync default pipeline stages
        $this->syncPipelineStages('DEAL_STAGE', 'DEAL_STAGE', $cloudStagesGrouped['DEAL_STAGE'] ?? []);

        // --- Custom pipelines ---
        $boxCatByName = [];
        foreach ($boxCategories as $c) {
            $boxCatByName[mb_strtolower(trim($c['NAME'] ?? ''))] = $c;
        }

        $created = 0;
        $total = count($cloudCategories);
        foreach ($cloudCategories as $i => $cat) {
            $this->rateLimit();
            $this->savePhaseProgress('pipelines', $i + 1, $total);
            $catId = (int)($cat['ID'] ?? 0);
            if ($catId === 0) continue; // Already handled above

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
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка создания воронки '{$name}': " . $e->getMessage());
                    continue;
                }
            }

            // Sync stages for this pipeline
            if (isset($this->pipelineMapCache[$catId])) {
                $cloudEntityId = 'DEAL_STAGE_' . $catId;
                $boxEntityId = 'DEAL_STAGE_' . $this->pipelineMapCache[$catId];
                $stages = $cloudStagesGrouped[$cloudEntityId] ?? [];
                $this->syncPipelineStages($cloudEntityId, $boxEntityId, $stages);
            }
        }

        $this->addLog("Воронки: создано $created, всего маппингов=" . count($this->pipelineMapCache));
    }

    private function syncPipelineStages($cloudEntityId, $boxEntityId, $cloudStages)
    {
        if (empty($cloudStages)) return;

        // Get current box stages for this entity
        $boxStatuses = $this->boxAPI->getStatuses();
        $boxStagesByStatusId = [];
        $boxStagesByName = [];
        foreach ($boxStatuses as $s) {
            if (($s['ENTITY_ID'] ?? '') === $boxEntityId) {
                $boxStagesByStatusId[$s['STATUS_ID']] = $s;
                $name = mb_strtolower(trim($s['NAME'] ?? ''));
                if ($name) $boxStagesByName[$name] = $s;
            }
        }

        foreach ($cloudStages as $stage) {
            $this->rateLimit();
            $cloudStatusId = $stage['STATUS_ID'] ?? '';

            // Remap STATUS_ID prefix for custom pipelines: C5:NEW → C12:NEW
            $boxStatusId = $cloudStatusId;
            if ($cloudEntityId !== $boxEntityId && preg_match('/^C(\d+):(.+)$/', $cloudStatusId, $m)) {
                $boxCatId = str_replace('DEAL_STAGE_', '', $boxEntityId);
                $boxStatusId = 'C' . $boxCatId . ':' . $m[2];
            }

            if (isset($boxStagesByStatusId[$boxStatusId])) {
                $this->stageMapCache[$cloudStatusId] = $boxStatusId;
            } else {
                // Try match by name (stage may exist with different prefix)
                $stageName = mb_strtolower(trim($stage['NAME'] ?? ''));
                if ($stageName && isset($boxStagesByName[$stageName])) {
                    $this->stageMapCache[$cloudStatusId] = $boxStagesByName[$stageName]['STATUS_ID'];
                } else {
                    // Create stage on box with remapped STATUS_ID
                    try {
                        $this->boxAPI->addStatus([
                            'ENTITY_ID' => $boxEntityId,
                            'STATUS_ID' => $boxStatusId,
                            'NAME' => $stage['NAME'] ?? $cloudStatusId,
                            'SORT' => $stage['SORT'] ?? 10,
                            'COLOR' => $stage['COLOR'] ?? '',
                            'SEMANTICS' => $stage['SEMANTICS'] ?? '',
                        ]);
                        $this->stageMapCache[$cloudStatusId] = $boxStatusId;
                    } catch (\Throwable $e) {
                        $this->stageMapCache[$cloudStatusId] = $boxStatusId;
                        $this->addLog("  Ошибка создания стадии '{$stage['NAME']}' ($boxStatusId): " . $e->getMessage());
                    }
                }
            }
        }
    }

    // =========================================================================
    // Phase 5: Currencies
    // =========================================================================

    private function migrateCurrencies()
    {
        $cloudCurrencies = $this->cloudAPI->getCurrencies();
        $boxCurrencyCodes = $this->boxAPI->getCurrencyCodes();

        $created = 0;
        $skipped = 0;

        foreach ($cloudCurrencies as $currency) {
            $code = $currency['CURRENCY'] ?? '';
            if (empty($code)) continue;

            if (isset($boxCurrencyCodes[$code])) {
                $skipped++;
                continue;
            }

            $this->rateLimit();

            // Fields accepted by crm.currency.add
            $fields = ['CURRENCY' => $code];
            $copyFields = ['AMOUNT_CNT', 'AMOUNT', 'SORT', 'FULL_NAME', 'DECIMALS',
                           'DEC_POINT', 'THOUSANDS_SEP', 'FORMAT_STRING', 'HIDE_ZERO'];
            foreach ($copyFields as $f) {
                if (isset($currency[$f]) && $currency[$f] !== '') {
                    $fields[$f] = $currency[$f];
                }
            }

            try {
                $this->boxAPI->addCurrency($fields);
                $created++;
                $this->addLog("  Валюта $code добавлена на коробку");
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка добавления валюты $code: " . $e->getMessage());
            }
        }

        $this->addLog("Валюты: добавлено=$created, уже существовало=$skipped из " . count($cloudCurrencies));
    }

    // =========================================================================
    // Phase 6: Companies
    // =========================================================================

    private function migrateCompanies()
    {
        // In incremental mode, pre-load existing mappings for timeline/activities
        if (!empty($this->plan['settings']['incremental'])) {
            $this->loadExistingMappings('company');
        }

        $total = $this->cloudAPI->getCompaniesCount();
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;
        $processed = 0;

        $dupSettings = $this->plan['duplicate_settings']['companies'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['TITLE'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        // Pre-build box companies title cache to avoid per-item API calls
        $boxCompaniesByTitle = [];
        if (in_array('TITLE', $matchBy)) {
            $this->boxAPI->fetchBatched('crm.company.list', ['select' => ['ID', 'TITLE']], function ($batch) use (&$boxCompaniesByTitle) {
                foreach ($batch as $c) {
                    $t = mb_strtolower(trim($c['TITLE'] ?? ''));
                    if ($t && !isset($boxCompaniesByTitle[$t])) $boxCompaniesByTitle[$t] = (int)$c['ID'];
                }
            });
            $this->addLog('Кэш компаний box: ' . count($boxCompaniesByTitle) . ' записей');
        }

        $params = ['select' => ['*', 'UF_*', 'PHONE', 'EMAIL']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;

        $self = $this;
        $this->cloudAPI->fetchBatched('crm.company.list', $params, function ($batch) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, $total, $dupSettings, $matchBy, $dupAction, &$boxCompaniesByTitle) {
        foreach ($batch as $company) {
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('companies', $processed, $total);
            $cloudId = (int)$company['ID'];

            // Skip if already migrated (HL-block check)
            if ($this->isAlreadyMigrated('company', $cloudId, 'companyMapCache')) {
                $skipped++;
                continue;
            }

            // Fast title lookup from pre-built cache
            $existing = null;
            $titleKey = mb_strtolower(trim($company['TITLE'] ?? ''));
            if ($titleKey && isset($boxCompaniesByTitle[$titleKey])) {
                $existing = ['ID' => $boxCompaniesByTitle[$titleKey]];
            } else {
                $nonTitleCriteria = array_values(array_filter($matchBy, fn($c) => $c !== 'TITLE'));
                if ($nonTitleCriteria) {
                    $existing = $this->findDuplicate('company', $company, $nonTitleCriteria);
                }
            }

            if ($existing) {
                $this->companyMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $this->buildCompanyFields($company);
                        BoxD7Service::updateCompany((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Throwable $e) {
                        $this->addLog("  Ошибка обновления компании #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $this->buildCompanyFields($company);
                if (!empty($company['DATE_CREATE'])) $fields['DATE_CREATE'] = $company['DATE_CREATE'];
                if (!empty($company['CREATED_BY_ID'])) {
                    $cb = $this->mapUser($company['CREATED_BY_ID']);
                    if ($cb) $fields['CREATED_BY_ID'] = $cb;
                }
                $newId = BoxD7Service::createCompany($fields);
                if ($newId) {
                    $this->companyMapCache[$cloudId] = $newId;
                    $this->saveToMap('company', $cloudId, $newId);
                    $created++;
                    if (!empty($company['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_company', $newId, $company['DATE_CREATE'], $company['DATE_MODIFY'] ?? '');
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания компании #{$cloudId}: " . $e->getMessage());
            }
        }
        }); // end fetchBatched callback

        $this->addLog("Компании: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['companies' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildCompanyFields($company)
    {
        $fields = [];
        $copy = ['TITLE', 'COMPANY_TYPE', 'INDUSTRY', 'EMPLOYEES', 'REVENUE', 'CURRENCY_ID',
                 'COMMENTS', 'OPENED', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                 'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                 'SOURCE_ID', 'SOURCE_DESCRIPTION', 'BANKING_DETAILS',
                 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT', 'UTM_TERM'];
        foreach ($copy as $k) {
            if (isset($company[$k]) && $company[$k] !== '') $fields[$k] = $company[$k];
        }

        // EMPLOYEES is required on some box setups — default to EMPLOYEES_1 if missing
        if (empty($fields['EMPLOYEES'])) {
            $fields['EMPLOYEES'] = 'EMPLOYEES_1';
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
        // In incremental mode, pre-load existing mappings for timeline/activities
        if (!empty($this->plan['settings']['incremental'])) {
            $this->loadExistingMappings('contact');
        }

        $total = $this->cloudAPI->getContactsCount();
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;
        $processed = 0;

        $dupSettings = $this->plan['duplicate_settings']['contacts'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['EMAIL'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        $params = ['select' => ['*', 'UF_*', 'PHONE', 'EMAIL']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;

        $self = $this;
        $this->cloudAPI->fetchBatched('crm.contact.list', $params, function ($batch) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, $total, $dupSettings, $matchBy, $dupAction) {
        foreach ($batch as $contact) {
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('contacts', $processed, $total);
            $cloudId = (int)$contact['ID'];

            if ($self->isAlreadyMigrated('contact', $cloudId, 'contactMapCache')) {
                $skipped++;
                continue;
            }

            $existing = $self->findDuplicate('contact', $contact, $matchBy);
            if ($existing) {
                $self->contactMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $self->buildContactFields($contact);
                        BoxD7Service::updateContact((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Throwable $e) {
                        $self->addLog("  Ошибка обновления контакта #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $self->buildContactFields($contact);
                if (!empty($contact['DATE_CREATE'])) $fields['DATE_CREATE'] = $contact['DATE_CREATE'];
                if (!empty($contact['CREATED_BY_ID'])) {
                    $cb = $self->mapUser($contact['CREATED_BY_ID']);
                    if ($cb) $fields['CREATED_BY_ID'] = $cb;
                }
                $newId = BoxD7Service::createContact($fields);
                if ($newId) {
                    $self->contactMapCache[$cloudId] = $newId;
                    $self->saveToMap('contact', $cloudId, $newId);
                    $created++;
                    if (!empty($contact['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_contact', $newId, $contact['DATE_CREATE'], $contact['DATE_MODIFY'] ?? '');
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }

                    $self->linkContactCompanies($cloudId, $newId, $contact);
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания контакта #{$cloudId}: " . $e->getMessage());
            }
        }
        }); // end fetchBatched callback

        $this->addLog("Контакты: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['contacts' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildContactFields($contact)
    {
        $fields = [];
        $copy = ['NAME', 'LAST_NAME', 'SECOND_NAME', 'POST', 'TYPE_ID', 'SOURCE_ID',
                 'SOURCE_DESCRIPTION', 'COMMENTS', 'OPENED', 'EXPORT',
                 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                 'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                 'BIRTHDATE', 'HONORIFIC',
                 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT', 'UTM_TERM'];
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
                $companyIds = array_column($boxItems, 'COMPANY_ID');
                BoxD7Service::setContactCompanies($boxContactId, $companyIds);
            }
        } catch (\Throwable $e) {
            // Non-critical
        }
    }

    // =========================================================================
    // Phase 7: Deals
    // =========================================================================

    private function migrateDeals()
    {
        // In incremental mode, pre-load existing mappings for timeline/activities
        if (!empty($this->plan['settings']['incremental'])) {
            $this->loadExistingMappings('deal');
        }

        $total = $this->cloudAPI->getDealsCount();
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;
        $processed = 0;

        $dupSettings = $this->plan['duplicate_settings']['deals'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['TITLE'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        // Pre-fetch box currencies to validate CURRENCY_ID before creation
        $boxCurrencies = [];
        try {
            $boxCurrencies = $this->boxAPI->getCurrencyCodes();
            $this->addLog('Валюты box: ' . implode(', ', array_keys($boxCurrencies)));
        } catch (\Throwable $e) {
            $this->addLog('Не удалось загрузить валюты box: ' . $e->getMessage());
        }

        // Pre-build box deals title cache to avoid per-item API calls
        $boxDealsByTitle = [];
        if (in_array('TITLE', $matchBy)) {
            $this->boxAPI->fetchBatched('crm.deal.list', ['select' => ['ID', 'TITLE']], function ($batch) use (&$boxDealsByTitle) {
                foreach ($batch as $d) {
                    $t = mb_strtolower(trim($d['TITLE'] ?? ''));
                    if ($t && !isset($boxDealsByTitle[$t])) $boxDealsByTitle[$t] = (int)$d['ID'];
                }
            });
            $this->addLog('Кэш сделок box: ' . count($boxDealsByTitle) . ' записей');
        }

        $params = ['select' => ['*', 'UF_*']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;

        $self = $this;
        $this->cloudAPI->fetchBatched('crm.deal.list', $params, function ($batch) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, $total, $dupSettings, $matchBy, $dupAction, &$boxDealsByTitle, $boxCurrencies) {
        foreach ($batch as $deal) {
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('deals', $processed, $total);
            $cloudId = (int)$deal['ID'];

            if ($self->isAlreadyMigrated('deal', $cloudId, 'dealMapCache')) {
                $skipped++;
                continue;
            }

            // Fast title lookup from pre-built cache
            $existing = null;
            $titleKey = mb_strtolower(trim($deal['TITLE'] ?? ''));
            if ($titleKey && isset($boxDealsByTitle[$titleKey])) {
                $existing = ['ID' => $boxDealsByTitle[$titleKey]];
            } else {
                $nonTitleCriteria = array_values(array_filter($matchBy, fn($c) => $c !== 'TITLE'));
                if ($nonTitleCriteria) {
                    $existing = $self->findDuplicate('deal', $deal, $nonTitleCriteria);
                }
            }

            if ($existing) {
                $self->dealMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $self->buildDealFields($deal);
                        if (!empty($fields['CURRENCY_ID']) && !empty($boxCurrencies)
                            && !isset($boxCurrencies[$fields['CURRENCY_ID']])) {
                            unset($fields['CURRENCY_ID'], $fields['OPPORTUNITY'], $fields['TAX_VALUE']);
                        }
                        BoxD7Service::updateDeal((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Throwable $e) {
                        $self->addLog("  Ошибка обновления сделки #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $self->buildDealFields($deal);
                // Drop CURRENCY_ID if it doesn't exist on box (avoids HTTP 400)
                if (!empty($fields['CURRENCY_ID']) && !empty($boxCurrencies)
                    && !isset($boxCurrencies[$fields['CURRENCY_ID']])) {
                    unset($fields['CURRENCY_ID'], $fields['OPPORTUNITY'], $fields['TAX_VALUE']);
                }
                if (!empty($deal['DATE_CREATE'])) $fields['DATE_CREATE'] = $deal['DATE_CREATE'];
                if (!empty($deal['CREATED_BY_ID'])) {
                    $cb = $self->mapUser($deal['CREATED_BY_ID']);
                    if ($cb) $fields['CREATED_BY_ID'] = $cb;
                }
                $newId = BoxD7Service::createDeal($fields);
                if ($newId) {
                    $self->dealMapCache[$cloudId] = $newId;
                    $self->saveToMap('deal', $cloudId, $newId);
                    $created++;
                    if (!empty($deal['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_deal', $newId, $deal['DATE_CREATE'], $deal['DATE_MODIFY'] ?? '');
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }

                    $self->linkDealContacts($cloudId, $newId);
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания сделки #{$cloudId}: " . $e->getMessage());
            }
        }
        }); // end fetchBatched callback

        $this->addLog("Сделки: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['deals' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildDealFields($deal)
    {
        $fields = [];
        $copy = ['TITLE', 'TYPE_ID', 'PROBABILITY', 'CURRENCY_ID', 'OPPORTUNITY',
                 'TAX_VALUE', 'COMMENTS', 'OPENED', 'CLOSED', 'BEGINDATE', 'CLOSEDATE',
                 'SOURCE_ID', 'SOURCE_DESCRIPTION', 'ADDITIONAL_INFO',
                 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT', 'UTM_TERM'];
        foreach ($copy as $k) {
            if (isset($deal[$k]) && $deal[$k] !== '') $fields[$k] = $deal[$k];
        }

        $catId = (int)($deal['CATEGORY_ID'] ?? 0);
        $boxCatId = $this->pipelineMapCache[$catId] ?? $catId;
        $fields['CATEGORY_ID'] = $boxCatId;

        if (!empty($deal['STAGE_ID'])) {
            $fields['STAGE_ID'] = $this->remapStageId($deal['STAGE_ID'], $catId, $boxCatId);
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

        if (!empty($deal['LEAD_ID']) && isset($this->leadMapCache[(int)$deal['LEAD_ID']])) {
            $fields['LEAD_ID'] = $this->leadMapCache[(int)$deal['LEAD_ID']];
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
                $contactIds = array_column($boxItems, 'CONTACT_ID');
                BoxD7Service::setDealContacts($boxDealId, $contactIds);
            }
        } catch (\Throwable $e) {
            // Non-critical
        }
    }

    // =========================================================================
    // Phase 8: Leads
    // =========================================================================

    private function migrateLeads()
    {
        // In incremental mode, pre-load existing mappings for timeline/activities
        if (!empty($this->plan['settings']['incremental'])) {
            $this->loadExistingMappings('lead');
        }

        $total = $this->cloudAPI->getLeadsCount();
        $created = 0;
        $skipped = 0;
        $updated = 0;
        $errors = 0;
        $processed = 0;

        $dupSettings = $this->plan['duplicate_settings']['leads'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['TITLE'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        // Reuse box currencies already fetched during deals phase (or fetch now)
        $boxCurrencies = [];
        try {
            $boxCurrencies = $this->boxAPI->getCurrencyCodes();
        } catch (\Throwable $e) {}

        $params = ['select' => ['*', 'UF_*', 'PHONE', 'EMAIL']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;

        $self = $this;
        $this->cloudAPI->fetchBatched('crm.lead.list', $params, function ($batch) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, $total, $dupSettings, $matchBy, $dupAction, $boxCurrencies) {
        foreach ($batch as $lead) {
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('leads', $processed, $total);
            $cloudId = (int)$lead['ID'];

            if ($self->isAlreadyMigrated('lead', $cloudId, 'leadMapCache')) {
                $skipped++;
                continue;
            }

            $existing = $self->findDuplicate('lead', $lead, $matchBy);
            if ($existing) {
                $self->leadMapCache[$cloudId] = (int)$existing['ID'];
                if ($dupAction === 'update') {
                    try {
                        $fields = $self->buildLeadFields($lead);
                        BoxD7Service::updateLead((int)$existing['ID'], $fields);
                        $updated++;
                    } catch (\Throwable $e) {
                        $self->addLog("  Ошибка обновления лида #{$cloudId}: " . $e->getMessage());
                    }
                } else if ($dupAction === 'skip') {
                    $skipped++;
                }
                if ($dupAction !== 'create') continue;
            }

            try {
                $fields = $self->buildLeadFields($lead);
                if (!empty($fields['CURRENCY_ID']) && !empty($boxCurrencies)
                    && !isset($boxCurrencies[$fields['CURRENCY_ID']])) {
                    unset($fields['CURRENCY_ID'], $fields['OPPORTUNITY']);
                }
                if (!empty($lead['DATE_CREATE'])) $fields['DATE_CREATE'] = $lead['DATE_CREATE'];
                if (!empty($lead['CREATED_BY_ID'])) {
                    $cb = $self->mapUser($lead['CREATED_BY_ID']);
                    if ($cb) $fields['CREATED_BY_ID'] = $cb;
                }
                $newId = BoxD7Service::createLead($fields);
                if ($newId) {
                    $self->leadMapCache[$cloudId] = $newId;
                    $self->saveToMap('lead', $cloudId, $newId);
                    $created++;
                    if (!empty($lead['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_lead', $newId, $lead['DATE_CREATE'], $lead['DATE_MODIFY'] ?? '');
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания лида #{$cloudId}: " . $e->getMessage());
            }
        }
        }); // end fetchBatched callback

        $this->addLog("Лиды: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['leads' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors]]);
    }

    private function buildLeadFields($lead)
    {
        $fields = [];
        $copy = ['TITLE', 'NAME', 'LAST_NAME', 'SECOND_NAME', 'STATUS_ID', 'STATUS_SEMANTIC_ID',
                 'SOURCE_ID', 'SOURCE_DESCRIPTION', 'CURRENCY_ID', 'OPPORTUNITY', 'COMMENTS', 'OPENED',
                 'COMPANY_TITLE', 'POST', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
                 'ADDRESS_POSTAL_CODE', 'ADDRESS_REGION', 'ADDRESS_PROVINCE', 'ADDRESS_COUNTRY',
                 'BIRTHDATE', 'HONORIFIC',
                 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN', 'UTM_CONTENT', 'UTM_TERM'];
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
        if (!empty($lead['WEB'])) $fields['WEB'] = $lead['WEB'];
        if (!empty($lead['IM'])) $fields['IM'] = $lead['IM'];

        foreach ($lead as $k => $v) {
            if (strncmp($k, 'UF_CRM_', 7) === 0 && $v !== '' && $v !== null) {
                $fields[$k] = $v;
            }
        }

        return $fields;
    }

    // =========================================================================
    // Phase 9: Requisites (companies + contacts)
    // =========================================================================

    private function migrateRequisites()
    {
        // entity type IDs: Company=4, Contact=3
        $entityMap = [
            4 => ['name' => 'company', 'cache' => &$this->companyMapCache],
            3 => ['name' => 'contact', 'cache' => &$this->contactMapCache],
        ];

        $totalReq = 0;
        $totalBank = 0;
        $errors = 0;

        foreach ($entityMap as $typeId => $cfg) {
            $cache = $cfg['cache'];
            $this->addLog("Реквизиты: обработка {$cfg['name']} (" . count($cache) . " шт)...");

            foreach ($cache as $cloudEntityId => $boxEntityId) {
                $this->rateLimit();
                try {
                    $requisites = $this->cloudAPI->getRequisites($typeId, $cloudEntityId);
                    foreach ($requisites as $req) {
                        $this->rateLimit();
                        $cloudReqId = (int)$req['ID'];
                        unset($req['ID'], $req['DATE_CREATE'], $req['DATE_MODIFY'],
                              $req['CREATED_BY_ID'], $req['MODIFY_BY_ID']);
                        $req['ENTITY_TYPE_ID'] = $typeId;
                        $req['ENTITY_ID'] = $boxEntityId;

                        try {
                            $boxReqId = BoxD7Service::addRequisite($req);
                            if ($boxReqId) {
                                $totalReq++;
                                // Migrate bank details for this requisite
                                try {
                                    $bankDetails = $this->cloudAPI->getBankDetails($cloudReqId);
                                    foreach ($bankDetails as $bank) {
                                        $this->rateLimit();
                                        unset($bank['ID'], $bank['DATE_CREATE'], $bank['DATE_MODIFY'],
                                              $bank['CREATED_BY_ID'], $bank['MODIFY_BY_ID']);
                                        $bank['ENTITY_ID'] = $boxReqId;
                                        try {
                                            BoxD7Service::addBankDetail($bank);
                                            $totalBank++;
                                        } catch (\Throwable $e) {
                                            $errors++;
                                        }
                                    }
                                } catch (\Throwable $e) {
                                    // non-critical
                                }
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            $this->addLog("  Ошибка реквизита ({$cfg['name']} box#{$boxEntityId}): " . $e->getMessage());
                        }
                    }
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка получения реквизитов ({$cfg['name']} cloud#{$cloudEntityId}): " . $e->getMessage());
                }
            }
        }

        $this->addLog("Реквизиты: создано=$totalReq, банковских реквизитов=$totalBank, ошибок=$errors");
    }

    // =========================================================================
    // Phase 10: Timeline (Activities & Comments)
    // =========================================================================

    private function migrateTimeline()
    {
        // Allow attaching files to timeline comments from any user context
        if (\Bitrix\Main\Loader::includeModule('disk')) {
            \Bitrix\Disk\Uf\FileUserType::setValueForAllowEdit('CRM_TIMELINE', true);
        }

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

        // For deduplication in main log: track first 3 unique error messages per phase
        $seenActivityErrors = [];
        $seenCommentErrors = [];

        // Log the errors file path so user knows where to look
        if ($this->errorLogFile) {
            $this->addLog("Детальные ошибки таймлайна пишутся в: " . basename($this->errorLogFile));
        }

        // Count total entities for progress
        $totalEntities = 0;
        foreach ($entityTypes as $config) {
            $totalEntities += count($config['cache']);
        }
        foreach ($this->smartItemsByType as $itemMap) {
            $totalEntities += count($itemMap);
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

                // Migrate activities via D7
                try {
                    $activities = $this->cloudAPI->getActivities($typeId, $cloudId);
                    foreach ($activities as $activity) {
                        $this->rateLimit();
                        $fields = $this->buildActivityFields($activity, $typeId, $boxId);

                        // Convert dates to Bitrix format DD.MM.YYYY HH:MI:SS
                        foreach (['START_TIME', 'END_TIME', 'DEADLINE'] as $dk) {
                            if (!empty($fields[$dk]) && !preg_match('/^\d{2}\.\d{2}\.\d{4}/', $fields[$dk])) {
                                $ts = strtotime($fields[$dk]);
                                $fields[$dk] = $ts ? date('d.m.Y H:i:s', $ts) : date('d.m.Y H:i:s');
                            }
                        }
                        if (empty($fields['END_TIME'])) $fields['END_TIME'] = $fields['START_TIME'] ?? date('d.m.Y H:i:s');
                        if (empty($fields['START_TIME'])) $fields['START_TIME'] = $fields['END_TIME'];

                        try {
                            // Step 1: Create activity without files
                            $actId = \CCrmActivity::Add($fields, false, false, ['REGISTER_SONET_EVENT' => false]);
                            if (!$actId) {
                                throw new \Exception(\CCrmActivity::GetLastErrorMessage() ?: 'CCrmActivity::Add returned 0');
                            }
                            $totalActivities++;
                            if (!empty($activity['CREATED'])) {
                                try {
                                    BoxD7Service::backdateEntity('b_crm_act', $actId, $activity['CREATED'], $activity['LAST_UPDATED'] ?? '');
                                } catch (\Throwable $e) { /* date preservation is non-critical */ }
                            }

                            // Step 2: Attach recording files separately
                            $actFiles = $activity['FILES'] ?? [];
                            if (!empty($actFiles)) {
                                foreach ($actFiles as $af) {
                                    $fileId = (int)($af['id'] ?? 0);
                                    if ($fileId <= 0) continue;
                                    try {
                                        $diskInfo = $this->cloudAPI->getDiskFile($fileId);
                                        $downloadUrl = $diskInfo['DOWNLOAD_URL'] ?? '';
                                        if (empty($downloadUrl)) continue;
                                        $boxDiskId = $this->downloadCloudFileToBox($downloadUrl, $diskInfo['NAME'] ?? "rec_{$fileId}.mp3");
                                        if ($boxDiskId > 0) {
                                            \CCrmActivity::Update($actId, [
                                                'STORAGE_TYPE_ID' => 3,
                                                'STORAGE_ELEMENT_IDS' => [$boxDiskId],
                                            ], false);
                                        }
                                    } catch (\Throwable $fe) { /* skip file error */ }
                                }
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            $errKey = substr($e->getMessage(), 0, 80);
                            $actInfo = $entityName . '#' . $cloudId . ' activity#' . ($activity['ID'] ?? '?') . ' type=' . ($activity['TYPE_ID'] ?? '?');
                            $this->addErrorDetail('timeline', $actInfo, $e->getMessage());
                            if (!isset($seenActivityErrors[$errKey])) {
                                $seenActivityErrors[$errKey] = 0;
                                $this->addLog("  Активность [$actInfo]: " . $e->getMessage());
                            }
                            $seenActivityErrors[$errKey]++;
                        }
                    }
                } catch (\Throwable $e) {
                    $this->addLog('  Ошибка получения активностей (' . $entityName . ' #' . $cloudId . '): ' . $e->getMessage());
                }

                // Migrate timeline comments via D7 CommentEntry::create
                try {
                    $comments = $this->cloudAPI->getTimelineComments($typeId, $cloudId);
                    foreach ($comments as $comment) {
                        $this->rateLimit();

                        // Download files via disk.file.get (auth URL, not urlDownload)
                        $diskFileIds = [];
                        $commentFiles = $comment['FILES'] ?? [];
                        if (!empty($commentFiles) && is_array($commentFiles)) {
                            foreach ($commentFiles as $cf) {
                                $cfId = (int)($cf['id'] ?? 0);
                                if ($cfId <= 0) continue;
                                try {
                                    $diskInfo = $this->cloudAPI->getDiskFile($cfId);
                                    $dlUrl = $diskInfo['DOWNLOAD_URL'] ?? '';
                                    if (empty($dlUrl)) continue;
                                    $boxDiskId = $this->downloadCloudFileToBox($dlUrl, $cf['name'] ?? $diskInfo['NAME'] ?? 'file');
                                    if ($boxDiskId > 0) $diskFileIds[] = $boxDiskId;
                                } catch (\Throwable $e) { /* skip */ }
                            }
                        }

                        try {
                            $authorId = 1;
                            if (!empty($comment['AUTHOR_ID'])) {
                                $mapped = $this->mapUser($comment['AUTHOR_ID']);
                                if ($mapped) $authorId = $mapped;
                            }
                            $createParams = [
                                'TEXT' => $comment['COMMENT'] ?? '',
                                'AUTHOR_ID' => $authorId,
                                'BINDINGS' => [['ENTITY_TYPE_ID' => $typeId, 'ENTITY_ID' => $boxId]],
                            ];
                            if (!empty($diskFileIds)) {
                                $createParams['FILES'] = array_map(fn($id) => 'n' . $id, $diskFileIds);
                                $createParams['SETTINGS'] = ['HAS_FILES' => 'Y'];
                            }
                            $commentId = \Bitrix\Crm\Timeline\CommentEntry::create($createParams);
                            $totalComments++;
                            if ($commentId > 0 && !empty($comment['CREATED'])) {
                                try {
                                    BoxD7Service::backdateEntity('b_crm_timeline', $commentId, $comment['CREATED']);
                                } catch (\Throwable $e) { /* date preservation is non-critical */ }
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            $errKey = substr($e->getMessage(), 0, 80);
                            $this->addErrorDetail('timeline-comment', $entityName . '#' . $cloudId, $e->getMessage());
                            if (!isset($seenCommentErrors[$errKey])) {
                                $seenCommentErrors[$errKey] = 0;
                                $this->addLog("  Комментарий (" . $entityName . ' #' . $cloudId . '): ' . $e->getMessage());
                            }
                            $seenCommentErrors[$errKey]++;
                        }
                    }
                } catch (\Throwable $e) {
                    // Non-critical
                }
            }
        }

        // Timeline for smart process items
        foreach ($this->smartItemsByType as $boxEntityTypeId => $itemMap) {
            $cloudEntityTypeId = array_search($boxEntityTypeId, $this->smartProcessMapCache);
            if ($cloudEntityTypeId === false) continue;

            $this->addLog("Таймлайн: обработка smart_{$boxEntityTypeId} (" . count($itemMap) . " шт)...");

            foreach ($itemMap as $cloudItemId => $boxItemId) {
                $this->rateLimit();
                $processedEntities++;
                $this->savePhaseProgress('timeline', $processedEntities, $totalEntities);

                // Migrate activities
                try {
                    $activities = $this->cloudAPI->getActivities($cloudEntityTypeId, $cloudItemId);
                    foreach ($activities as $activity) {
                        $this->rateLimit();
                        $fields = $this->buildActivityFields($activity, $boxEntityTypeId, $boxItemId);

                        foreach (['START_TIME', 'END_TIME', 'DEADLINE'] as $dk) {
                            if (!empty($fields[$dk]) && !preg_match('/^\d{2}\.\d{2}\.\d{4}/', $fields[$dk])) {
                                $ts = strtotime($fields[$dk]);
                                $fields[$dk] = $ts ? date('d.m.Y H:i:s', $ts) : date('d.m.Y H:i:s');
                            }
                        }
                        if (empty($fields['END_TIME'])) $fields['END_TIME'] = $fields['START_TIME'] ?? date('d.m.Y H:i:s');
                        if (empty($fields['START_TIME'])) $fields['START_TIME'] = $fields['END_TIME'];

                        try {
                            $actId = \CCrmActivity::Add($fields, false, false, ['REGISTER_SONET_EVENT' => false]);
                            if (!$actId) {
                                throw new \Exception(\CCrmActivity::GetLastErrorMessage() ?: 'CCrmActivity::Add returned 0');
                            }
                            $totalActivities++;
                            if (!empty($activity['CREATED'])) {
                                try {
                                    BoxD7Service::backdateEntity('b_crm_act', $actId, $activity['CREATED'], $activity['LAST_UPDATED'] ?? '');
                                } catch (\Throwable $e) { /* date preservation is non-critical */ }
                            }

                            $actFiles = $activity['FILES'] ?? [];
                            if (!empty($actFiles)) {
                                foreach ($actFiles as $af) {
                                    $fileId = (int)($af['id'] ?? 0);
                                    if ($fileId <= 0) continue;
                                    try {
                                        $diskInfo = $this->cloudAPI->getDiskFile($fileId);
                                        $downloadUrl = $diskInfo['DOWNLOAD_URL'] ?? '';
                                        if (empty($downloadUrl)) continue;
                                        $boxDiskId = $this->downloadCloudFileToBox($downloadUrl, $diskInfo['NAME'] ?? "rec_{$fileId}.mp3");
                                        if ($boxDiskId > 0) {
                                            \CCrmActivity::Update($actId, [
                                                'STORAGE_TYPE_ID' => 3,
                                                'STORAGE_ELEMENT_IDS' => [$boxDiskId],
                                            ], false);
                                        }
                                    } catch (\Throwable $fe) { /* skip file error */ }
                                }
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            $errKey = substr($e->getMessage(), 0, 80);
                            $actInfo = "smart_{$boxEntityTypeId}#{$cloudItemId} activity#" . ($activity['ID'] ?? '?') . ' type=' . ($activity['TYPE_ID'] ?? '?');
                            $this->addErrorDetail('timeline', $actInfo, $e->getMessage());
                            if (!isset($seenActivityErrors[$errKey])) {
                                $seenActivityErrors[$errKey] = 0;
                                $this->addLog("  Активность [$actInfo]: " . $e->getMessage());
                            }
                            $seenActivityErrors[$errKey]++;
                        }
                    }
                } catch (\Throwable $e) {
                    $this->addLog('  Ошибка получения активностей (smart_' . $boxEntityTypeId . ' #' . $cloudItemId . '): ' . $e->getMessage());
                }
            }
        }

        // Summary of unique error types
        if (!empty($seenActivityErrors)) {
            foreach ($seenActivityErrors as $msg => $count) {
                if ($count > 1) {
                    $this->addLog("  ^ Ошибка повторилась ещё $count раз: $msg");
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
            'OWNER_ID'      => $boxOwnerId,
            'TYPE_ID'       => $activity['TYPE_ID'] ?? 0,
            'SUBJECT'       => $activity['SUBJECT'] ?? '',
            'DESCRIPTION'   => $activity['DESCRIPTION'] ?? '',
            'DIRECTION'     => $activity['DIRECTION'] ?? 0,
            'COMPLETED'     => $activity['COMPLETED'] ?? 'N',
            'PRIORITY'      => $activity['PRIORITY'] ?? '2',
        ];

        if (!empty($activity['START_TIME'])) $fields['START_TIME'] = $activity['START_TIME'];
        if (!empty($activity['END_TIME']))   $fields['END_TIME']   = $activity['END_TIME'];
        if (!empty($activity['DEADLINE']))   $fields['DEADLINE']   = $activity['DEADLINE'];

        // Map responsible (required field — fallback to admin)
        $fields['RESPONSIBLE_ID'] = 1;
        if (!empty($activity['RESPONSIBLE_ID'])) {
            $boxUser = $this->mapUser($activity['RESPONSIBLE_ID']);
            if ($boxUser) $fields['RESPONSIBLE_ID'] = $boxUser;
        }

        // Provider (required for telephony/voip activities)
        if (!empty($activity['PROVIDER_ID']))      $fields['PROVIDER_ID']      = $activity['PROVIDER_ID'];
        if (!empty($activity['PROVIDER_TYPE_ID'])) $fields['PROVIDER_TYPE_ID'] = $activity['PROVIDER_TYPE_ID'];

        // ORIGIN_ID with VI_ prefix — required for call recording player in timeline
        if (($activity['PROVIDER_ID'] ?? '') === 'VOXIMPLANT_CALL' && !empty($activity['FILES'])) {
            $fields['ORIGIN_ID'] = 'VI_imported_' . ($activity['ID'] ?? 0) . '.' . time();
        }

        // Call-specific fields
        if (!empty($activity['PHONE_NUMBER']))  $fields['PHONE_NUMBER']  = $activity['PHONE_NUMBER'];
        if (isset($activity['CALL_DURATION']) && $activity['CALL_DURATION'] !== '')
            $fields['CALL_DURATION'] = $activity['CALL_DURATION'];

        // Communications (phone numbers, emails linked to activity)
        if (!empty($activity['COMMUNICATIONS']) && is_array($activity['COMMUNICATIONS'])) {
            $fields['COMMUNICATIONS'] = $activity['COMMUNICATIONS'];
        }

        return $fields;
    }

    private function buildTimelineCommentFields($comment, $entityTypeId, $boxEntityId)
    {
        // crm.timeline.comment.add requires ENTITY_TYPE as string
        $typeMap = [1 => 'lead', 2 => 'deal', 3 => 'contact', 4 => 'company'];
        $fields = [
            'ENTITY_TYPE' => $typeMap[$entityTypeId] ?? 'deal',
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

            $boxGroupId = 0;

            if (isset($boxByName[$lowerName])) {
                $boxGroupId = (int)$boxByName[$lowerName]['ID'];
                $this->groupMapCache[$cloudId] = $boxGroupId;
                $matched++;
                if ($conflictRes === 'skip') {
                    $this->addWorkgroupMembers($cloudId, $boxGroupId);
                    continue;
                }
            }

            if (!$boxGroupId) {
                try {
                    $fields = [
                        'NAME'        => $name,
                        'DESCRIPTION' => $group['DESCRIPTION'] ?? '',
                        'VISIBLE'     => $group['VISIBLE'] ?? 'Y',
                        'OPENED'      => $group['OPENED'] ?? 'Y',
                        'PROJECT'     => $group['PROJECT'] ?? 'N',
                        'SUBJECT_ID'  => (int)($group['SUBJECT_ID'] ?? 0), // 0 → BoxD7Service picks first available
                    ];

                    // Map owner — default to admin (1) if not found
                    $boxOwner = 1;
                    if (!empty($group['OWNER_ID'])) {
                        $mapped = $this->mapUser((int)$group['OWNER_ID']);
                        if ($mapped > 0) $boxOwner = $mapped;
                    }

                    // Download group image/avatar from cloud
                    if (!empty($group['IMAGE'])) {
                        $imageUrl = $group['IMAGE'];
                        if (!preg_match('/^https?:\/\//', $imageUrl)) {
                            $imageUrl = $this->cloudAPI->getPortalBaseUrl() . $imageUrl;
                        }
                        $photo = BoxD7Service::downloadPhoto($imageUrl);
                        if ($photo) {
                            $fields['IMAGE_ID'] = $photo;
                        }
                    }

                    // D7 creation bypasses REST restriction that ignores OWNER_ID for non-admins
                    $newId = BoxD7Service::createWorkgroup($fields, $boxOwner);
                    if ($newId) {
                        $boxGroupId = $newId;
                        $this->groupMapCache[$cloudId] = $boxGroupId;
                        $created++;
                    }
                } catch (\Throwable $e) {
                    $errors++;
                    $this->addLog("  Ошибка создания группы '{$name}': " . $e->getMessage());
                }
            }

            if ($boxGroupId > 0) {
                $this->addWorkgroupMembers($cloudId, $boxGroupId);
            }
        }

        $this->addLog("Рабочие группы: совпали=$matched, создано=$created, ошибок=$errors из $total");
    }

    /**
     * Fetch cloud group members and add them to the box group via D7.
     * D7 roles: A=owner, E=moderator, K=member.
     * REST sonet_group.user.add ignores ROLE — so we use D7 directly.
     */
    private function addWorkgroupMembers(int $cloudGroupId, int $boxGroupId)
    {
        try {
            $members = $this->cloudAPI->getWorkgroupMembers($cloudGroupId);
            if (empty($members)) return;

            $added = 0;
            foreach ($members as $member) {
                $cloudUserId = (int)($member['USER_ID'] ?? 0);
                if ($cloudUserId <= 0) continue;

                $boxUserId = $this->mapUser($cloudUserId);
                if ($boxUserId <= 0) continue;

                // D7 roles: A=owner, E=moderator, K=member
                $role = $member['ROLE'] ?? \SONET_ROLES_USER;
                if (!in_array($role, [\SONET_ROLES_OWNER, \SONET_ROLES_MODERATOR, \SONET_ROLES_USER], true)) {
                    $role = \SONET_ROLES_USER;
                }

                try {
                    // Owner transfer handled separately
                    if ($role === \SONET_ROLES_OWNER) {
                        BoxD7Service::addWorkgroupMember($boxGroupId, $boxUserId, \SONET_ROLES_OWNER);
                        BoxD7Service::setWorkgroupOwner($boxGroupId, $boxUserId);
                    } else {
                        BoxD7Service::addWorkgroupMember($boxGroupId, $boxUserId, $role);
                    }
                    $added++;
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка добавления участника box#$boxUserId в группу box#$boxGroupId: " . $e->getMessage());
                }
            }

            if ($added > 0) {
                $this->addLog("  Группа box#$boxGroupId: добавлено $added участников");
            }
        } catch (\Throwable $e) {
            $this->addLog("  Ошибка загрузки участников группы cloud#$cloudGroupId: " . $e->getMessage());
        }
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
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка создания смарт-процесса '{$title}': " . $e->getMessage());
                    continue;
                }
            }

            if (!$boxEntityTypeId) continue;

            $this->smartProcessMapCache[$cloudEntityTypeId] = $boxEntityTypeId;

            // Delete existing userfields on box SP if enabled
            $deleteUfSettings = $this->plan['delete_userfields'] ?? [];
            $deleteUfEnabled = ($deleteUfSettings['enabled'] ?? true) === true;
            $skipEntities = $deleteUfSettings['skip_entities'] ?? [];

            if ($deleteUfEnabled && !in_array('smart_' . $spId, $skipEntities)) {
                try {
                    $boxEntityId = 'CRM_' . $boxEntityTypeId;
                    $boxUfFields = $this->boxAPI->getSmartProcessUserfields($boxEntityId);
                    if (!empty($boxUfFields)) {
                        $this->addLog("Удаление UF-полей смарт-процесса '{$title}': " . count($boxUfFields) . " шт.");
                        foreach ($boxUfFields as $uf) {
                            $this->rateLimit();
                            $ufId = (int)($uf['id'] ?? $uf['ID'] ?? 0);
                            if ($ufId) {
                                try {
                                    $this->boxAPI->deleteSmartProcessUserfield($ufId);
                                } catch (\Throwable $e) {
                                    $this->addLog("  Ошибка удаления UF #{$ufId}: " . $e->getMessage());
                                }
                            }
                        }
                    }
                } catch (\Throwable $e) {
                    $this->addLog("  Ошибка получения UF-полей SP '{$title}': " . $e->getMessage());
                }
            }

            try {
                $spFilter = $this->getIncrementalFilter();
                // Smart process items use lowercase 'createdTime' instead of DATE_CREATE
                if (!empty($spFilter['>DATE_CREATE'])) {
                    $spFilter['>createdTime'] = $spFilter['>DATE_CREATE'];
                    unset($spFilter['>DATE_CREATE']);
                }
                $cloudItems = $this->cloudAPI->getSmartProcessItems($cloudEntityTypeId, ['*', 'uf_*'], $spFilter);
                foreach ($cloudItems as $item) {
                    $this->rateLimit();
                    $fields = $this->buildSmartProcessItemFields($item, $spId);

                    try {
                        $newItemId = BoxD7Service::addSmartItem($boxEntityTypeId, $fields);
                        if ($newItemId > 0) {
                            $totalItems++;
                            $cloudItemId = (int)($item['id'] ?? 0);
                            if ($cloudItemId > 0) {
                                $this->smartItemMapCache[$cloudItemId] = $newItemId;
                                $this->smartItemsByType[$boxEntityTypeId][$cloudItemId] = $newItemId;
                            }
                            if (!empty($item['createdTime'])) {
                                try {
                                    $table = 'b_crm_dynamic_items_' . $boxEntityTypeId;
                                    BoxD7Service::backdateEntity($table, $newItemId, $item['createdTime'], $item['updatedTime'] ?? '');
                                } catch (\Throwable $e) { /* date preservation is non-critical */ }
                            }
                        }
                    } catch (\Throwable $e) {
                        $this->addLog("  Ошибка создания записи SP #{$item['id']}: " . $e->getMessage());
                    }
                }
            } catch (\Throwable $e) {
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
            if ($v === '' || $v === null) continue;
            // Match UF fields in any case: ufCrm..., uf_crm..., UF_CRM_...
            if (strncmp($k, 'uf', 2) === 0 || strncmp($k, 'UF', 2) === 0) {
                // Convert camelCase to UPPER_SNAKE_CASE for crm.item.add
                $upperKey = $this->ufFieldToUpperCase($k);
                if (!in_array($k, $excludedFields) && !in_array($upperKey, $excludedFields)) {
                    $fields[$upperKey] = $v;
                }
            }
        }

        return $fields;
    }

    /**
     * Convert camelCase UF field name to UPPER_SNAKE_CASE.
     * e.g. ufCrm5_1234567890 → UF_CRM_5_1234567890
     *      ufCrmBigTask → UF_CRM_BIG_TASK
     *      UF_CRM_FIELD → UF_CRM_FIELD (already uppercase)
     */
    /**
     * Remap STAGE_ID from cloud category to box category.
     * Default pipeline: "NEW" → "NEW" (no prefix, no change needed)
     * Custom pipeline:  "C5:NEW" → "C7:NEW" (replace cloud catId with box catId)
     */
    private function remapStageId($stageId, $cloudCatId, $boxCatId)
    {
        // Use stageMapCache first (populated by syncPipelineStages with correct prefix remapping)
        if (isset($this->stageMapCache[$stageId])) {
            return $this->stageMapCache[$stageId];
        }
        if ($cloudCatId === 0 || $cloudCatId === $boxCatId) {
            return $stageId;
        }
        // Fallback: remap prefix C{cloudCatId}:CODE → C{boxCatId}:CODE
        $prefix = 'C' . $cloudCatId . ':';
        if (strncmp($stageId, $prefix, strlen($prefix)) === 0) {
            return 'C' . $boxCatId . ':' . substr($stageId, strlen($prefix));
        }
        return $stageId;
    }

    private function ufFieldToUpperCase($fieldName)
    {
        // Already uppercase
        if (strtoupper($fieldName) === $fieldName) {
            return $fieldName;
        }

        // Insert underscore before uppercase letters, then uppercase all
        $result = preg_replace('/([a-z\d])([A-Z])/', '$1_$2', $fieldName);
        return strtoupper($result);
    }

    // =========================================================================
    // Phase 12: Tasks (delegate to TaskMigrationService)
    // =========================================================================

    private function migrateTasks()
    {
        $taskService = new TaskMigrationService($this->cloudAPI, $this->boxAPI, $this->plan);
        $taskService->setUserMapCache($this->userMapCache);
        $taskService->setGroupMapCache($this->groupMapCache);
        $taskService->setCrmMapCaches(
            $this->companyMapCache,
            $this->contactMapCache,
            $this->dealMapCache,
            $this->leadMapCache
        );
        if ($this->logFile) {
            $taskService->setLogFile($this->logFile);
        }
        if ($this->migrationFolderId > 0) {
            $taskService->setMigrationFolderId($this->migrationFolderId);
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
                } catch (\Throwable $e) {}
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
    // Disk file operations
    // =========================================================================

    /**
     * Download file from cloud URL → save to temp → upload to box disk via D7.
     * Returns box disk file ID or 0 on failure.
     */
    private function downloadCloudFileToBox(string $downloadUrl, string $fileName): int
    {
        if ($this->migrationFolderId <= 0) return 0;

        $tmpPath = tempnam(sys_get_temp_dir(), 'bx_mig_');
        try {
            $ch = curl_init($downloadUrl);
            $fp = fopen($tmpPath, 'wb');
            curl_setopt_array($ch, [
                CURLOPT_FILE           => $fp,
                CURLOPT_TIMEOUT        => 120,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_SSL_VERIFYPEER => false,
            ]);
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);
            fclose($fp);

            if ($httpCode !== 200 || filesize($tmpPath) === 0) {
                return 0;
            }

            return BoxD7Service::uploadFileToFolder($this->migrationFolderId, $tmpPath, $fileName) ?? 0;
        } finally {
            @unlink($tmpPath);
        }
    }

    /**
     * Migrate STORAGE_ELEMENT_IDS from cloud activity to box disk files.
     * Returns array of box disk file IDs.
     */
    private function migrateActivityFiles(array $cloudStorageIds): array
    {
        $boxFileIds = [];
        foreach ($cloudStorageIds as $cloudFileId) {
            try {
                $fileInfo = $this->cloudAPI->getDiskFile((int)$cloudFileId);
                $this->rateLimit();
                $downloadUrl = $fileInfo['DOWNLOAD_URL'] ?? '';
                $fileName = $fileInfo['NAME'] ?? ('activity_file_' . $cloudFileId);
                if (empty($downloadUrl)) continue;

                $boxId = $this->downloadCloudFileToBox($downloadUrl, $fileName);
                if ($boxId > 0) {
                    $boxFileIds[] = $boxId;
                }
            } catch (\Throwable $e) {
                // skip file, non-critical
            }
        }
        return $boxFileIds;
    }

    // =========================================================================
    // Rate limiting & progress
    // =========================================================================

    /**
     * Rate limit for REST API calls. sleep(0.33) between requests.
     * Call only before REST operations — D7 ops don't need pausing.
     */
    private function rateLimit()
    {
        $this->opCount++;
        usleep(333000); // 333ms (~3 req/s)
        // Periodic memory cleanup every 50 operations — Bitrix cache engine leaks
        if ($this->opCount % 50 === 0) {
            gc_collect_cycles();
            try {
                $app = \Bitrix\Main\Application::getInstance();
                $app->getManagedCache()->cleanAll();
                $app->getTaggedCache()->clearByTag('*');
            } catch (\Throwable $e) {}
        }
        $this->checkStop();
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

    /**
     * Write a detailed error entry ONLY to the errors file (not to the main UI buffer).
     * Use this for high-volume errors (e.g. timeline) to avoid polluting the 500-line UI log.
     */
    private function addErrorDetail(string $context, string $entityInfo, string $errorMsg): void
    {
        if (!$this->errorLogFile) return;
        $line = date('Y-m-d H:i:s') . " [ERROR] [$context] $entityInfo: $errorMsg\n";
        @file_put_contents($this->errorLogFile, $line, FILE_APPEND);
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
