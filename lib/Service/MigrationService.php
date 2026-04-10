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
    private $rootDeptId = 0;         // box root department ID (explicitly stored)
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
    private $invoiceMapCache = [];       // cloud SmartInvoice ID => box SmartInvoice ID

    // Scoped (test) migration support.
    // $scopeMode: null — full migration; 'company' | 'contact' | 'task' — scoped.
    // $scopeRootIds: user-selected cloud IDs (the entry point).
    // $scopedIds: whitelist of cloud IDs per logical group, populated in initScope().
    private $scopeMode = null;
    private $scopeRootIds = [];
    private $scopedIds = [
        'company'  => [],   // int[] of cloud company IDs
        'contact'  => [],
        'lead'     => [],
        'deal'     => [],
        'invoice'  => [],
        'task'     => [],
    ];

    // UF field schemas + enum mappings, populated during migrateCrmFields()
    // Structure: $ufFieldSchema[entityType][fieldName] = ['type' => 'enumeration|file|date|...', 'multiple' => 'Y'|'N']
    //            $ufEnumMap[entityType][fieldName][cloudEnumId] = boxEnumId
    private $ufFieldSchema = [];
    private $ufEnumMap = [];

    private $log = [];
    private $opCount = 0;
    private $saveMapEnabled = true;  // persist created IDs to MigratorMap HL block (always on — required for cross-run linking)
    private $migrationFolderId = 0;  // disk folder for file uploads
    public $batchRespawnRequested = false; // set by any phase when batch size reached (public for closures via $self)

    /**
     * Per-process batch size. When a phase creates this many entities in the
     * current PHP process, it signals respawn so the OS can reclaim memory
     * held by Bitrix's eval'd ORM classes (unclearable mid-process).
     */
    private function getBatchSize(): int
    {
        return max(100, (int)($this->plan['settings']['batch_size'] ?? 2000));
    }

    /**
     * Mark current phase for respawn. Phase method exits after saveStats,
     * migrate() loop breaks out, install/cli/migrate.php respawns a fresh
     * worker that skips done phases and resumes via phase_cursor_*.
     */
    public function requestBatchRespawn(string $phase, int $batchCreated): void
    {
        $this->batchRespawnRequested = true;
        Option::set($this->moduleId, 'migration_respawn', '1');
        $this->addLog("[$phase] обработан batch ($batchCreated). Респаун воркера...");
    }

    public function getPhaseResumeCursor(string $phase): int
    {
        return (int)Option::get($this->moduleId, 'phase_cursor_' . $phase, '0');
    }

    /** Public so closures using $self (a copy of $this) can call it. */
    public function setPhaseResumeCursor(string $phase, $cursor): void
    {
        Option::set($this->moduleId, 'phase_cursor_' . $phase, (string)(int)$cursor);
    }

    /**
     * Release memory accumulated by Bitrix CRM Add operations.
     * Called inside per-item CRM creation loops every ~50 items.
     * Public so closures via $self can call it.
     */
    public function freeMemory(): void
    {
        $this->saveLog();
        // flushCrmCaches() already calls USER_FIELD_MANAGER->CleanCache()
        // and ManagedCache::cleanAll(). Do NOT call clearByTag('*') —
        // it wipes ServiceLocator bindings and triggers ORM class re-eval
        // fatal "Cannot declare class MediatorFromNodeMemberToRoleViaRoleTable".
        BoxD7Service::flushCrmCaches();
        if (class_exists('\CCrmSearch', false) && method_exists('\CCrmSearch', 'ClearResultCache')) {
            try { \CCrmSearch::ClearResultCache(); } catch (\Throwable $e) {}
        }
        gc_collect_cycles();
    }

    /**
     * Scoped (test) migration — the user picks ONE company, contact, or task,
     * and the migrator processes only that entity and its direct children.
     * Called once from migrate() before the phase loop.
     */
    private function initScope(): void
    {
        $scope = $this->plan['scope'] ?? null;
        if (empty($scope['entity_type']) || empty($scope['entity_ids'])) {
            return;
        }
        $this->scopeMode = (string)$scope['entity_type'];
        $this->scopeRootIds = array_values(array_filter(array_map('intval', (array)$scope['entity_ids'])));
        if (empty($this->scopeRootIds)) {
            $this->scopeMode = null;
            return;
        }

        $this->addLog("=== Тестовый прогон: scope={$this->scopeMode}, ids=" . implode(',', $this->scopeRootIds) . " ===");

        if ($this->scopeMode === 'task') {
            // Task mode: only the selected tasks. No CRM phases.
            $this->scopedIds['task'] = $this->scopeRootIds;
            return;
        }

        if ($this->scopeMode === 'company') {
            $this->scopedIds['company'] = $this->scopeRootIds;
            foreach ($this->scopeRootIds as $companyId) {
                // Contacts linked via crm.company.contact.items.get (M:N)
                try {
                    $items = $this->cloudAPI->request('crm.company.contact.items.get', ['id' => $companyId]);
                    foreach (($items['result'] ?? []) as $it) {
                        if (!empty($it['CONTACT_ID'])) {
                            $this->scopedIds['contact'][] = (int)$it['CONTACT_ID'];
                        }
                    }
                } catch (\Throwable $e) { /* no contacts */ }

                // Deals with COMPANY_ID = root, plus M:N via crm.company.deal.items.get if supported
                try {
                    $deals = $this->cloudAPI->request('crm.deal.list', [
                        'filter' => ['COMPANY_ID' => $companyId],
                        'select' => ['ID'],
                    ]);
                    foreach (($deals['result'] ?? []) as $d) {
                        $this->scopedIds['deal'][] = (int)$d['ID'];
                    }
                } catch (\Throwable $e) {}

                // Leads
                try {
                    $leads = $this->cloudAPI->request('crm.lead.list', [
                        'filter' => ['COMPANY_ID' => $companyId],
                        'select' => ['ID'],
                    ]);
                    foreach (($leads['result'] ?? []) as $l) {
                        $this->scopedIds['lead'][] = (int)$l['ID'];
                    }
                } catch (\Throwable $e) {}

                // Invoices (SmartInvoice entityTypeId=31) — by companyId and parentId4
                foreach (['companyId' => $companyId, 'parentId4' => $companyId] as $key => $val) {
                    try {
                        $invs = $this->cloudAPI->request('crm.item.list', [
                            'entityTypeId' => 31,
                            'filter' => [$key => $val],
                            'select' => ['id'],
                        ]);
                        foreach (($invs['result']['items'] ?? []) as $i) {
                            $this->scopedIds['invoice'][] = (int)$i['id'];
                        }
                    } catch (\Throwable $e) {}
                }
            }
        } elseif ($this->scopeMode === 'contact') {
            $this->scopedIds['contact'] = $this->scopeRootIds;
            foreach ($this->scopeRootIds as $contactId) {
                // Companies linked via crm.contact.company.items.get
                try {
                    $items = $this->cloudAPI->request('crm.contact.company.items.get', ['id' => $contactId]);
                    foreach (($items['result'] ?? []) as $it) {
                        if (!empty($it['COMPANY_ID'])) {
                            $this->scopedIds['company'][] = (int)$it['COMPANY_ID'];
                        }
                    }
                } catch (\Throwable $e) {}

                // Deals with CONTACT_ID primary
                try {
                    $deals = $this->cloudAPI->request('crm.deal.list', [
                        'filter' => ['CONTACT_ID' => $contactId],
                        'select' => ['ID'],
                    ]);
                    foreach (($deals['result'] ?? []) as $d) {
                        $this->scopedIds['deal'][] = (int)$d['ID'];
                    }
                } catch (\Throwable $e) {}

                // Leads
                try {
                    $leads = $this->cloudAPI->request('crm.lead.list', [
                        'filter' => ['CONTACT_ID' => $contactId],
                        'select' => ['ID'],
                    ]);
                    foreach (($leads['result'] ?? []) as $l) {
                        $this->scopedIds['lead'][] = (int)$l['ID'];
                    }
                } catch (\Throwable $e) {}

                // Invoices by contactId and parentId3
                foreach (['contactId' => $contactId, 'parentId3' => $contactId] as $key => $val) {
                    try {
                        $invs = $this->cloudAPI->request('crm.item.list', [
                            'entityTypeId' => 31,
                            'filter' => [$key => $val],
                            'select' => ['id'],
                        ]);
                        foreach (($invs['result']['items'] ?? []) as $i) {
                            $this->scopedIds['invoice'][] = (int)$i['id'];
                        }
                    } catch (\Throwable $e) {}
                }
            }
        }

        // Dedup all sets
        foreach ($this->scopedIds as $k => $ids) {
            $this->scopedIds[$k] = array_values(array_unique(array_map('intval', $ids)));
        }

        $this->addLog('  Собрано в scope: '
            . 'companies=' . count($this->scopedIds['company'])
            . ', contacts=' . count($this->scopedIds['contact'])
            . ', leads=' . count($this->scopedIds['lead'])
            . ', deals=' . count($this->scopedIds['deal'])
            . ', invoices=' . count($this->scopedIds['invoice']));

        // Force recreate: удаляем ранее созданные на коробке сущности scope
        // и их маппинги, чтобы фазы пересоздали их заново.
        $this->forceRecreateScopedEntities();
    }

    /**
     * For scoped test runs: delete already-migrated box entities and their
     * MigratorMap rows so each CRM phase re-creates them from scratch.
     */
    private function forceRecreateScopedEntities(): void
    {
        $deleters = [
            'deal'    => [BoxD7Service::class, 'deleteDeal'],
            'lead'    => [BoxD7Service::class, 'deleteLead'],
            'contact' => [BoxD7Service::class, 'deleteContact'],
            'company' => [BoxD7Service::class, 'deleteCompany'],
        ];
        $counts = [];
        // deal → lead → contact → company to respect FK order
        foreach (['deal', 'lead', 'contact', 'company'] as $type) {
            $ids = $this->scopedIds[$type] ?? [];
            if (empty($ids)) continue;
            $deleted = 0;
            foreach ($ids as $cloudId) {
                try {
                    $localId = MapService::getLocalId($type, (int)$cloudId);
                    if ($localId) {
                        try { call_user_func($deleters[$type], (int)$localId); } catch (\Throwable $e) {}
                    }
                    MapService::deleteMap($type, (int)$cloudId);
                    $deleted++;
                } catch (\Throwable $e) {}
            }
            if ($deleted > 0) $counts[] = "$type=$deleted";
        }
        if ($counts) {
            $this->addLog('  [force-recreate] удалено: ' . implode(', ', $counts));
        }
    }

    public function isScopedMode(): bool
    {
        return $this->scopeMode !== null;
    }

    /**
     * Check if a cloud entity is in scope for the given phase.
     * Returns true in full-migration mode. Public so closures can call via $self.
     *
     * @param string $entityKey One of: company, contact, lead, deal, invoice, task
     */
    public function inScope(string $entityKey, int $cloudId): bool
    {
        if ($this->scopeMode === null) return true;
        $allowed = $this->scopedIds[$entityKey] ?? [];
        return in_array($cloudId, $allowed, true);
    }

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
        'invoices',
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
        'invoices'        => 'Счета',
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
        // HL-block persistence is always on — it's required for cross-run
        // linking (see CLAUDE.md "Маппинг ID и работа через несколько запусков").
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
            // Detect respawn: if any phase is already done/running in Option,
            // this is a batch-respawn continuation — skip cleanup (otherwise
            // we'd delete the entities created in previous batches).
            $existingPhases = json_decode(Option::get($this->moduleId, 'migration_phases', '{}'), true) ?: [];
            $isRespawn = false;
            foreach ($existingPhases as $st) {
                if (in_array($st, ['done', 'running'], true)) { $isRespawn = true; break; }
            }

            if ($isRespawn) {
                $this->addLog('=== Respawn continuation — пропуск фаз очистки ===');
                // Restore in-memory maps built by earlier phases (persisted to Option).
                $tmp = json_decode(Option::get($this->moduleId, 'pipeline_map_cache', ''), true);
                if (is_array($tmp)) {
                    $this->pipelineMapCache = array_map('intval', $tmp);
                    $this->addLog('Восстановлен pipelineMapCache: ' . count($this->pipelineMapCache));
                }
                $tmp = json_decode(Option::get($this->moduleId, 'stage_map_cache', ''), true);
                if (is_array($tmp)) {
                    $this->stageMapCache = $tmp;
                    $this->addLog('Восстановлен stageMapCache: ' . count($this->stageMapCache));
                }
                $tmp = json_decode(Option::get($this->moduleId, 'uf_field_schema', ''), true);
                if (is_array($tmp)) {
                    $this->ufFieldSchema = $tmp;
                    $this->addLog('Восстановлена UF схема: ' . array_sum(array_map('count', $tmp)) . ' полей');
                }
                $tmp = json_decode(Option::get($this->moduleId, 'uf_enum_map', ''), true);
                if (is_array($tmp)) $this->ufEnumMap = $tmp;

                $tmp = json_decode(Option::get($this->moduleId, 'smart_process_map_cache', ''), true);
                if (is_array($tmp)) {
                    $this->smartProcessMapCache = array_map('intval', $tmp);
                    $this->addLog('Восстановлен smartProcessMapCache: ' . count($this->smartProcessMapCache));
                }
                $tmp = json_decode(Option::get($this->moduleId, 'smart_items_by_type', ''), true);
                if (is_array($tmp)) {
                    $this->smartItemsByType = $tmp;
                    $totalSmart = 0;
                    foreach ($tmp as $m) $totalSmart += count($m);
                    $this->addLog("Восстановлен smartItemsByType: $totalSmart элементов");
                }
            } else {
                // Тестовый (scoped) прогон: не чистим существующие данные на коробке,
                // просто создаём выбранные элементы поверх имеющихся. Очистка опасна —
                // снесёт реальные сделки/контакты, которые не имеют отношения к тесту.
                if (!empty($this->plan['scope']['entity_type'])) {
                    $this->addLog('[scope] Тестовый прогон — cleanup пропущен');
                } else {
                    // --- Step 0a: Delete previously migrated data via HL block (if enabled) ---
                    if (!empty($this->plan['settings']['delete_migrated_data'])) {
                        $this->runHlBlockCleanup();
                    }

                    // --- Step 0b: Run all cleanups first, before any migration ---
                    $this->runAllCleanups();
                }
            }

            // --- Step 0c: Build user map (cloud→box by email) — needed by all phases ---
            $this->buildUserMapCache();

            // --- Step 0c-scope: resolve the scope set (test mode). No-op for full migration. ---
            $this->initScope();

            // --- Step 0d: Create migration folder on shared disk for file uploads ---
            try {
                $this->migrationFolderId = BoxD7Service::getOrCreateMigrationFolder('Миграция с облака');
                $this->addLog('Папка миграции на диске: ID ' . $this->migrationFolderId);
            } catch (\Throwable $e) {
                $this->addLog('ВНИМАНИЕ: не удалось создать папку миграции на диске: ' . $e->getMessage());
            }

            // Scoped-run phase filter. In test mode some phases are
            // globally skipped regardless of plan toggles.
            $skipInScope = [];
            if ($this->isScopedMode()) {
                $skipPrereqs = !empty($this->plan['scope']['skip_prereqs']);
                if ($skipPrereqs) {
                    $skipInScope = array_merge($skipInScope, ['departments','users','crm_fields','pipelines','currencies']);
                }
                // Workgroups are never scoped to a single CRM entity.
                $skipInScope[] = 'workgroups';
                // Task scope = only the tasks phase runs.
                if ($this->scopeMode === 'task') {
                    $skipInScope = array_merge($skipInScope, ['departments','users','crm_fields','pipelines','currencies','companies','contacts','leads','deals','invoices','requisites','smart_processes','timeline']);
                }
            }

            foreach (self::PHASES as $phase) {
                $this->checkStop();

                // On respawn: skip phases already marked done in previous batch.
                $phaseStatus = $existingPhases[$phase] ?? '';
                if ($phaseStatus === 'done') {
                    continue;
                }
                if ($phaseStatus === 'skipped') {
                    continue;
                }

                // Scope skip
                if (in_array($phase, $skipInScope, true)) {
                    $this->addLog("[$phase] Пропущено (scope)");
                    $this->savePhaseStatus($phase, 'skipped');
                    continue;
                }

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

                // Batched respawn: phase hit per-process limit, keep it "running"
                // so next worker resumes via phase_cursor_*. Break loop → migrate.php
                // sees migration_respawn=1 and spawns a fresh worker.
                if ($this->batchRespawnRequested) {
                    $this->addLog("[$phase] Выход для респауна (фаза остаётся в статусе 'running')");
                    return;
                }

                $this->savePhaseStatus($phase, 'done');
                $this->addLog("[$phase] Завершено");

                // Free memory between phases — Bitrix cache engine leaks memory
                BoxD7Service::flushCrmCaches();
                gc_collect_cycles();
                try {
                    $app = \Bitrix\Main\Application::getInstance();
                    $app->getManagedCache()->cleanAll();
                    // NOTE: clearByTag('*') removed — wildcard clear invalidates
            // ServiceLocator bindings for crm.service.container and triggers
            // Entity::compileObjectClass re-declarations ("Cannot declare
            // class MediatorFromNodeMemberToRoleViaRoleTable"). Managed cache
            // cleanAll is enough to release per-entity cached data.
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
        // Periodic memory cleanup every 50 iterations — Bitrix cache engine leaks heavily
        if ($this->checkStopCounter % 50 === 0) {
            BoxD7Service::flushCrmCaches();
            gc_collect_cycles();
            try {
                $app = \Bitrix\Main\Application::getInstance();
                $app->getManagedCache()->cleanAll();
                // NOTE: clearByTag('*') removed — wildcard clear invalidates
            // ServiceLocator bindings for crm.service.container and triggers
            // Entity::compileObjectClass re-declarations ("Cannot declare
            // class MediatorFromNodeMemberToRoleViaRoleTable"). Managed cache
            // cleanAll is enough to release per-entity cached data.
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
            'invoices'        => 'invoices',
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
            $this->rootDeptId = $rootDeptId;

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
                } elseif ($this->rootDeptId > 0) {
                    $fields['UF_DEPARTMENT'] = [$this->rootDeptId];
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
        $this->addLog("Очистка воронок на box (D7)...");
        // Delete non-default deal categories via D7 (CCrmDealCategory::Delete
        // also removes associated stages). ID=0 is default, never delete it.
        $catIds = BoxD7Service::listDealCategoryIds();
        foreach ($catIds as $bcId) {
            if ($bcId === 0) continue;
            try {
                BoxD7Service::deleteDealCategory($bcId);
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка удаления воронки #$bcId: " . $e->getMessage());
            }
        }
        // Delete non-system stages of the default pipeline
        $stageIds = BoxD7Service::listDealStageIds();
        foreach ($stageIds as $sid) {
            try { BoxD7Service::deleteCrmStatus($sid); } catch (\Throwable $e) {}
        }
        $this->addLog("Очистка воронок выполнена (" . count($catIds) . " воронок, " . count($stageIds) . " стадий)");
    }

    private function cleanupTasks()
    {
        $this->addLog("Очистка задач на box (D7, SQL listing)...");
        $ids = BoxD7Service::listTaskIds();
        $total = count($ids);
        $deleted = 0;
        $errors = 0;

        foreach ($ids as $taskId) {
            if ($deleted % 200 === 0) $this->checkStop();
            try {
                BoxD7Service::deleteTask((int)$taskId);
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
        $this->addLog("Очистка смарт-процессов на box (D7)...");
        $types = BoxD7Service::listSmartProcessTypes();
        $itemsDeleted = 0;
        $typesDeleted = 0;
        $typesSkipped = 0;
        foreach ($types as $t) {
            $etId = $t['entity_type_id'];
            if (!$etId) continue;

            // Always clean items — user may want to drop migrated SmartInvoice/
            // SmartDocument records even though we keep the type itself.
            try {
                $itemIds = BoxD7Service::listSmartProcessItemIds($etId);
                foreach ($itemIds as $itemId) {
                    try {
                        if (BoxD7Service::deleteSmartProcessItem($etId, $itemId)) {
                            $itemsDeleted++;
                        }
                    } catch (\Throwable $e) { /* skip individual failures */ }
                }
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка листинга SP #{$etId} '{$t['title']}': " . $e->getMessage());
            }

            // Delete the type itself ONLY for user-created types. System types
            // (BX_SMART_INVOICE=31, BX_SMART_DOCUMENT=36, BX_SMART_B2E_DOC=39)
            // are core Bitrix infrastructure — TypeTable::delete() has no
            // guard and would drop their item tables, breaking the portal.
            if ($t['is_system']) {
                $typesSkipped++;
                $this->addLog("  Системный SP '{$t['title']}' (code={$t['code']}, etId={$etId}) — тип сохранён, items очищены");
                continue;
            }
            try {
                BoxD7Service::deleteSmartProcessType($t['row_id']);
                $typesDeleted++;
            } catch (\Throwable $e) {
                $this->addLog("  Не удалось удалить SP '{$t['title']}': " . $e->getMessage());
            }
        }
        $this->addLog("Очистка смарт-процессов: типов удалено=$typesDeleted (системных пропущено=$typesSkipped), элементов=$itemsDeleted");
    }

    /**
     * Delete all items of a CRM entity type on box via D7 (companies, contacts, deals, leads).
     */
    private function cleanupCrmEntity($entityType)
    {
        $d7Map = [
            'companies' => ['list' => 'listCompanyIds', 'del' => 'deleteCompany'],
            'contacts'  => ['list' => 'listContactIds', 'del' => 'deleteContact'],
            'deals'     => ['list' => 'listDealIds',    'del' => 'deleteDeal'],
            'leads'     => ['list' => 'listLeadIds',    'del' => 'deleteLead'],
        ];

        $cfg = $d7Map[$entityType] ?? null;
        if (!$cfg) return;

        $this->addLog("Очистка $entityType на box (D7, SQL listing)...");
        // Direct SQL listing + D7 delete — no REST, no rate limit.
        $ids = BoxD7Service::{$cfg['list']}();
        $total = count($ids);
        $deleted = 0;
        $errors = 0;
        $firstError = '';
        foreach ($ids as $id) {
            if ($deleted % 200 === 0) $this->checkStop();
            try {
                BoxD7Service::{$cfg['del']}((int)$id);
                $deleted++;
            } catch (\Throwable $e) {
                $errors++;
                if ($errors <= 3) {
                    $firstError .= "  ID={$id}: {$e->getMessage()}\n";
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
                $skippedSystem = 0;
                foreach ($boxFields as $f) {
                    $fId = (int)($f['ID'] ?? 0);
                    $fName = $f['FIELD_NAME'] ?? '';
                    $fXmlId = $f['XML_ID'] ?? '';
                    if (!$fId) continue;
                    // Skip system/protected fields managed by other modules
                    if (BoxD7Service::isProtectedUserfield($fName, $fXmlId)) {
                        $skippedSystem++;
                        continue;
                    }
                    try {
                        BoxD7Service::deleteUserfield($fId);
                        $totalDeleted++;
                    } catch (\Throwable $e) {
                        $this->addLog("  Ошибка удаления поля $fName ($entityType): " . $e->getMessage());
                    }
                }
                if ($skippedSystem > 0) {
                    $this->addLog("  Пропущено системных полей ($entityType): $skippedSystem");
                }
                // Clear userfield cache after deletion so new fields can be created
                BoxD7Service::flushCrmCaches();
                // Re-fetch remaining fields (some may have failed to delete)
                $remainingFields = BoxD7Service::getUserfields($entityType);
                $boxFieldNames = [];
                foreach ($remainingFields as $f) {
                    $boxFieldNames[] = $f['FIELD_NAME'] ?? '';
                }
                $this->addLog("  После удаления осталось полей ($entityType): " . count($boxFieldNames));
            } else {
                $boxFieldNames = [];
                foreach ($boxFields as $f) {
                    $boxFieldNames[] = $f['FIELD_NAME'] ?? '';
                }
            }

            // Known system fields that are auto-created by CRM module on box.
            // Cloud returns them WITHOUT the UF_CRM_ prefix (e.g. UF_LOGO), but box stores
            // them WITH the prefix (UF_CRM_LOGO). These should NEVER be migrated — they're
            // module-managed and attempting to create them always fails with "уже существует".
            $systemCrmFields = [
                'company' => ['UF_LOGO', 'UF_STAMP', 'UF_DIRECTOR_SIGN', 'UF_ACCOUNTANT_SIGN'],
                'contact' => [],
                'deal'    => [],
                'lead'    => [],
            ];
            $systemFieldsForEntity = $systemCrmFields[$entityType] ?? [];

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

                // Skip system fields auto-managed by CRM module
                if (in_array($fieldName, $systemFieldsForEntity, true)) {
                    continue;
                }

                // Check if field already exists on box — match both with and without UF_CRM_ prefix
                // (cloud may return UF_LOGO while box stores it as UF_CRM_LOGO, and vice versa)
                $fieldNameWithPrefix = (strncmp($fieldName, 'UF_CRM_', 7) === 0) ? $fieldName : 'UF_CRM_' . substr($fieldName, 3);
                if (in_array($fieldName, $boxFieldNames) || in_array($fieldNameWithPrefix, $boxFieldNames)) {
                    continue;
                }

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

        // Build UF field schema cache + enum mappings (used by build*Fields to transform values)
        $this->buildUfFieldSchemaCache($entityTypes, $allCloudFields);
    }

    /**
     * Build UF field schema cache + cloud→box enum item mapping for all CRM entity types.
     * Cache is consumed by transformUfValue() during entity migration.
     */
    private function buildUfFieldSchemaCache(array $entityTypes, array $allCloudFields): void
    {
        \Bitrix\Main\Loader::includeModule('main');
        $entityIdMap = [
            'deal'    => 'CRM_DEAL',
            'contact' => 'CRM_CONTACT',
            'company' => 'CRM_COMPANY',
            'lead'    => 'CRM_LEAD',
        ];

        foreach ($entityTypes as $entityType) {
            $boxEntityId = $entityIdMap[$entityType] ?? null;
            if (!$boxEntityId) continue;

            $cloudFieldsByName = [];
            foreach (($allCloudFields[$entityType] ?? []) as $f) {
                $cloudFieldsByName[$f['FIELD_NAME']] = $f;
            }

            // Iterate box UF fields for this entity
            $res = \CUserTypeEntity::GetList([], ['ENTITY_ID' => $boxEntityId]);
            while ($boxField = $res->Fetch()) {
                $fn = $boxField['FIELD_NAME'];
                $type = $boxField['USER_TYPE_ID'];
                $this->ufFieldSchema[$entityType][$fn] = [
                    'type' => $type,
                    'multiple' => ($boxField['MULTIPLE'] ?? 'N') === 'Y',
                ];

                // For enumeration: build cloud_enum_id → box_enum_id mapping by VALUE
                if ($type === 'enumeration' && !empty($cloudFieldsByName[$fn]['LIST'])) {
                    $boxItemsByValue = [];
                    $enumRes = \CUserFieldEnum::GetList([], ['USER_FIELD_ID' => $boxField['ID']]);
                    while ($item = $enumRes->Fetch()) {
                        $key = mb_strtolower(trim($item['VALUE']));
                        if ($key !== '') $boxItemsByValue[$key] = (int)$item['ID'];
                    }
                    foreach ($cloudFieldsByName[$fn]['LIST'] as $cloudItem) {
                        $key = mb_strtolower(trim($cloudItem['VALUE'] ?? ''));
                        if ($key !== '' && isset($boxItemsByValue[$key])) {
                            $this->ufEnumMap[$entityType][$fn][(int)$cloudItem['ID']] = $boxItemsByValue[$key];
                        }
                    }
                }
            }
        }

        $totalEnumFields = 0;
        foreach ($this->ufEnumMap as $entityMap) $totalEnumFields += count($entityMap);
        $this->addLog("UF схема: " . array_sum(array_map('count', $this->ufFieldSchema)) . " полей, enum маппингов: $totalEnumFields");

        // Persist UF schema + enum map so respawned workers restore them
        // (they're built by scanning all CRM fields — expensive to rebuild).
        Option::set($this->moduleId, 'uf_field_schema', json_encode($this->ufFieldSchema));
        Option::set($this->moduleId, 'uf_enum_map', json_encode($this->ufEnumMap));
    }

    /**
     * Transform a single UF value from cloud format to box format based on field type.
     * Returns null if value should be skipped (e.g., unmapped enum, file download failed).
     *
     * @param string $entityType  'deal'|'contact'|'company'|'lead'
     * @param string $fieldName   UF field name (UF_CRM_*)
     * @param mixed  $cloudValue  Raw value from cloud REST response
     * @return mixed|null  Transformed value or null to skip
     */
    private function transformUfValue(string $entityType, string $fieldName, $cloudValue)
    {
        if ($cloudValue === '' || $cloudValue === null) return null;

        $schema = $this->ufFieldSchema[$entityType][$fieldName] ?? null;
        if (!$schema) return $cloudValue; // unknown field, copy as-is

        $type = $schema['type'];
        $isMultiple = $schema['multiple'];

        // Handle multiple values: recursively transform each
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

            case 'file':
                return $this->downloadCrmFileField($value);

            default:
                return $value;
        }
    }

    /**
     * Download a CRM file (either disk_file or legacy CFile) from cloud and return
     * a CFile array suitable for CCrm*::Add. Returns null on failure.
     *
     * Used by:
     * - UF fields of type `file` (through transformSingleUfValue)
     * - Standard fields PHOTO (contact), LOGO (company)
     *
     * Cloud format: int ID or object {id, showUrl, downloadUrl}
     *
     * KNOWN LIMITATION: For CRM legacy file fields (b_file direct, not disk),
     * webhook auth token doesn't work against /bitrix/components/.../show_file.php
     * — it returns HTML login page. See TODO: OAuth Local App in CLAUDE.md.
     */
    private function downloadCrmFileField($value): ?array
    {
        $fileId = is_array($value) ? (int)($value['id'] ?? 0) : (int)$value;
        $relUrl = is_array($value) ? ($value['downloadUrl'] ?? '') : '';
        if ($fileId <= 0) return null;

        // Try Disk first (works for disk_file UFs and Disk-stored CRM files)
        try {
            $diskInfo = $this->cloudAPI->getDiskFile($fileId);
            $dlUrl = $diskInfo['DOWNLOAD_URL'] ?? '';
            if (!empty($dlUrl)) {
                $tmp = $this->downloadCrmFileToTemp($dlUrl);
                if ($tmp) {
                    $arr = \CFile::MakeFileArray($tmp);
                    if (!empty($diskInfo['NAME'])) $arr['name'] = $diskInfo['NAME'];
                    return $arr;
                }
            }
        } catch (\Throwable $e) { /* not in disk — try CRM file storage */ }

        // Fall back to CRM file storage URL with webhook auth
        // (will fail with HTML login page for webhook — OAuth required)
        if (!empty($relUrl)) {
            $absUrl = $this->cloudAPI->getPortalBaseUrl() . $relUrl;
            $absUrl = $this->cloudAPI->authorizeUrl($absUrl);
            $tmp = $this->downloadCrmFileToTemp($absUrl);
            if ($tmp) {
                return \CFile::MakeFileArray($tmp);
            }
            $this->addLog("  Файл #$fileId не скачан (webhook не авторизован для component endpoint, нужен OAuth)");
        }
        return null;
    }

    /**
     * Download a CRM file to a temp path and return the path. Caller must unlink.
     */
    private function downloadCrmFileToTemp(string $url): ?string
    {
        $tmp = tempnam(sys_get_temp_dir(), 'bx_uf_');
        $ch = curl_init($url);
        $fp = fopen($tmp, 'wb');
        curl_setopt_array($ch, [
            CURLOPT_FILE => $fp,
            CURLOPT_TIMEOUT => 120,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_SSL_VERIFYPEER => false,
        ]);
        curl_exec($ch);
        $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $contentType = curl_getinfo($ch, CURLINFO_CONTENT_TYPE);
        curl_close($ch);
        fclose($fp);

        if ($code !== 200 || filesize($tmp) === 0) {
            @unlink($tmp);
            return null;
        }

        // Detect HTML login page (happens when cloud session auth is required but webhook
        // token isn't accepted — e.g. CRM standard file fields PHOTO/LOGO, UF type=file).
        // The response is HTTP 200 but contains the Bitrix login form instead of the file.
        if (stripos((string)$contentType, 'text/html') !== false) {
            $head = @file_get_contents($tmp, false, null, 0, 200);
            if (stripos($head, '<!DOCTYPE') !== false || stripos($head, '<html') !== false) {
                @unlink($tmp);
                return null;
            }
        }

        return $tmp;
    }

    /**
     * Apply transformUfValue to all UF_CRM_* keys in $entity, populate $fields.
     * Used by build*Fields methods.
     */
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

    // =========================================================================
    // Phase 4: Pipelines (Deal Categories + Stages)
    // =========================================================================

    private function migratePipelines()
    {
        $cloudCategories = $this->cloudAPI->getDealCategories();
        $boxCategories = $this->boxAPI->getDealCategories();

        $this->addLog("Воронок в облаке: " . count($cloudCategories) . ", в коробке: " . count($boxCategories));
        foreach ($cloudCategories as $cat) {
            $this->addLog("  Cloud воронка: ID=" . ($cat['ID'] ?? '?') . " NAME='" . ($cat['NAME'] ?? '') . "'");
        }

        $cloudStagesGrouped = $this->cloudAPI->getAllDealStagesGrouped();

        // --- Default pipeline (ID=0) ---
        $this->pipelineMapCache[0] = 0;

        // Rename default pipeline on box to match cloud
        // Cloud crm.dealcategory.list does NOT return ID=0 — use crm.dealcategory.default.get
        $cloudDefaultName = '';
        try {
            $defaultCat = $this->cloudAPI->getDefaultDealCategory();
            if (!empty($defaultCat['NAME'])) {
                $cloudDefaultName = $defaultCat['NAME'];
            }
        } catch (\Throwable $e) {
            $this->addLog("  Не удалось получить имя основной воронки из облака: " . $e->getMessage());
        }
        // Fallback: try crm.dealcategory.list (rare case where ID=0 is included)
        if (empty($cloudDefaultName)) {
            foreach ($cloudCategories as $cat) {
                if ((int)($cat['ID'] ?? -1) === 0 && !empty($cat['NAME'])) {
                    $cloudDefaultName = $cat['NAME'];
                    break;
                }
            }
        }

        $this->addLog("Основная воронка облака: '" . ($cloudDefaultName ?: '(не получено)') . "'");
        if (!empty($cloudDefaultName)) {
            try {
                if (BoxD7Service::setDefaultDealCategoryName($cloudDefaultName)) {
                    $this->addLog("Основная воронка переименована: '{$cloudDefaultName}'");
                } else {
                    $this->addLog("  Не удалось переименовать основную воронку (D7 вернул false)");
                }
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

        // Diagnostic: dump pipeline + stage caches
        $this->addLog('  pipelineMapCache: ' . json_encode($this->pipelineMapCache));
        $this->addLog('  stageMapCache (' . count($this->stageMapCache) . ' entries):');
        foreach ($this->stageMapCache as $cloudId => $boxId) {
            $this->addLog('    ' . $cloudId . ' -> ' . $boxId);
        }

        // Migrate lead statuses (entity STATUS) — lead phase uses these for STATUS_ID
        try {
            $cloudLeadStatuses = $this->cloudAPI->getStatusesByEntityId('STATUS');
            if (!empty($cloudLeadStatuses)) {
                $this->addLog("Статусы лидов в облаке: " . count($cloudLeadStatuses));
                $this->syncPipelineStages('STATUS', 'STATUS', $cloudLeadStatuses);
            }
        } catch (\Throwable $e) {
            $this->addLog("  Ошибка миграции статусов лидов: " . $e->getMessage());
        }

        // Also migrate lead source (SOURCE) and source description (SOURCE_DESCRIPTION)
        foreach (['SOURCE', 'CONTACT_TYPE', 'COMPANY_TYPE', 'INDUSTRY'] as $entId) {
            try {
                $cloudList = $this->cloudAPI->getStatusesByEntityId($entId);
                if (!empty($cloudList)) {
                    $this->syncPipelineStages($entId, $entId, $cloudList);
                }
            } catch (\Throwable $e) { /* non-critical */ }
        }

        // CRITICAL: invalidate CRM status/pipeline caches.
        // Bitrix's StatusTable::getStatusesByEntityId() caches via ManagedCache (TTL=3600)
        // and also via static $statusesCache in memory. Without explicit invalidation, the
        // CCrmDeal::Add stage validation may use STALE data from previous run, causing it
        // to silently reset CATEGORY_ID=0 and STAGE_ID=NEW. This is the root cause of
        // "deals sometimes all fly to default pipeline" on re-runs.
        $this->flushCrmStatusCaches();

        // Persist pipeline/stage maps so respawned workers can restore them
        // (these are NOT stored in HL-map; they live only in memory otherwise).
        Option::set($this->moduleId, 'pipeline_map_cache', json_encode($this->pipelineMapCache));
        Option::set($this->moduleId, 'stage_map_cache', json_encode($this->stageMapCache));
    }

    /**
     * Force invalidation of all CRM status/pipeline caches (static + managed).
     * Required after creating/modifying deal categories and their stages, otherwise
     * CCrmDeal::Add may validate stage_id against stale data and silently downgrade
     * the deal to default category.
     */
    public function flushCrmStatusCaches(): void
    {
        // 1. Clear Bitrix managed cache for b_crm_status table
        try {
            $app = \Bitrix\Main\Application::getInstance();
            $app->getManagedCache()->cleanAll();
        } catch (\Throwable $e) {}

        // 2. Clear static cache in StatusTable via reflection (removeStatusesFromCache is protected)
        try {
            $ref = new \ReflectionClass('\\Bitrix\\Crm\\StatusTable');
            if ($ref->hasProperty('statusesCache')) {
                $prop = $ref->getProperty('statusesCache');
                $prop->setAccessible(true);
                $prop->setValue(null, []);
            }
        } catch (\Throwable $e) {}

        // 3. Clear DealCategory::$all static cache
        try {
            $ref = new \ReflectionClass('\\Bitrix\\Crm\\Category\\DealCategory');
            foreach (['all', 'fieldInfos'] as $propName) {
                if ($ref->hasProperty($propName)) {
                    $prop = $ref->getProperty($propName);
                    $prop->setAccessible(true);
                    $prop->setValue(null, null);
                }
            }
        } catch (\Throwable $e) {}

        // 4. Clear cache_dir files for b_crm_status table explicitly
        try {
            $app = \Bitrix\Main\Application::getInstance();
            $cache = $app->getCache();
            $cache->cleanDir('/b_crm_status');
            $cache->cleanDir('/b_crm_deal');
        } catch (\Throwable $e) {}

        $this->addLog("  CRM status caches invalidated");
    }

    /**
     * Bitrix limits STATUS_ID to 17 characters (b_crm_status.STATUS_ID varchar(17)).
     * For DEAL_STAGE prefixed IDs ("C152:PREPAYMENT_INVOICE" = 23 chars), truncate
     * only the suffix — the "C<catId>:" prefix must be preserved to keep the stage
     * bound to the correct pipeline.
     */
    private static function truncateStatusId(string $statusId): string
    {
        if (strlen($statusId) <= 17) return $statusId;
        if (preg_match('/^(C\d+:)(.+)$/', $statusId, $m)) {
            $maxSuffix = 17 - strlen($m[1]);
            if ($maxSuffix < 1) return substr($statusId, 0, 17);
            return $m[1] . substr($m[2], 0, $maxSuffix);
        }
        return substr($statusId, 0, 17);
    }

    private function syncPipelineStages($cloudEntityId, $boxEntityId, $cloudStages)
    {
        if (empty($cloudStages)) return;

        // Get all current box stages for this entity
        $boxStatuses = $this->boxAPI->getStatuses();
        $existingBoxStages = [];
        foreach ($boxStatuses as $s) {
            if (($s['ENTITY_ID'] ?? '') === $boxEntityId) {
                $existingBoxStages[$s['STATUS_ID']] = $s;
            }
        }

        // Build set of cloud STATUS_IDs (after prefix remap) — these are the FINAL stages
        $boxCatId = str_replace('DEAL_STAGE_', '', $boxEntityId);
        $isDealPipeline = (strpos($boxEntityId, 'DEAL_STAGE') === 0);
        $remap = function ($cloudStatusId) use ($cloudEntityId, $boxEntityId, $boxCatId) {
            if ($cloudEntityId !== $boxEntityId && preg_match('/^C(\d+):(.+)$/', $cloudStatusId, $m)) {
                $result = 'C' . $boxCatId . ':' . $m[2];
            } else {
                $result = $cloudStatusId;
            }
            return self::truncateStatusId($result);
        };

        // STEP 1: Delete existing box stages that don't match any cloud stage
        // Skip system stages (SYSTEM='Y') — Bitrix protects them
        $cloudStatusIds = [];
        foreach ($cloudStages as $stage) {
            $cloudStatusIds[$remap($stage['STATUS_ID'] ?? '')] = true;
        }
        foreach ($existingBoxStages as $sid => $boxStage) {
            if (isset($cloudStatusIds[$sid])) continue; // keep matching
            if (($boxStage['SYSTEM'] ?? 'N') === 'Y') continue; // system stage
            try {
                $this->boxAPI->deleteStatus((int)$boxStage['ID']);
            } catch (\Throwable $e) {
                // Non-critical
            }
        }

        // Re-fetch box stages after delete
        $boxStatuses = $this->boxAPI->getStatuses();
        $existingBoxStages = [];
        foreach ($boxStatuses as $s) {
            if (($s['ENTITY_ID'] ?? '') === $boxEntityId) {
                $existingBoxStages[$s['STATUS_ID']] = $s;
            }
        }

        // STEP 2a: BUMP existing WIN/LOST stages to very high SORT so new process stages
        // (with SORT 100-900) fit BEFORE them. Bitrix requires: process SORT < won/lost SORT.
        $winBumpSort = 10000;
        $lostBumpSort = 20000;
        foreach ($existingBoxStages as $sid => $s) {
            $sem = $s['SEMANTICS'] ?? '';
            if ($sem === 'S' || $sem === 'F') {
                try {
                    $newSort = ($sem === 'S') ? $winBumpSort : $lostBumpSort;
                    $this->boxAPI->updateStatus((int)$s['ID'], ['SORT' => $newSort]);
                    $existingBoxStages[$sid]['SORT'] = $newSort;
                    if ($sem === 'S') $winBumpSort += 10;
                    else $lostBumpSort += 10;
                } catch (\Throwable $e) { /* non-critical */ }
            }
        }

        // STEP 2b: Sort cloud stages by semantic — Bitrix REQUIRES order: process → WON → LOST
        $processStages = [];
        $wonStages = [];
        $lostStages = [];
        foreach ($cloudStages as $stage) {
            $sem = $stage['SEMANTICS'] ?? '';
            if ($sem === 'S') {
                $wonStages[] = $stage;
            } elseif ($sem === 'F') {
                $lostStages[] = $stage;
            } else {
                $processStages[] = $stage;
            }
        }
        $orderedStages = array_merge($processStages, $wonStages, $lostStages);

        // STEP 3: Add/update stages with SORT bucketed by semantic
        $processSort = 100;
        $wonSort = 10000;
        $lostSort = 20000;

        foreach ($orderedStages as $stage) {
            $this->rateLimit();
            $cloudStatusId = $stage['STATUS_ID'] ?? '';
            $boxStatusId = $remap($cloudStatusId);

            $semantic = $stage['SEMANTICS'] ?? '';
            if ($semantic === 'S') {
                $sort = $wonSort;
                $wonSort += 10;
            } elseif ($semantic === 'F') {
                $sort = $lostSort;
                $lostSort += 10;
            } else {
                $sort = $processSort;
                $processSort += 10;
            }

            if (isset($existingBoxStages[$boxStatusId])) {
                // Stage exists — try to update SORT/NAME to match cloud order
                $this->stageMapCache[$cloudStatusId] = $boxStatusId;
                try {
                    $updFields = [
                        'NAME' => $stage['NAME'] ?? $cloudStatusId,
                        'SORT' => $sort,
                        'COLOR' => $stage['COLOR'] ?? '',
                    ];
                    $this->boxAPI->updateStatus((int)$existingBoxStages[$boxStatusId]['ID'], $updFields);
                } catch (\Throwable $e) { /* non-critical */ }
                continue;
            }

            $stageFields = [
                'ENTITY_ID' => $boxEntityId,
                'STATUS_ID' => $boxStatusId,
                'NAME' => $stage['NAME'] ?? $cloudStatusId,
                'SORT' => $sort,
                'COLOR' => $stage['COLOR'] ?? '',
            ];
            // Only set SEMANTICS for deal pipelines, and only if non-empty
            if ($isDealPipeline && !empty($semantic)) {
                $stageFields['SEMANTICS'] = $semantic;
            }

            try {
                $this->boxAPI->addStatus($stageFields);
                $this->stageMapCache[$cloudStatusId] = $boxStatusId;
            } catch (\Throwable $e) {
                // Retry without SEMANTICS if conflict
                try {
                    unset($stageFields['SEMANTICS']);
                    $this->boxAPI->addStatus($stageFields);
                    $this->stageMapCache[$cloudStatusId] = $boxStatusId;
                    if (!empty($semantic)) {
                        $this->addLog("  Стадия '{$stage['NAME']}' создана без SEMANTICS=$semantic");
                    }
                } catch (\Throwable $e2) {
                    $this->addLog("  WARN: Стадия '{$stage['NAME']}' ($boxStatusId) НЕ создана: " . $e2->getMessage());
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
        // Always preload HL-map — needed for resume after respawn.
        $this->loadExistingMappings('company');
        $batchSize = $this->getBatchSize();

        $total = $this->cloudAPI->getCompaniesCount();
        // Cumulative counters: restore from previously-saved stats so respawn
        // continues counting from where the previous worker left off.
        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['companies'] ?? [];
        $created   = (int)($ps['created']   ?? 0);
        $skipped   = (int)($ps['skipped']   ?? 0);
        $updated   = (int)($ps['updated']   ?? 0);
        $errors    = (int)($ps['errors']    ?? 0);
        $processed = (int)($ps['processed'] ?? 0);

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
        // Scope: fetch only whitelisted IDs server-side (cloud supports ID filter).
        if ($this->isScopedMode() && !empty($this->scopedIds['company'])) {
            $params['filter'] = array_merge($params['filter'] ?? [], ['ID' => $this->scopedIds['company']]);
        }

        $self = $this;
        $batchCreated = 0;
        $batchReached = false;
        $resumeCursor = $this->getPhaseResumeCursor('companies');
        if ($resumeCursor > 0) $this->addLog("Компании: возобновление с cloud offset=$resumeCursor");
        $this->cloudAPI->fetchBatched('crm.company.list', $params, function ($batch, $currentStart, $newNext) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, &$batchCreated, &$batchReached, $batchSize, $total, $dupSettings, $matchBy, $dupAction, &$boxCompaniesByTitle) {
        foreach ($batch as $company) {
            if ($batchReached) {
                $self->setPhaseResumeCursor('companies', $currentStart);
                return false;
            }
            // Scope filter (secondary safety — server-side filter already narrows it)
            if (!$self->inScope('company', (int)$company['ID'])) { continue; }
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('companies', $processed, $total);
            if ($processed % 50 === 0) $self->freeMemory();
            $cloudId = (int)$company['ID'];

            // Skip if already migrated (HL-block check)
            if ($this->isAlreadyMigrated('company', $cloudId, 'companyMapCache')) {
                $skipped++;
                continue;
            }

            // crm.company.list does NOT return multifields (PHONE/EMAIL/WEB/IM)
            // → fetch full data via crm.company.get
            try {
                $full = $this->cloudAPI->getCompany($cloudId);
                if (!empty($full)) $company = $full;
            } catch (\Throwable $e) { /* fall back to list data */ }

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
                    $batchCreated++;
                    if ($batchCreated >= $batchSize) $batchReached = true;
                    if (!empty($company['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_company', $newId, $company['DATE_CREATE'], $company['DATE_MODIFY'] ?? '');
                            BoxD7Service::backdateTimelineForEntity(4, $newId, $company['DATE_CREATE']);
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания компании #{$cloudId}: " . $e->getMessage());
            }
        }
        $self->setPhaseResumeCursor('companies', (int)$newNext);
        }, null, $resumeCursor); // end fetchBatched callback

        $this->addLog("Компании: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['companies' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors, 'processed' => $processed]]);
        if ($batchReached) {
            $this->requestBatchRespawn('companies', $batchCreated);
        } else {
            $this->setPhaseResumeCursor('companies', 0);
        }
    }

    /**
     * Convert cloud-style flat multifield arrays (PHONE/EMAIL/WEB/IM) to the
     * FM[] format expected by CCrmContact/Company/Lead::Add().
     *
     * Cloud format: $entity['PHONE'] = [['ID'=>123, 'VALUE'=>'+375...', 'VALUE_TYPE'=>'WORK'], ...]
     * Box FM format: $fields['FM']['PHONE']['n1'] = ['VALUE'=>'+375...', 'VALUE_TYPE'=>'WORK']
     */
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

    private function buildCompanyFields($company)
    {
        $fields = [];
        $copy = ['TITLE', 'COMPANY_TYPE', 'INDUSTRY', 'EMPLOYEES', 'REVENUE', 'CURRENCY_ID',
                 'COMMENTS', 'OPENED', 'IS_MY_COMPANY', 'ADDRESS', 'ADDRESS_2', 'ADDRESS_CITY',
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

        $fm = $this->packMultifields($company);
        if (!empty($fm)) $fields['FM'] = $fm;

        // Download LOGO (standard CRM file field, same format as UF type=file)
        if (!empty($company['LOGO'])) {
            $logoArray = $this->downloadCrmFileField($company['LOGO']);
            if ($logoArray) {
                $fields['LOGO'] = $logoArray;
            }
        }

        $this->copyUfFields('company', $company, $fields);

        return $fields;
    }

    // =========================================================================
    // Phase 7: Contacts
    // =========================================================================

    private function migrateContacts()
    {
        // Always preload HL-map — needed for resume after respawn.
        $this->loadExistingMappings('contact');
        $this->loadExistingMappings('company');
        $batchSize = $this->getBatchSize();

        $total = $this->cloudAPI->getContactsCount();
        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['contacts'] ?? [];
        $created   = (int)($ps['created']   ?? 0);
        $skipped   = (int)($ps['skipped']   ?? 0);
        $updated   = (int)($ps['updated']   ?? 0);
        $errors    = (int)($ps['errors']    ?? 0);
        $processed = (int)($ps['processed'] ?? 0);

        $dupSettings = $this->plan['duplicate_settings']['contacts'] ?? [];
        $matchBy = $dupSettings['match_by'] ?? ['EMAIL'];
        $dupAction = $dupSettings['action'] ?? 'skip';

        $params = ['select' => ['*', 'UF_*', 'PHONE', 'EMAIL']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;
        if ($this->isScopedMode() && !empty($this->scopedIds['contact'])) {
            $params['filter'] = array_merge($params['filter'] ?? [], ['ID' => $this->scopedIds['contact']]);
        }

        $self = $this;
        $batchCreated = 0;
        $batchReached = false;
        $resumeCursor = $this->getPhaseResumeCursor('contacts');
        if ($resumeCursor > 0) $this->addLog("Контакты: возобновление с cloud offset=$resumeCursor");
        $this->cloudAPI->fetchBatched('crm.contact.list', $params, function ($batch, $currentStart, $newNext) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, &$batchCreated, &$batchReached, $batchSize, $total, $dupSettings, $matchBy, $dupAction) {
        foreach ($batch as $contact) {
            if ($batchReached) {
                $self->setPhaseResumeCursor('contacts', $currentStart);
                return false;
            }
            if (!$self->inScope('contact', (int)$contact['ID'])) { continue; }
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('contacts', $processed, $total);
            if ($processed % 50 === 0) $self->freeMemory();
            $cloudId = (int)$contact['ID'];

            if ($self->isAlreadyMigrated('contact', $cloudId, 'contactMapCache')) {
                $skipped++;
                continue;
            }

            // crm.contact.list does NOT return multifields (PHONE/EMAIL/WEB/IM)
            // → fetch full data via crm.contact.get
            try {
                $full = $self->cloudAPI->getContact($cloudId);
                if (!empty($full)) $contact = $full;
            } catch (\Throwable $e) { /* fall back to list data */ }

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
                    $batchCreated++;
                    if ($batchCreated >= $batchSize) $batchReached = true;
                    if (!empty($contact['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_contact', $newId, $contact['DATE_CREATE'], $contact['DATE_MODIFY'] ?? '');
                            BoxD7Service::backdateTimelineForEntity(3, $newId, $contact['DATE_CREATE']);
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }

                    $self->linkContactCompanies($cloudId, $newId, $contact);
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания контакта #{$cloudId}: " . $e->getMessage());
            }
        }
        $self->setPhaseResumeCursor('contacts', (int)$newNext);
        }, null, $resumeCursor); // end fetchBatched callback

        $this->addLog("Контакты: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['contacts' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors, 'processed' => $processed]]);
        if ($batchReached) {
            $this->requestBatchRespawn('contacts', $batchCreated);
        } else {
            $this->setPhaseResumeCursor('contacts', 0);
        }
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

        $fm = $this->packMultifields($contact);
        if (!empty($fm)) $fields['FM'] = $fm;

        // Download PHOTO (standard CRM file field, same format as UF type=file)
        if (!empty($contact['PHOTO'])) {
            $photoArray = $this->downloadCrmFileField($contact['PHOTO']);
            if ($photoArray) {
                $fields['PHOTO'] = $photoArray;
            }
        }

        $this->copyUfFields('contact', $contact, $fields);

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
    // Phase 9: Deals
    // =========================================================================

    private function migrateDeals()
    {
        // Always preload HL-map mappings — needed for resume and for foreign
        // keys (company/contact/lead IDs referenced by deals).
        $this->loadExistingMappings('deal');
        $this->loadExistingMappings('company');
        $this->loadExistingMappings('contact');
        $this->loadExistingMappings('lead');
        $batchSize = $this->getBatchSize();

        $total = $this->cloudAPI->getDealsCount();
        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['deals'] ?? [];
        $created   = (int)($ps['created']   ?? 0);
        $skipped   = (int)($ps['skipped']   ?? 0);
        $updated   = (int)($ps['updated']   ?? 0);
        $errors    = (int)($ps['errors']    ?? 0);
        $processed = (int)($ps['processed'] ?? 0);

        // Контроль дублей для сделок отключён в UI — см. buildPlanSection.
        $dupSettings = [];
        $matchBy = [];
        $dupAction = 'create';

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

        // order=ID ASC — обязательно для устойчивой start-based пагинации между respawn.
        // Без этого Bitrix по умолчанию отдаёт ID DESC → свежие сделки в облаке смещают
        // курсор между процессами и первые (самые большие) воронки теряют/дублируют записи.
        $params = ['select' => ['*', 'UF_*'], 'order' => ['ID' => 'ASC']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;
        if ($this->isScopedMode() && !empty($this->scopedIds['deal'])) {
            $params['filter'] = array_merge($params['filter'] ?? [], ['ID' => $this->scopedIds['deal']]);
        }

        $self = $this;
        $batchCreated = 0;
        $batchReached = false;
        $resumeCursor = $this->getPhaseResumeCursor('deals');
        if ($resumeCursor > 0) $this->addLog("Сделки: возобновление с cloud offset=$resumeCursor");
        $this->cloudAPI->fetchBatched('crm.deal.list', $params, function ($batch, $currentStart, $newNext) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, &$batchCreated, &$batchReached, $batchSize, $total, $dupSettings, $matchBy, $dupAction, &$boxDealsByTitle, $boxCurrencies) {
        foreach ($batch as $deal) {
            if ($batchReached) {
                $self->setPhaseResumeCursor('deals', $currentStart);
                return false;
            }
            if (!$self->inScope('deal', (int)$deal['ID'])) { continue; }
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('deals', $processed, $total);
            if ($processed % 50 === 0) $self->freeMemory();
            if ($processed % 200 === 0) {
                $self->addLog('  Сделки ' . $processed . '/' . $total
                    . ', память: ' . round(memory_get_usage(true) / 1024 / 1024) . 'MB');
            }
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
                $sentCatId = $fields['CATEGORY_ID'] ?? 0;
                $sentStageId = $fields['STAGE_ID'] ?? '';

                // PRE-CREATE CHECK: verify pipeline actually exists in DB and has the stage
                if ($sentCatId > 0) {
                    try {
                        $conn = \Bitrix\Main\Application::getConnection();
                        $catRow = $conn->query("SELECT ID FROM b_crm_deal_category WHERE ID = " . (int)$sentCatId)->fetch();
                        $stageCount = 0;
                        if ($sentStageId) {
                            $stageEntityId = 'DEAL_STAGE_' . (int)$sentCatId;
                            $stageRow = $conn->query("SELECT COUNT(*) AS cnt FROM b_crm_status WHERE ENTITY_ID = '" . $conn->getSqlHelper()->forSql($stageEntityId) . "' AND STATUS_ID = '" . $conn->getSqlHelper()->forSql($sentStageId) . "'")->fetch();
                            $stageCount = (int)($stageRow['cnt'] ?? 0);
                        }
                        if (!$catRow || $stageCount === 0) {
                            $self->addLog('    PRE-CHECK: cat#' . $sentCatId . ' exists=' . ($catRow ? 'YES' : 'NO') . ', stage "' . $sentStageId . '" exists=' . $stageCount);
                            // Invalidate caches and retry verification
                            $self->flushCrmStatusCaches();
                        }
                    } catch (\Throwable $e) { /* diagnostic only */ }
                }

                $newId = BoxD7Service::createDeal($fields);
                if ($newId) {
                    $self->dealMapCache[$cloudId] = $newId;
                    $self->saveToMap('deal', $cloudId, $newId);
                    $created++;
                    $batchCreated++;
                    if ($batchCreated >= $batchSize) $batchReached = true;

                    // Verify: fetch the deal from DB and check what Bitrix actually stored
                    try {
                        $conn = \Bitrix\Main\Application::getConnection();
                        $row = $conn->query("SELECT CATEGORY_ID, STAGE_ID FROM b_crm_deal WHERE ID = " . (int)$newId)->fetch();
                        if ($row) {
                            $actualCat = (int)$row['CATEGORY_ID'];
                            $actualStage = $row['STAGE_ID'];
                            if ($actualCat !== (int)$sentCatId || $actualStage !== $sentStageId) {
                                $self->addLog('    OVERRIDE: box deal #' . $newId . ' cat=' . $actualCat . ' (sent ' . $sentCatId . '), stage=' . $actualStage . ' (sent ' . $sentStageId . ')');
                                // Attempt to FIX by direct SQL UPDATE (bypasses CCrmDeal::Update validation)
                                $conn->queryExecute("UPDATE b_crm_deal SET CATEGORY_ID = " . (int)$sentCatId
                                    . ", STAGE_ID = '" . $conn->getSqlHelper()->forSql($sentStageId) . "'"
                                    . " WHERE ID = " . (int)$newId);
                                $self->addLog('    FIXED: SQL UPDATE cat=' . $sentCatId . ' stage=' . $sentStageId);
                            }
                        }
                    } catch (\Throwable $e) { /* diagnostic only */ }

                    if (!empty($deal['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_deal', $newId, $deal['DATE_CREATE'], $deal['DATE_MODIFY'] ?? '');
                            BoxD7Service::backdateTimelineForEntity(2, $newId, $deal['DATE_CREATE']);
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }

                    $self->linkDealContacts($cloudId, $newId);
                    $self->linkDealCompanies($cloudId, $newId);
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания сделки #{$cloudId}: " . $e->getMessage());
            }
        }
        $self->setPhaseResumeCursor('deals', (int)$newNext);
        }, null, $resumeCursor); // end fetchBatched callback

        $this->addLog("Сделки: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['deals' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors, 'processed' => $processed]]);
        if ($batchReached) {
            $this->requestBatchRespawn('deals', $batchCreated);
        } else {
            $this->setPhaseResumeCursor('deals', 0);
        }
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
        if (isset($this->pipelineMapCache[$catId])) {
            $boxCatId = $this->pipelineMapCache[$catId];
        } else {
            // Cloud category not mapped to box → log warning, fall back to default
            $this->addLog("  WARN: Сделка #{$deal['ID']} ссылается на воронку $catId которой нет в pipelineMapCache, ставлю CATEGORY_ID=0");
            $boxCatId = 0;
        }
        $fields['CATEGORY_ID'] = $boxCatId;

        $cloudStageId = $deal['STAGE_ID'] ?? '';
        $boxStageId = '';
        if (!empty($cloudStageId)) {
            $boxStageId = $this->remapStageId($cloudStageId, $catId, $boxCatId);
            $fields['STAGE_ID'] = $boxStageId;
        }

        // Diagnostic: log per-deal mapping (cloud cat/stage -> box cat/stage)
        $stageMappedFlag = (isset($this->stageMapCache[$cloudStageId])) ? 'OK' : 'NO_MAP';
        $this->addLog('  Сделка #' . $deal['ID'] . " '" . ($deal['TITLE'] ?? '') . "': cat " . $catId . '->' . $boxCatId
            . ', stage ' . $cloudStageId . '->' . $boxStageId . ' [' . $stageMappedFlag . ']');

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

        $this->copyUfFields('deal', $deal, $fields);

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

    /**
     * Set the primary company on a deal. Bitrix's `crm.deal.list` often omits
     * COMPANY_ID from the default '*' select, so buildDealFields can't rely
     * on it. We fetch the deal→company binding via crm.deal.company.items.get
     * and write the first linked company as the primary COMPANY_ID.
     */
    private function linkDealCompanies($cloudDealId, $boxDealId)
    {
        try {
            $cloudCompanies = $this->cloudAPI->getDealCompanyItems($cloudDealId);
            if (empty($cloudCompanies)) return;

            $primaryBoxCompanyId = 0;
            foreach ($cloudCompanies as $item) {
                $cid = (int)($item['COMPANY_ID'] ?? 0);
                if ($cid && isset($this->companyMapCache[$cid])) {
                    $primaryBoxCompanyId = (int)$this->companyMapCache[$cid];
                    break;
                }
            }

            if ($primaryBoxCompanyId > 0) {
                BoxD7Service::updateDeal($boxDealId, ['COMPANY_ID' => $primaryBoxCompanyId]);
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
        $this->loadExistingMappings('lead');
        $this->loadExistingMappings('company');
        $this->loadExistingMappings('contact');
        $batchSize = $this->getBatchSize();

        $total = $this->cloudAPI->getLeadsCount();
        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['leads'] ?? [];
        $created   = (int)($ps['created']   ?? 0);
        $skipped   = (int)($ps['skipped']   ?? 0);
        $updated   = (int)($ps['updated']   ?? 0);
        $errors    = (int)($ps['errors']    ?? 0);
        $processed = (int)($ps['processed'] ?? 0);

        // Контроль дублей для лидов отключён в UI.
        $dupSettings = [];
        $matchBy = [];
        $dupAction = 'create';

        // Reuse box currencies already fetched during deals phase (or fetch now)
        $boxCurrencies = [];
        try {
            $boxCurrencies = $this->boxAPI->getCurrencyCodes();
        } catch (\Throwable $e) {}

        // order=ID ASC — см. комментарий в migrateDeals(), та же проблема с respawn.
        $params = ['select' => ['*', 'UF_*', 'PHONE', 'EMAIL'], 'order' => ['ID' => 'ASC']];
        $filter = $this->getIncrementalFilter();
        if (!empty($filter)) $params['filter'] = $filter;
        if ($this->isScopedMode() && !empty($this->scopedIds['lead'])) {
            $params['filter'] = array_merge($params['filter'] ?? [], ['ID' => $this->scopedIds['lead']]);
        }

        $self = $this;
        $batchCreated = 0;
        $batchReached = false;
        $resumeCursor = $this->getPhaseResumeCursor('leads');
        if ($resumeCursor > 0) $this->addLog("Лиды: возобновление с cloud offset=$resumeCursor");
        $this->cloudAPI->fetchBatched('crm.lead.list', $params, function ($batch, $currentStart, $newNext) use ($self, &$created, &$skipped, &$updated, &$errors, &$processed, &$batchCreated, &$batchReached, $batchSize, $total, $dupSettings, $matchBy, $dupAction, $boxCurrencies) {
        foreach ($batch as $lead) {
            if ($batchReached) {
                $self->setPhaseResumeCursor('leads', $currentStart);
                return false;
            }
            if (!$self->inScope('lead', (int)$lead['ID'])) { continue; }
            $self->checkStop();
            $processed++;
            $self->savePhaseProgress('leads', $processed, $total);
            if ($processed % 50 === 0) $self->freeMemory();
            $cloudId = (int)$lead['ID'];

            if ($self->isAlreadyMigrated('lead', $cloudId, 'leadMapCache')) {
                $skipped++;
                continue;
            }

            // crm.lead.list does NOT return multifields (PHONE/EMAIL/WEB/IM)
            // → fetch full data via crm.lead.get
            try {
                $full = $self->cloudAPI->getLead($cloudId);
                if (!empty($full)) $lead = $full;
            } catch (\Throwable $e) { /* fall back to list data */ }

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
                    $batchCreated++;
                    if ($batchCreated >= $batchSize) $batchReached = true;
                    if (!empty($lead['DATE_CREATE'])) {
                        try {
                            BoxD7Service::backdateEntity('b_crm_lead', $newId, $lead['DATE_CREATE'], $lead['DATE_MODIFY'] ?? '');
                            BoxD7Service::backdateTimelineForEntity(1, $newId, $lead['DATE_CREATE']);
                        } catch (\Throwable $e) { /* date preservation is non-critical */ }
                    }
                }
            } catch (\Throwable $e) {
                $errors++;
                $self->addLog("  Ошибка создания лида #{$cloudId}: " . $e->getMessage());
            }
        }
        $self->setPhaseResumeCursor('leads', (int)$newNext);
        }, null, $resumeCursor); // end fetchBatched callback

        $this->addLog("Лиды: создано=$created, обновлено=$updated, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['leads' => ['total' => $total, 'created' => $created, 'updated' => $updated, 'skipped' => $skipped, 'errors' => $errors, 'processed' => $processed]]);
        if ($batchReached) {
            $this->requestBatchRespawn('leads', $batchCreated);
        } else {
            $this->setPhaseResumeCursor('leads', 0);
        }
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

        $fm = $this->packMultifields($lead);
        if (!empty($fm)) $fields['FM'] = $fm;

        $this->copyUfFields('lead', $lead, $fields);

        return $fields;
    }

    // =========================================================================
    // Phase 10: Invoices (SmartInvoice — smart process with entityTypeId=31)
    // =========================================================================

    /**
     * SmartInvoice is a smart process with a fixed entityTypeId=31, accessed
     * via crm.item.list / crm.item.add (same endpoint as other smart items).
     * Bindings:
     *   companyId, contactId  — direct fields
     *   parentId2 (Deal=2)    — parent deal
     *   ufCrm_SMART_INVOICE_* — user fields
     */
    private function migrateInvoices()
    {
        $entityTypeId = defined('\\CCrmOwnerType::SmartInvoice') ? \CCrmOwnerType::SmartInvoice : 31;

        // Preload all CRM maps — invoices reference companies, contacts, deals, leads.
        $this->loadExistingMappings('invoice');
        $this->loadExistingMappings('company');
        $this->loadExistingMappings('contact');
        $this->loadExistingMappings('deal');
        $this->loadExistingMappings('lead');

        $batchSize = $this->getBatchSize();
        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['invoices'] ?? [];
        $created   = (int)($ps['created']   ?? 0);
        $skipped   = (int)($ps['skipped']   ?? 0);
        $errors    = (int)($ps['errors']    ?? 0);
        $processed = (int)($ps['processed'] ?? 0);

        try {
            $filter = $this->getIncrementalFilter();
            // SmartInvoice uses lowercase createdTime, not DATE_CREATE
            if (!empty($filter['>DATE_CREATE'])) {
                $filter['>createdTime'] = $filter['>DATE_CREATE'];
                unset($filter['>DATE_CREATE']);
            }
            $cloudInvoices = $this->cloudAPI->getSmartProcessItems($entityTypeId, ['*', 'uf_*'], $filter);
        } catch (\Throwable $e) {
            $this->addLog('  Не удалось получить счета из облака: ' . $e->getMessage());
            return;
        }

        $total = count($cloudInvoices);
        $this->addLog("Счета (SmartInvoice): найдено в облаке=$total");

        $batchCreated = 0;
        $batchReached = false;
        $resumeCursor = $this->getPhaseResumeCursor('invoices');
        if ($resumeCursor > 0) $this->addLog("Счета: возобновление, пропуск первых $resumeCursor");

        $globalIdx = 0;
        foreach ($cloudInvoices as $invoice) {
            if ($batchReached) break;
            if ($globalIdx < $resumeCursor) { $globalIdx++; continue; }
            $cloudId = (int)($invoice['id'] ?? 0);
            if (!$cloudId) { $globalIdx++; continue; }
            if (!$this->inScope('invoice', $cloudId)) { $globalIdx++; continue; }
            $this->checkStop();
            $processed++;
            $this->savePhaseProgress('invoices', $processed, $total);
            if ($processed % 50 === 0) $this->freeMemory();

            if (isset($this->invoiceMapCache[$cloudId])) {
                $skipped++;
                $globalIdx++;
                continue;
            }

            try {
                $fields = $this->buildInvoiceFields($invoice);
                $newId = BoxD7Service::addSmartItem($entityTypeId, $fields);
                if ($newId > 0) {
                    $this->invoiceMapCache[$cloudId] = $newId;
                    $this->saveToMap('invoice', $cloudId, $newId);
                    $created++;
                    $batchCreated++;
                    if ($batchCreated >= $batchSize) $batchReached = true;

                    if (!empty($invoice['createdTime'])) {
                        try {
                            $table = 'b_crm_dynamic_items_' . $entityTypeId;
                            BoxD7Service::backdateEntity($table, $newId, $invoice['createdTime'], $invoice['updatedTime'] ?? '');
                        } catch (\Throwable $e) { /* non-critical */ }
                    }
                }
            } catch (\Throwable $e) {
                $errors++;
                $this->addLog("  Ошибка создания счёта #{$cloudId}: " . $e->getMessage());
            }
            $globalIdx++;
        }
        unset($cloudInvoices);

        $this->addLog("Счета: создано=$created, пропущено=$skipped, ошибок=$errors из $total");
        $this->saveStats(['invoices' => ['total' => $total, 'created' => $created, 'skipped' => $skipped, 'errors' => $errors, 'processed' => $processed]]);

        if ($batchReached) {
            $this->setPhaseResumeCursor('invoices', $globalIdx);
            $this->requestBatchRespawn('invoices', $batchCreated);
        } else {
            $this->setPhaseResumeCursor('invoices', 0);
        }
    }

    /**
     * Build SmartInvoice fields for BoxD7Service::addSmartItem (camelCase, like other smart items).
     */
    private function buildInvoiceFields(array $invoice): array
    {
        $fields = [];
        $copy = ['title', 'accountNumber', 'opportunity', 'taxValue', 'currencyId',
                 'stageId', 'categoryId', 'opened', 'begindate', 'closedate',
                 'sourceId', 'sourceDescription', 'comments',
                 'utmSource', 'utmMedium', 'utmCampaign', 'utmContent', 'utmTerm'];
        foreach ($copy as $k) {
            if (isset($invoice[$k]) && $invoice[$k] !== '' && $invoice[$k] !== null) {
                $fields[$k] = $invoice[$k];
            }
        }

        if (!empty($invoice['assignedById'])) {
            $boxUser = $this->mapUser($invoice['assignedById']);
            if ($boxUser) $fields['assignedById'] = $boxUser;
        }
        if (!empty($invoice['createdBy'])) {
            $boxUser = $this->mapUser($invoice['createdBy']);
            if ($boxUser) $fields['createdBy'] = $boxUser;
        }

        // Direct bindings
        if (!empty($invoice['companyId']) && isset($this->companyMapCache[(int)$invoice['companyId']])) {
            $fields['companyId'] = $this->companyMapCache[(int)$invoice['companyId']];
        }
        if (!empty($invoice['contactId']) && isset($this->contactMapCache[(int)$invoice['contactId']])) {
            $fields['contactId'] = $this->contactMapCache[(int)$invoice['contactId']];
        }
        if (!empty($invoice['mycompanyId']) && isset($this->companyMapCache[(int)$invoice['mycompanyId']])) {
            $fields['mycompanyId'] = $this->companyMapCache[(int)$invoice['mycompanyId']];
        }

        // Parent bindings: parentId1=Lead, parentId2=Deal, parentId3=Contact, parentId4=Company
        if (!empty($invoice['parentId1']) && isset($this->leadMapCache[(int)$invoice['parentId1']])) {
            $fields['parentId1'] = $this->leadMapCache[(int)$invoice['parentId1']];
        }
        if (!empty($invoice['parentId2']) && isset($this->dealMapCache[(int)$invoice['parentId2']])) {
            $fields['parentId2'] = $this->dealMapCache[(int)$invoice['parentId2']];
        }
        if (!empty($invoice['parentId3']) && isset($this->contactMapCache[(int)$invoice['parentId3']])) {
            $fields['parentId3'] = $this->contactMapCache[(int)$invoice['parentId3']];
        }
        if (!empty($invoice['parentId4']) && isset($this->companyMapCache[(int)$invoice['parentId4']])) {
            $fields['parentId4'] = $this->companyMapCache[(int)$invoice['parentId4']];
        }

        // Multi-contact binding (crm.item returns contactIds array)
        if (!empty($invoice['contactIds']) && is_array($invoice['contactIds'])) {
            $boxContactIds = [];
            foreach ($invoice['contactIds'] as $cid) {
                if (isset($this->contactMapCache[(int)$cid])) {
                    $boxContactIds[] = $this->contactMapCache[(int)$cid];
                }
            }
            if ($boxContactIds) $fields['contactIds'] = $boxContactIds;
        }

        // UF fields — copy as-is (camelCase → UPPER_SNAKE_CASE).
        // Enum/file/date transformations for smart processes are TODO (see CLAUDE.md).
        foreach ($invoice as $k => $v) {
            if ($v === '' || $v === null) continue;
            if (strncmp($k, 'uf', 2) === 0 || strncmp($k, 'UF', 2) === 0) {
                $upperKey = $this->ufFieldToUpperCase($k);
                $fields[$upperKey] = $v;
            }
        }

        return $fields;
    }

    // =========================================================================
    // Phase 10: Requisites (companies + contacts)
    // =========================================================================

    private function migrateRequisites()
    {
        // entity type IDs: Company=4, Contact=3
        $entityMap = [
            4 => ['name' => 'company', 'cache' => &$this->companyMapCache],
            3 => ['name' => 'contact', 'cache' => &$this->contactMapCache],
        ];

        // Build cloud→box preset mapping (critical — cloud and box may use
        // different preset IDs, especially when country differs: cloud BY vs box RU).
        // Match by (COUNTRY_ID, NAME) with fallback to (NAME) only.
        $presetMap = $this->buildRequisitePresetMap();
        $this->addLog("  Маппинг пресетов реквизитов: " . json_encode($presetMap));

        $totalReq = 0;
        $totalBank = 0;
        $errors = 0;

        foreach ($entityMap as $typeId => $cfg) {
            $cache = $cfg['cache'];
            // В тестовом прогоне обрабатываем только scoped сущности
            if ($this->isScopedMode()) {
                $cache = array_filter($cache, fn($boxId, $cloudId) => $this->inScope($cfg['name'], (int)$cloudId), ARRAY_FILTER_USE_BOTH);
            }
            $this->addLog("Реквизиты: обработка {$cfg['name']} (" . count($cache) . " шт)...");

            foreach ($cache as $cloudEntityId => $boxEntityId) {
                $this->rateLimit();
                try {
                    $requisites = $this->cloudAPI->getRequisites($typeId, $cloudEntityId);
                    foreach ($requisites as $req) {
                        $this->rateLimit();
                        $cloudReqId = (int)$req['ID'];
                        $cloudPresetId = (int)($req['PRESET_ID'] ?? 0);
                        unset($req['ID'], $req['DATE_CREATE'], $req['DATE_MODIFY'],
                              $req['CREATED_BY_ID'], $req['MODIFY_BY_ID']);
                        $req['ENTITY_TYPE_ID'] = $typeId;
                        $req['ENTITY_ID'] = $boxEntityId;

                        // Remap PRESET_ID (or create missing preset on box)
                        if ($cloudPresetId > 0) {
                            if (isset($presetMap[$cloudPresetId])) {
                                $req['PRESET_ID'] = $presetMap[$cloudPresetId];
                            } else {
                                $this->addLog("  WARN: preset #$cloudPresetId не найден в маппинге, будет использован как есть");
                            }
                        }

                        try {
                            $requisiteObj = new \Bitrix\Crm\EntityRequisite();
                            $result = $requisiteObj->add($req);
                            if ($result->isSuccess()) {
                                $boxReqId = $result->getId();
                                $boxPresetId = (int)($req['PRESET_ID'] ?? 0);
                                $totalReq++;

                                // Migrate addresses attached to this requisite (from crm.address.list)
                                // Address ENTITY_TYPE_ID=8 (Requisite), ENTITY_ID=requisite_id
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
                                            $this->addLog("  Ошибка адреса реквизита #{$boxReqId}: " . $e->getMessage());
                                        }
                                    }
                                } catch (\Throwable $e) { /* non-critical */ }

                                // Migrate bank details for this requisite
                                try {
                                    $bankDetails = $this->cloudAPI->getBankDetails($cloudReqId);
                                    foreach ($bankDetails as $bank) {
                                        $this->rateLimit();
                                        unset($bank['ID'], $bank['DATE_CREATE'], $bank['DATE_MODIFY'],
                                              $bank['CREATED_BY_ID'], $bank['MODIFY_BY_ID']);
                                        $bank['ENTITY_ID'] = $boxReqId;
                                        // Required: ENTITY_TYPE_ID=8 (Requisite) — без этого "Не заполнено обязательное поле ID типа сущности"
                                        $bank['ENTITY_TYPE_ID'] = \CCrmOwnerType::Requisite;
                                        try {
                                            $bankObj = new \Bitrix\Crm\EntityBankDetail();
                                            // Required: FIELD_CHECK_OPTIONS[PRESET_ID] — без этого "The presetId must be defined"
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
                                } catch (\Throwable $e) {
                                    // non-critical
                                }
                            } else {
                                $errors++;
                                $this->addLog("  Ошибка реквизита ({$cfg['name']} box#{$boxEntityId}): "
                                    . implode('; ', $result->getErrorMessages()));
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

    /**
     * Build cloud preset ID → box preset ID mapping.
     * Cloud and box may have different preset IDs even for same-named presets,
     * especially when the default country differs (cloud BY vs box RU).
     *
     * Strategy:
     * 1. Match by (COUNTRY_ID, NAME) — exact match
     * 2. If not found, create a new preset on box by copying cloud preset fields
     * 3. Fallback: match by NAME only (any country)
     */
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

        // Load box presets — skip empty duplicates (no FIELDS in SETTINGS)
        // that were created by previous buggy runs of the migrator.
        $conn = \Bitrix\Main\Application::getConnection();
        $boxPresets = $conn->query("SELECT ID, NAME, COUNTRY_ID, ENTITY_TYPE_ID, SETTINGS FROM b_crm_preset")->fetchAll();

        // Index box presets by (country_id + name) and by name
        $byCountryName = [];
        $byName = [];
        foreach ($boxPresets as $p) {
            $name = mb_strtolower(trim($p['NAME'] ?? ''));
            $country = (int)($p['COUNTRY_ID'] ?? 0);
            $id = (int)$p['ID'];

            // Skip presets without field definitions in SETTINGS — these are
            // empty duplicates created by previous buggy runs of the migrator.
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

            // 1. Exact match by country + name
            $key = "$cloudCountry|$cloudName";
            if (isset($byCountryName[$key])) {
                $map[$cloudId] = $byCountryName[$key];
                continue;
            }

            // 2. Fallback: match by name only (existing box preset already has
            //    field definitions in b_crm_preset_field — creating a new one
            //    would produce an empty duplicate without fields).
            if (isset($byName[$cloudName])) {
                $map[$cloudId] = $byName[$cloudName];
                $this->addLog("  Пресет cloud#$cloudId '{$cp['NAME']}' → box#{$byName[$cloudName]} (по имени, country $cloudCountry не совпал)");
                continue;
            }

            // 3. No match at all — create new preset (custom presets absent on box)
            try {
                $presetObj = new \Bitrix\Crm\EntityPreset();
                $newPresetFields = [
                    'NAME' => $cp['NAME'] ?? 'Preset ' . $cloudId,
                    'COUNTRY_ID' => $cloudCountry,
                    'ENTITY_TYPE_ID' => (int)($cp['ENTITY_TYPE_ID'] ?? 8),
                    'ACTIVE' => 'Y',
                    'SORT' => (int)($cp['SORT'] ?? 500),
                    'SETTINGS' => $cp['SETTINGS'] ?? [],
                ];
                $presetResult = $presetObj->add($newPresetFields);
                if ($presetResult->isSuccess()) {
                    $newId = $presetResult->getId();
                    $map[$cloudId] = $newId;
                    $byCountryName[$key] = $newId;
                    $byName[$cloudName] = $newId;
                    $this->addLog("  Создан пресет '{$cp['NAME']}' (country=$cloudCountry) box#$newId");
                } else {
                    $this->addLog("  Ошибка создания пресета '{$cp['NAME']}': " . implode('; ', $presetResult->getErrorMessages()));
                }
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка создания пресета '{$cp['NAME']}': " . $e->getMessage());
            }
        }

        return $map;
    }

    // =========================================================================
    // Phase 13: Timeline (Activities & Comments)
    // =========================================================================

    private function migrateTimeline()
    {
        // Authorize admin (USER ID=1) in CLI — required for FileUserType::checkFields
        // which calls $fileModel->canRead($securityContext) using the current user
        global $USER;
        if (!is_object($USER)) {
            $USER = new \CUser;
        }
        if (!$USER->IsAuthorized()) {
            $USER->Authorize(1);
        }

        // Allow attaching files to timeline comments from any user context
        if (\Bitrix\Main\Loader::includeModule('disk')) {
            \Bitrix\Disk\Uf\FileUserType::setValueForAllowEdit('CRM_TIMELINE', true);
        }

        // CRITICAL: preload HL-map caches. Timeline runs AFTER all CRM phases,
        // and on respawn (fresh PHP process) these caches start EMPTY — the
        // phase would then iterate zero entities and silently finish.
        $this->loadExistingMappings('company');
        $this->loadExistingMappings('contact');
        $this->loadExistingMappings('deal');
        $this->loadExistingMappings('lead');
        $this->addLog('Таймлайн: загружено маппингов — компаний=' . count($this->companyMapCache)
            . ', контактов=' . count($this->contactMapCache)
            . ', сделок=' . count($this->dealMapCache)
            . ', лидов=' . count($this->leadMapCache));

        // CRM entity type IDs: Lead=1, Deal=2, Contact=3, Company=4
        $entityTypes = [
            'company' => ['typeId' => 4, 'cache' => &$this->companyMapCache],
            'contact' => ['typeId' => 3, 'cache' => &$this->contactMapCache],
            'deal'    => ['typeId' => 2, 'cache' => &$this->dealMapCache],
            'lead'    => ['typeId' => 1, 'cache' => &$this->leadMapCache],
        ];

        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['timeline'] ?? [];
        $totalActivities = (int)($ps['activities'] ?? 0);
        $totalComments   = (int)($ps['comments']   ?? 0);
        $errors          = (int)($ps['errors']     ?? 0);

        // Timeline batch limit: per-entity work is much heavier than CRM Add
        // (multiple activities + comments + file downloads each), so use a
        // smaller bucket — 1/4 of normal batchSize.
        $batchSize = max(50, (int)($this->getBatchSize() / 4));
        $resumeCursor = $this->getPhaseResumeCursor('timeline');
        $batchProcessed = 0;
        $batchReached = false;
        if ($resumeCursor > 0) $this->addLog("Таймлайн: возобновление, пропуск первых $resumeCursor сущностей");

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
            if ($batchReached) break;
            $typeId = $config['typeId'];
            $cache = $config['cache'];

            // В тестовом прогоне обрабатываем только сущности из scope
            if ($this->isScopedMode()) {
                $cache = array_filter($cache, fn($boxId, $cloudId) => $this->inScope($entityName, (int)$cloudId), ARRAY_FILTER_USE_BOTH);
            }

            $this->addLog("Таймлайн: обработка {$entityName} (" . count($cache) . " шт)...");

            foreach ($cache as $cloudId => $boxId) {
                if ($batchReached) break;
                // Skip until cursor — only counted, no work done
                if ($processedEntities < $resumeCursor) {
                    $processedEntities++;
                    continue;
                }
                $this->rateLimit();
                $processedEntities++;
                $this->savePhaseProgress('timeline', $processedEntities, $totalEntities);
                if ($batchProcessed > 0 && $batchProcessed % 50 === 0) $this->freeMemory();

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
                                    BoxD7Service::backdateTimelineForActivity((int)$actId, $activity['CREATED']);
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

                        // crm.timeline.comment.list does NOT return FILES
                        // → fetch full data via crm.timeline.comment.get
                        try {
                            $cId = (int)($comment['ID'] ?? 0);
                            if ($cId > 0) {
                                $full = $this->cloudAPI->getTimelineComment($cId);
                                if (!empty($full)) $comment = $full;
                            }
                        } catch (\Throwable $e) { /* fall back to list data */ }

                        // Download files via disk.file.get (auth URL, not urlDownload)
                        $diskFileIds = [];
                        $commentFiles = $comment['FILES'] ?? [];

                        // DIAGNOSTIC: log full comment structure if FILES is empty/missing
                        if (empty($commentFiles)) {
                            $cId = (int)($comment['ID'] ?? 0);
                            $hasFilesFlag = $comment['SETTINGS']['HAS_FILES'] ?? '?';
                            $textPreview = mb_substr($comment['COMMENT'] ?? '', 0, 30);
                            $allKeys = implode(',', array_keys($comment));
                            $this->addLog("  Комментарий #$cId: FILES пустое, HAS_FILES=$hasFilesFlag, текст='$textPreview', ключи: $allKeys");
                        }

                        if (!empty($commentFiles) && is_array($commentFiles)) {
                            $this->addLog("  Комментарий #" . ($comment['ID'] ?? '?') . ": " . count($commentFiles) . " файлов");
                            foreach ($commentFiles as $cf) {
                                $cfId = (int)($cf['id'] ?? $cf['ID'] ?? 0);
                                $cfName = $cf['name'] ?? $cf['NAME'] ?? 'file';
                                if ($cfId <= 0) {
                                    $this->addLog("    Файл без ID: " . json_encode($cf, JSON_UNESCAPED_UNICODE));
                                    continue;
                                }
                                try {
                                    $diskInfo = $this->cloudAPI->getDiskFile($cfId);
                                    $dlUrl = $diskInfo['DOWNLOAD_URL'] ?? '';
                                    if (empty($dlUrl)) {
                                        $this->addLog("    Файл #$cfId '$cfName': нет DOWNLOAD_URL");
                                        continue;
                                    }
                                    $boxDiskId = $this->downloadCloudFileToBox($dlUrl, $cfName);
                                    if ($boxDiskId > 0) {
                                        $diskFileIds[] = $boxDiskId;
                                        $this->addLog("    Файл '$cfName' -> box disk #$boxDiskId");
                                    }
                                } catch (\Throwable $e) {
                                    $this->addLog("    Файл #$cfId '$cfName': " . $e->getMessage());
                                }
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
                                // FILES go through CommentEntry::create -> attachFiles -> $USER_FIELD_MANAGER->Update
                                // Field name is UF_CRM_COMMENT_FILES (CommentController::UF_COMMENT_FILE_NAME)
                                // Requires authorized $USER (set above) for FileUserType::checkFields canRead check
                                $createParams['FILES'] = array_map(fn($id) => 'n' . $id, $diskFileIds);
                                $createParams['SETTINGS'] = ['HAS_FILES' => 'Y'];
                            }
                            $commentId = \Bitrix\Crm\Timeline\CommentEntry::create($createParams);
                            $totalComments++;

                            if ($commentId > 0 && !empty($diskFileIds)) {
                                $this->addLog("    Прикреплено " . count($diskFileIds) . " файлов к комментарию #$commentId");
                            }

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
                $batchProcessed++;
                if ($batchProcessed >= $batchSize) $batchReached = true;
            }
        }

        // Timeline for smart process items
        foreach ($this->smartItemsByType as $boxEntityTypeId => $itemMap) {
            if ($batchReached) break;
            $cloudEntityTypeId = array_search($boxEntityTypeId, $this->smartProcessMapCache);
            if ($cloudEntityTypeId === false) continue;

            // В тестовом прогоне смарт-процессы не в scope — пропускаем
            if ($this->isScopedMode()) continue;

            $this->addLog("Таймлайн: обработка smart_{$boxEntityTypeId} (" . count($itemMap) . " шт)...");

            foreach ($itemMap as $cloudItemId => $boxItemId) {
                if ($batchReached) break;
                if ($processedEntities < $resumeCursor) {
                    $processedEntities++;
                    continue;
                }
                $this->rateLimit();
                $processedEntities++;
                $this->savePhaseProgress('timeline', $processedEntities, $totalEntities);
                if ($batchProcessed > 0 && $batchProcessed % 50 === 0) $this->freeMemory();

                // Migrate activities
                try {
                    $activities = $this->cloudAPI->getActivities($cloudEntityTypeId, $cloudItemId);
                    foreach ($activities as $activity) {
                        if ((int)($activity['TYPE_ID'] ?? 0) === 6) continue;
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
                                    BoxD7Service::backdateTimelineForActivity((int)$actId, $activity['CREATED']);
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
                $batchProcessed++;
                if ($batchProcessed >= $batchSize) $batchReached = true;
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

        if ($batchReached) {
            $this->setPhaseResumeCursor('timeline', $processedEntities);
            $this->requestBatchRespawn('timeline', $batchProcessed);
        } else {
            $this->setPhaseResumeCursor('timeline', 0);
        }
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
    // Phase 12: Workgroups
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
    // Phase 11: Smart Processes (types + items)
    // =========================================================================

    private function migrateSmartProcesses()
    {
        $cloudTypes = $this->cloudAPI->getSmartProcessTypes();
        $boxTypes = $this->boxAPI->getSmartProcessTypes();

        $boxTypeByTitle = [];
        foreach ($boxTypes as $t) {
            $boxTypeByTitle[mb_strtolower(trim($t['title'] ?? ''))] = $t;
        }

        $batchSize = $this->getBatchSize();
        // Flat resume cursor: total items already created across ALL types in
        // previous workers. New worker skips that many items in deterministic
        // type order, then processes from there.
        $resumeCursor = $this->getPhaseResumeCursor('smart_processes');
        $globalIdx = 0;          // counts every item we IT THROUGH (skipped or created)
        $batchCreated = 0;       // items CREATED in this process
        $batchReached = false;
        if ($resumeCursor > 0) $this->addLog("Смарт-процессы: возобновление, пропуск первых $resumeCursor элементов");

        $prev = json_decode(Option::get($this->moduleId, 'migration_stats', '{}'), true) ?: [];
        $ps = $prev['smart_processes'] ?? [];
        $totalTypes = (int)($ps['types']  ?? 0);
        $totalItems = (int)($ps['items']  ?? 0);

        foreach ($cloudTypes as $type) {
            if ($batchReached) break;
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

            // Delete existing userfields on box SP if enabled (only on first
            // visit to this type — don't re-delete after respawn).
            $deleteUfSettings = $this->plan['delete_userfields'] ?? [];
            $deleteUfEnabled = ($deleteUfSettings['enabled'] ?? true) === true;
            $skipEntities = $deleteUfSettings['skip_entities'] ?? [];
            $skipDelUfDueToResume = ($globalIdx < $resumeCursor);

            if ($deleteUfEnabled && !$skipDelUfDueToResume && !in_array('smart_' . $spId, $skipEntities)) {
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
                // Scope filter: narrow smart process items to those bound to
                // the selected company/contact via parentId<N>.
                if ($this->isScopedMode()) {
                    // Dynamic types bind CRM entities via companyId/contactId
                    // (not parentId<N> — that's for smart-process→smart-process parents).
                    if ($this->scopeMode === 'company' && !empty($this->scopedIds['company'])) {
                        $spFilter['companyId'] = $this->scopedIds['company'];
                    } elseif ($this->scopeMode === 'contact' && !empty($this->scopedIds['contact'])) {
                        $spFilter['contactId'] = $this->scopedIds['contact'];
                    } else {
                        // Scope doesn't apply to smart processes in task mode.
                        continue;
                    }
                }
                $cloudItems = $this->cloudAPI->getSmartProcessItems($cloudEntityTypeId, ['*', 'uf_*'], $spFilter);
                foreach ($cloudItems as $item) {
                    // Skip until we reach saved cursor position
                    if ($globalIdx < $resumeCursor) {
                        $globalIdx++;
                        continue;
                    }
                    if ($batchReached) break;

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
                            $batchCreated++;
                            if ($batchCreated % 50 === 0) $this->freeMemory();
                            if ($batchCreated >= $batchSize) {
                                $batchReached = true;
                            }
                        }
                    } catch (\Throwable $e) {
                        $this->addLog("  Ошибка создания записи SP #{$item['id']}: " . $e->getMessage());
                    }
                    $globalIdx++;
                }
                unset($cloudItems);
            } catch (\Throwable $e) {
                $this->addLog("  Ошибка получения записей SP '{$title}': " . $e->getMessage());
            }
        }

        $this->addLog("Смарт-процессы: типов создано=$totalTypes, записей=$totalItems");
        $this->saveStats(['smart_processes' => ['types' => $totalTypes, 'items' => $totalItems]]);

        // Persist smart caches so respawned worker AND timeline phase can use them.
        Option::set($this->moduleId, 'smart_process_map_cache', json_encode($this->smartProcessMapCache));
        Option::set($this->moduleId, 'smart_items_by_type', json_encode($this->smartItemsByType));

        if ($batchReached) {
            $this->setPhaseResumeCursor('smart_processes', $globalIdx);
            $this->requestBatchRespawn('smart_processes', $batchCreated);
        } else {
            $this->setPhaseResumeCursor('smart_processes', 0);
        }
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
            return self::truncateStatusId($stageId);
        }
        // Fallback: remap prefix C{cloudCatId}:CODE → C{boxCatId}:CODE
        $prefix = 'C' . $cloudCatId . ':';
        if (strncmp($stageId, $prefix, strlen($prefix)) === 0) {
            return self::truncateStatusId('C' . $boxCatId . ':' . substr($stageId, strlen($prefix)));
        }
        return self::truncateStatusId($stageId);
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
    // Phase 14: Tasks (delegate to TaskMigrationService)
    // =========================================================================

    private function migrateTasks()
    {
        // On respawn the CRM map caches can be empty — tasks rely on them
        // to resolve UF_CRM_TASK references (CO_<id> → box company ID, etc.).
        $this->loadExistingMappings('company');
        $this->loadExistingMappings('contact');
        $this->loadExistingMappings('deal');
        $this->loadExistingMappings('lead');

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

        // Scope injection
        if ($this->isScopedMode()) {
            if ($this->scopeMode === 'task') {
                // Direct ID list — migrate only the selected task(s).
                $taskService->setTaskIdsOverride($this->scopedIds['task']);
            } else {
                // company/contact scope — narrow by UF_CRM_TASK filter.
                // Values use cloud-side entity IDs because we're filtering on cloud.
                $filter = [];
                foreach ($this->scopedIds['company'] as $id) $filter[] = 'CO_' . $id;
                foreach ($this->scopedIds['contact'] as $id) $filter[] = 'C_'  . $id;
                foreach ($this->scopedIds['deal']    as $id) $filter[] = 'D_'  . $id;
                foreach ($this->scopedIds['lead']    as $id) $filter[] = 'L_'  . $id;
                if (!empty($filter)) {
                    $taskService->setTaskCrmFilter($filter);
                } else {
                    // Scope produced zero CRM entities — nothing to do in tasks phase.
                    $taskService->setTaskIdsOverride([]);
                }
            }
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
        if (empty($downloadUrl)) return 0;

        // Try downloading the URL as-is first (cloud DOWNLOAD_URL has its own embedded token)
        // If that fails with 401, try with ?auth=<webhook_secret> appended
        $tmpPath = tempnam(sys_get_temp_dir(), 'bx_mig_');
        try {
            $download = function ($url) use ($tmpPath) {
                @ftruncate(fopen($tmpPath, 'wb'), 0);
                $ch = curl_init($url);
                $fp = fopen($tmpPath, 'wb');
                curl_setopt_array($ch, [
                    CURLOPT_FILE           => $fp,
                    CURLOPT_TIMEOUT        => 120,
                    CURLOPT_FOLLOWLOCATION => true,
                    CURLOPT_SSL_VERIFYPEER => false,
                ]);
                curl_exec($ch);
                $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                curl_close($ch);
                fclose($fp);
                return $code;
            };

            $httpCode = $download($downloadUrl);
            if ($httpCode !== 200 || filesize($tmpPath) === 0) {
                // Retry with webhook auth token
                $authedUrl = $this->cloudAPI->authorizeUrl($downloadUrl);
                if ($authedUrl !== $downloadUrl) {
                    $httpCode = $download($authedUrl);
                }
            }

            if ($httpCode !== 200 || filesize($tmpPath) === 0) {
                $body = @file_get_contents($tmpPath, false, null, 0, 200);
                $this->addLog("  Файл '$fileName' не скачан: HTTP $httpCode, размер " . filesize($tmpPath) . ", body: " . substr($body, 0, 150));
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
                // NOTE: clearByTag('*') removed — wildcard clear invalidates
            // ServiceLocator bindings for crm.service.container and triggers
            // Entity::compileObjectClass re-declarations ("Cannot declare
            // class MediatorFromNodeMemberToRoleViaRoleTable"). Managed cache
            // cleanAll is enough to release per-entity cached data.
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
