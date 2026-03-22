<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Integration\CloudAPI;
use Bitrix\Main\Config\Option;
use BitrixMigrator\Service\BoxD7Service;

class TaskMigrationService
{
    const MODULE_ID = 'bitrix_migrator';
    const FOLDER_NAME = 'Миграция с облака';

    private $cloudAPI;
    private $boxAPI;
    private $plan;

    // Caches
    private $userMapCache    = []; // cloud email => box user ID
    private $groupMapCache   = []; // cloud group name => box group ID
    private $taskIdMap       = []; // cloud task ID => box task ID
    private $crmEntityCache  = []; // "TYPE_TITLE" => box entity ID

    private $migrationFolderId = 0;
    private $log               = [];
    private $logFile           = null;
    private $stats             = [
        'total'     => 0,
        'migrated'  => 0,
        'skipped'   => 0,
        'errors'    => 0,
        'current'   => 0,
    ];

    public function __construct(CloudAPI $cloudAPI, CloudAPI $boxAPI, array $plan = [])
    {
        $this->cloudAPI = $cloudAPI;
        $this->boxAPI   = $boxAPI;
        $this->plan     = $plan;
    }

    public function setUserMapCache(array $cache)
    {
        $this->userMapCache = $cache;
    }

    public function setGroupMapCache(array $cache)
    {
        $this->groupMapCache = $cache;
    }

    public function setLogFile($path)
    {
        $this->logFile = $path;
    }

    public function setMigrationFolderId(int $id)
    {
        $this->migrationFolderId = $id;
    }

    private function checkStop()
    {
        $flag = Option::get(self::MODULE_ID, 'migration_stop', '0');
        if ($flag === '1') {
            throw new MigrationStoppedException('Остановлено пользователем');
        }
        while ($flag === 'pause') {
            Option::set(self::MODULE_ID, 'migration_status', 'paused');
            Option::set(self::MODULE_ID, 'migration_message', 'Миграция на паузе');
            sleep(3);
            $flag = Option::get(self::MODULE_ID, 'migration_stop', '0');
            if ($flag === '1') {
                throw new MigrationStoppedException('Остановлено пользователем');
            }
        }
    }

    /**
     * Run full task migration
     */
    public function migrate(): array
    {
        $this->saveStatus('running', 'Запуск миграции задач...');

        try {
            // 1. Prepare migration folder on box
            $this->migrationFolderId = $this->getOrCreateMigrationFolder();
            $this->addLog('Папка миграции: ID ' . $this->migrationFolderId);

            // 2. Pre-build user map cache (cloud active users)
            $this->buildUserMapCache();
            $this->addLog('Кэш пользователей: ' . count($this->userMapCache) . ' маппингов');

            // 3. Fetch all task IDs from cloud
            $this->checkStop();
            $this->saveStatus('running', 'Получение списка задач из облака...');
            $taskIds = $this->fetchAllTaskIds();
            $this->stats['total'] = count($taskIds);
            $this->addLog('Найдено задач в облаке: ' . count($taskIds));

            if (empty($taskIds)) {
                $this->saveStatus('completed', 'Нет задач для миграции');
                return $this->getResult();
            }

            // 4. Migrate each task (first pass)
            $this->saveStatus('running', 'Миграция задач: 0/' . $this->stats['total']);
            foreach ($taskIds as $i => $cloudTaskId) {
                $this->checkStop();
                $this->stats['current'] = $i + 1;

                try {
                    $this->migrateTask($cloudTaskId);
                    $this->stats['migrated']++;
                } catch (MigrationStoppedException $e) {
                    throw $e;
                } catch (\Throwable $e) {
                    $this->stats['errors']++;
                    $this->addLog('ОШИБКА задача #' . $cloudTaskId . ': ' . $e->getMessage());
                }

                $this->rateLimitPause();

                // Update progress every 10 tasks
                if (($i + 1) % 10 === 0 || $i === count($taskIds) - 1) {
                    $this->saveStatus('running', 'Миграция задач: ' . ($i + 1) . '/' . $this->stats['total']);
                }
            }

            // 5. Second pass: link parent tasks
            $this->checkStop();
            $this->saveStatus('running', 'Создание связей между задачами...');
            $this->linkParentTasks();

            $this->saveStatus('completed', 'Миграция задач завершена');
            $this->addLog('Готово: ' . $this->stats['migrated'] . ' перенесено, ' . $this->stats['errors'] . ' ошибок');

        } catch (MigrationStoppedException $e) {
            $this->addLog('=== Миграция задач остановлена пользователем ===');
            $this->saveStatus('stopped', 'Миграция задач остановлена');
        } catch (\Throwable $e) {
            $this->saveStatus('error', $e->getMessage());
            $this->addLog('ФАТАЛЬНАЯ ОШИБКА: ' . $e->getMessage() . ' в ' . $e->getFile() . ':' . $e->getLine());
        }

        $this->saveResult();
        return $this->getResult();
    }

    // =========================================================================
    // Single task migration
    // =========================================================================

    private function migrateTask($cloudTaskId)
    {
        // Get full task data from cloud
        $task = $this->cloudAPI->getTask($cloudTaskId);
        if (empty($task)) {
            throw new \Exception('Задача не найдена в облаке');
        }

        $title = $task['title'] ?? $task['TITLE'] ?? '';
        $this->addLog('Задача #' . $cloudTaskId . ': ' . mb_substr($title, 0, 60));

        // Map creator
        $creatorId = $this->mapUser((int)($task['createdBy'] ?? $task['CREATED_BY'] ?? 0));

        // Map responsible
        $responsibleId = $this->mapUser((int)($task['responsibleId'] ?? $task['RESPONSIBLE_ID'] ?? 0));

        // Map group
        $groupId = 0;
        $cloudGroupId = (int)($task['groupId'] ?? $task['GROUP_ID'] ?? 0);
        if ($cloudGroupId > 0) {
            $groupId = $this->mapGroup($cloudGroupId);
        }

        // Handle files
        $fileIds = [];
        $cloudFiles = $task['ufTaskWebdavFiles'] ?? $task['UF_TASK_WEBDAV_FILES'] ?? [];
        if (!empty($cloudFiles) && is_array($cloudFiles)) {
            $fileIds = $this->migrateTaskFiles($cloudFiles);
        }

        // Handle CRM bindings
        $crmBindings = [];
        $cloudCrm = $task['ufCrmTask'] ?? $task['UF_CRM_TASK'] ?? [];
        if (!empty($cloudCrm) && is_array($cloudCrm)) {
            $crmBindings = $this->mapCrmBindings($cloudCrm);
        }

        // Build task fields for box
        $fields = [
            'TITLE'          => $title,
            'DESCRIPTION'    => $task['description'] ?? $task['DESCRIPTION'] ?? '',
            'CREATED_BY'     => $creatorId ?: 1,
            'RESPONSIBLE_ID' => $responsibleId ?: 1,
            'PRIORITY'       => $task['priority'] ?? $task['PRIORITY'] ?? '1',
        ];

        if ($groupId > 0) {
            $fields['GROUP_ID'] = $groupId;
        }

        // Dates
        $deadline = $task['deadline'] ?? $task['DEADLINE'] ?? '';
        if ($deadline) $fields['DEADLINE'] = $deadline;

        $dateStart = $task['dateStart'] ?? $task['DATE_START'] ?? '';
        if ($dateStart) $fields['DATE_START'] = $dateStart;

        $startDatePlan = $task['startDatePlan'] ?? $task['START_DATE_PLAN'] ?? '';
        if ($startDatePlan) $fields['START_DATE_PLAN'] = $startDatePlan;

        $endDatePlan = $task['endDatePlan'] ?? $task['END_DATE_PLAN'] ?? '';
        if ($endDatePlan) $fields['END_DATE_PLAN'] = $endDatePlan;

        // Tags
        $tags = $task['tags'] ?? $task['TAGS'] ?? [];
        if (!empty($tags) && is_array($tags)) {
            $fields['TAGS'] = $tags;
        }

        // Files
        if (!empty($fileIds)) {
            $fields['UF_TASK_WEBDAV_FILES'] = $fileIds;
        }

        // CRM bindings
        if (!empty($crmBindings)) {
            $fields['UF_CRM_TASK'] = $crmBindings;
        }

        // Create task on box
        $boxTask = $this->boxAPI->createTask($fields);
        $boxTaskId = (int)($boxTask['id'] ?? $boxTask['ID'] ?? 0);

        if ($boxTaskId <= 0) {
            throw new \Exception('Не удалось создать задачу на коробке');
        }

        // Store mapping
        $this->taskIdMap[$cloudTaskId] = $boxTaskId;

        // Store parent ID for second pass
        $parentId = (int)($task['parentId'] ?? $task['PARENT_ID'] ?? 0);
        if ($parentId > 0) {
            $this->storeParentLink($cloudTaskId, $parentId);
        }

        // Checklists
        $this->migrateChecklists($cloudTaskId, $boxTaskId);

        // Comments
        $this->migrateComments($cloudTaskId, $boxTaskId);

        // Status
        $status = (int)($task['status'] ?? $task['STATUS'] ?? 0);
        $this->applyTaskStatus($boxTaskId, $status);

        // Accomplices
        $accomplices = $task['accomplices'] ?? $task['ACCOMPLICES'] ?? [];
        if (!empty($accomplices) && is_array($accomplices)) {
            $boxAccomplices = $this->mapUserList($accomplices);
            if (!empty($boxAccomplices)) {
                try {
                    $this->boxAPI->updateTask($boxTaskId, ['ACCOMPLICES' => $boxAccomplices]);
                } catch (\Throwable $e) {
                    $this->addLog('  Соисполнители #' . $cloudTaskId . ': ' . $e->getMessage());
                }
            }
        }

        // Auditors
        $auditors = $task['auditors'] ?? $task['AUDITORS'] ?? [];
        if (!empty($auditors) && is_array($auditors)) {
            $boxAuditors = $this->mapUserList($auditors);
            if (!empty($boxAuditors)) {
                try {
                    $this->boxAPI->updateTask($boxTaskId, ['AUDITORS' => $boxAuditors]);
                } catch (\Throwable $e) {
                    $this->addLog('  Наблюдатели #' . $cloudTaskId . ': ' . $e->getMessage());
                }
            }
        }

        usleep(333000); // rate limit between tasks
    }

    // =========================================================================
    // User mapping
    // =========================================================================

    private function buildUserMapCache()
    {
        $cloudUsers = $this->cloudAPI->getUsers();
        $boxUsers   = $this->boxAPI->getUsers();

        // Build box email => ID map
        $boxEmailMap = [];
        foreach ($boxUsers as $u) {
            $email = strtolower(trim($u['EMAIL'] ?? ''));
            if ($email) {
                $boxEmailMap[$email] = (int)$u['ID'];
            }
        }

        // Build cloud ID => box ID via email
        foreach ($cloudUsers as $u) {
            $email = strtolower(trim($u['EMAIL'] ?? ''));
            $cloudId = (int)$u['ID'];
            if ($email && isset($boxEmailMap[$email])) {
                $this->userMapCache[$cloudId] = $boxEmailMap[$email];
            }
        }
    }

    private function mapUser($cloudUserId)
    {
        if ($cloudUserId <= 0) return 0;
        return $this->userMapCache[$cloudUserId] ?? 0;
    }

    private function mapUserList(array $cloudUserIds)
    {
        $boxIds = [];
        foreach ($cloudUserIds as $cid) {
            $bid = $this->mapUser((int)$cid);
            if ($bid > 0) $boxIds[] = $bid;
        }
        return $boxIds;
    }

    // =========================================================================
    // Group mapping
    // =========================================================================

    private function mapGroup($cloudGroupId)
    {
        if (isset($this->groupMapCache[$cloudGroupId])) {
            return $this->groupMapCache[$cloudGroupId];
        }

        try {
            // Get cloud group name
            $cloudGroups = $this->cloudAPI->getWorkgroups();
            $cloudGroupName = '';
            foreach ($cloudGroups as $g) {
                if ((int)($g['ID'] ?? 0) === $cloudGroupId) {
                    $cloudGroupName = $g['NAME'] ?? '';
                    break;
                }
            }

            if (empty($cloudGroupName)) {
                $this->groupMapCache[$cloudGroupId] = 0;
                return 0;
            }

            // Find on box by name
            $boxGroup = $this->boxAPI->getGroupByName($cloudGroupName);
            $boxGroupId = $boxGroup ? (int)($boxGroup['ID'] ?? 0) : 0;

            $this->groupMapCache[$cloudGroupId] = $boxGroupId;
            if ($boxGroupId > 0) {
                $this->addLog('  Группа "' . $cloudGroupName . '" → box ID ' . $boxGroupId);
            } else {
                $this->addLog('  Группа "' . $cloudGroupName . '" не найдена на коробке');
            }

            return $boxGroupId;
        } catch (\Throwable $e) {
            $this->groupMapCache[$cloudGroupId] = 0;
            return 0;
        }
    }

    // =========================================================================
    // Files
    // =========================================================================

    private function migrateTaskFiles(array $cloudFileRefs): array
    {
        $boxFileIds = [];

        foreach ($cloudFileRefs as $ref) {
            try {
                $attachId = $this->extractAttachId($ref);
                if ($attachId <= 0) continue;

                // Get attached object info from cloud (REST call)
                $attachInfo = $this->cloudAPI->getAttachedObject($attachId);
                usleep(333000);

                $downloadUrl = $attachInfo['DOWNLOAD_URL'] ?? '';
                $fileName    = $attachInfo['NAME'] ?? ('file_' . $attachId);

                if (empty($downloadUrl)) {
                    $this->addLog('  Файл #' . $attachId . ': нет DOWNLOAD_URL');
                    continue;
                }

                // Download to temp file, upload via D7
                $boxFileId = $this->downloadAndUploadFile($downloadUrl, $fileName);

                if ($boxFileId > 0) {
                    $boxFileIds[] = 'n' . $boxFileId;
                    $this->addLog('  Файл "' . $fileName . '" → box disk ID ' . $boxFileId);
                } else {
                    $this->addLog('  Файл "' . $fileName . '": не удалось загрузить на диск');
                }
            } catch (\Throwable $e) {
                $this->addLog('  Файл #' . ($ref ?? '') . ': ' . $e->getMessage());
            }
        }

        return $boxFileIds;
    }

    private function extractAttachId($ref)
    {
        if (is_numeric($ref)) return (int)$ref;
        // Format can be "n123" or just "123"
        if (is_string($ref) && preg_match('/(\d+)/', $ref, $m)) {
            return (int)$m[1];
        }
        return 0;
    }

    // =========================================================================
    // CRM bindings
    // =========================================================================

    private function mapCrmBindings(array $cloudBindings)
    {
        $boxBindings = [];

        foreach ($cloudBindings as $binding) {
            try {
                // Format: "C_123", "D_456", "L_789", "CO_12"
                if (!preg_match('/^(C|D|L|CO)_(\d+)$/', $binding, $m)) continue;

                $entityType = $m[1];
                $cloudId    = (int)$m[2];

                // Get entity title from cloud
                $cloudEntity = $this->cloudAPI->getCrmEntity($entityType, $cloudId);
                if (empty($cloudEntity)) continue;

                $title = $cloudEntity['TITLE'] ?? '';
                if ($entityType === 'CO') {
                    $title = trim(($cloudEntity['NAME'] ?? '') . ' ' . ($cloudEntity['LAST_NAME'] ?? ''));
                }

                if (empty($title)) continue;

                // Check cache
                $cacheKey = $entityType . '_' . $title;
                if (isset($this->crmEntityCache[$cacheKey])) {
                    $boxId = $this->crmEntityCache[$cacheKey];
                } else {
                    // Find on box by title
                    $boxEntity = $this->boxAPI->findCrmEntityByTitle($entityType, $title);
                    $boxId = $boxEntity ? (int)($boxEntity['ID'] ?? 0) : 0;
                    $this->crmEntityCache[$cacheKey] = $boxId;
                }

                if ($boxId > 0) {
                    $boxBindings[] = $entityType . '_' . $boxId;
                }

                usleep(333000);
            } catch (\Throwable $e) {
                $this->addLog('  CRM привязка ' . $binding . ': ' . $e->getMessage());
            }
        }

        return $boxBindings;
    }

    // =========================================================================
    // Checklists
    // =========================================================================

    private function migrateChecklists($cloudTaskId, $boxTaskId)
    {
        try {
            $items = $this->cloudAPI->getTaskChecklists($cloudTaskId);
            if (empty($items) || !is_array($items)) return;

            foreach ($items as $item) {
                $fields = [
                    'TITLE'      => $item['TITLE'] ?? $item['title'] ?? '',
                    'IS_COMPLETE' => ($item['IS_COMPLETE'] ?? $item['isComplete'] ?? 'N') === 'Y' ? 'Y' : 'N',
                ];
                if (empty($fields['TITLE'])) continue;

                try {
                    $this->boxAPI->addTaskChecklistItem($boxTaskId, $fields);
                } catch (\Throwable $e) {
                    // non-critical
                }
                usleep(100000);
            }
        } catch (\Throwable $e) {
            $this->addLog('  Чеклист #' . $cloudTaskId . ': ' . $e->getMessage());
        }
    }

    // =========================================================================
    // Comments
    // =========================================================================

    private function migrateComments($cloudTaskId, $boxTaskId)
    {
        try {
            $comments = $this->cloudAPI->getTaskComments($cloudTaskId);
            if (empty($comments) || !is_array($comments)) return;

            foreach ($comments as $commentInfo) {
                $commentId = (int)($commentInfo['ID'] ?? $commentInfo['id'] ?? (is_numeric($commentInfo) ? $commentInfo : 0));
                if ($commentId <= 0) continue;

                try {
                    $comment = $this->cloudAPI->getTaskComment($cloudTaskId, $commentId);
                    $text = $comment['POST_MESSAGE'] ?? $comment['postMessage'] ?? '';
                    if (empty($text)) continue;

                    // Map author
                    $authorId = $this->mapUser((int)($comment['AUTHOR_ID'] ?? $comment['authorId'] ?? 0));

                    $fields = [
                        'POST_MESSAGE' => $text,
                    ];
                    if ($authorId > 0) {
                        $fields['AUTHOR_ID'] = $authorId;
                    }

                    // Handle attached files in comment
                    $attachedObjects = $comment['ATTACHED_OBJECTS'] ?? $comment['attachedObjects'] ?? [];
                    if (!empty($attachedObjects) && is_array($attachedObjects)) {
                        $commentFileIds = $this->migrateCommentFiles($attachedObjects);
                        if (!empty($commentFileIds)) {
                            $fields['UF_FORUM_MESSAGE_DOC'] = $commentFileIds;
                        }
                    }

                    $this->boxAPI->addTaskComment($boxTaskId, $fields);
                    usleep(333000);
                } catch (\Throwable $e) {
                    $this->addLog('  Комментарий #' . $commentId . ': ' . $e->getMessage());
                }
            }
        } catch (\Throwable $e) {
            $this->addLog('  Комментарии #' . $cloudTaskId . ': ' . $e->getMessage());
        }
    }

    private function migrateCommentFiles(array $attachedObjects): array
    {
        $boxFileIds = [];

        foreach ($attachedObjects as $obj) {
            try {
                $fileId = (int)($obj['FILE_ID'] ?? $obj['fileId'] ?? 0);
                if ($fileId <= 0) continue;

                $fileInfo = $this->cloudAPI->getDiskFile($fileId);
                usleep(333000);

                $downloadUrl = $fileInfo['DOWNLOAD_URL'] ?? '';
                $fileName    = $fileInfo['NAME'] ?? ('comment_file_' . $fileId);

                if (empty($downloadUrl)) continue;

                $boxFileId = $this->downloadAndUploadFile($downloadUrl, $fileName);
                if ($boxFileId > 0) {
                    $boxFileIds[] = 'n' . $boxFileId;
                }
            } catch (\Throwable $e) {
                $this->addLog('  Файл комментария #' . ($obj['FILE_ID'] ?? $obj['fileId'] ?? '?') . ': ' . $e->getMessage());
            }
        }

        return $boxFileIds;
    }

    /**
     * Download file from URL to temp file, upload to migration folder via D7.
     * Returns box disk file ID or 0.
     */
    private function downloadAndUploadFile(string $downloadUrl, string $fileName): int
    {
        $tmpPath = tempnam(sys_get_temp_dir(), 'bx_mig_');

        try {
            $ch = curl_init($downloadUrl);
            $fp = fopen($tmpPath, 'wb');
            curl_setopt_array($ch, [
                CURLOPT_FILE           => $fp,
                CURLOPT_TIMEOUT        => 60,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_SSL_VERIFYPEER => false,
            ]);
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);
            fclose($fp);

            if ($httpCode !== 200 || filesize($tmpPath) === 0) {
                $this->addLog('  Скачивание "' . $fileName . '": HTTP ' . $httpCode);
                return 0;
            }

            $boxFileId = BoxD7Service::uploadFileToFolder($this->migrationFolderId, $tmpPath, $fileName);
            return $boxFileId ?? 0;

        } finally {
            @unlink($tmpPath);
        }
    }

    // =========================================================================
    // Status & parent links
    // =========================================================================

    private function applyTaskStatus($boxTaskId, $status)
    {
        try {
            // Bitrix task statuses: 2=Waiting, 3=In progress, 4=Supposedly completed, 5=Completed, 6=Deferred
            if ($status === 5) {
                $this->boxAPI->completeTask($boxTaskId);
            } elseif ($status === 6) {
                $this->boxAPI->deferTask($boxTaskId);
            }
        } catch (\Throwable $e) {
            $this->addLog('  Статус box #' . $boxTaskId . ': ' . $e->getMessage());
        }
    }

    private function storeParentLink($cloudTaskId, $cloudParentId)
    {
        $links = json_decode(Option::get(self::MODULE_ID, 'task_parent_links', '[]'), true) ?: [];
        $links[$cloudTaskId] = $cloudParentId;
        Option::set(self::MODULE_ID, 'task_parent_links', json_encode($links));
    }

    private function linkParentTasks()
    {
        $links = json_decode(Option::get(self::MODULE_ID, 'task_parent_links', '[]'), true) ?: [];
        if (empty($links)) return;

        $linked = 0;
        foreach ($links as $cloudTaskId => $cloudParentId) {
            $boxTaskId   = $this->taskIdMap[$cloudTaskId]   ?? 0;
            $boxParentId = $this->taskIdMap[$cloudParentId] ?? 0;

            if ($boxTaskId > 0 && $boxParentId > 0) {
                try {
                    $this->boxAPI->updateTask($boxTaskId, ['PARENT_ID' => $boxParentId]);
                    $linked++;
                    usleep(333000);
                } catch (\Throwable $e) {
                    $this->addLog('Связь ' . $cloudTaskId . '→' . $cloudParentId . ': ' . $e->getMessage());
                }
            }

            $this->rateLimitPause();
        }

        $this->addLog('Связи между задачами: ' . $linked . ' из ' . count($links));
        Option::delete(self::MODULE_ID, ['name' => 'task_parent_links']);
    }

    // =========================================================================
    // Migration folder
    // =========================================================================

    private function getOrCreateMigrationFolder(): int
    {
        if ($this->migrationFolderId > 0) {
            return $this->migrationFolderId;
        }
        return BoxD7Service::getOrCreateMigrationFolder(self::FOLDER_NAME);
    }

    // =========================================================================
    // Task list
    // =========================================================================

    private function fetchAllTaskIds()
    {
        // tasks.task.list returns result.tasks (nested array), not result directly
        $allTasks = $this->cloudAPI->fetchAll('tasks.task.list', ['select' => ['ID']], 'tasks');
        $ids = [];
        foreach ($allTasks as $t) {
            $id = (int)($t['id'] ?? $t['ID'] ?? 0);
            if ($id > 0) $ids[] = $id;
        }
        return $ids;
    }

    // =========================================================================
    // Rate limiting
    // =========================================================================

    private function rateLimitPause()
    {
        $this->checkStop();
    }

    // =========================================================================
    // Logging & status
    // =========================================================================

    private function addLog($message)
    {
        $line = date('H:i:s') . ' ' . $message;
        $this->log[] = $line;

        // Keep last 500 lines in option for UI
        $last = array_slice($this->log, -500);
        Option::set(self::MODULE_ID, 'migration_log', json_encode($last, JSON_UNESCAPED_UNICODE));

        // File log
        if ($this->logFile) {
            @file_put_contents($this->logFile, date('Y-m-d ') . $line . "\n", FILE_APPEND);
        }
    }

    private function saveStatus($status, $message)
    {
        Option::set(self::MODULE_ID, 'migration_status', $status);
        Option::set(self::MODULE_ID, 'migration_message', $message);
        Option::set(self::MODULE_ID, 'migration_stats', json_encode($this->stats));
    }

    private function saveResult()
    {
        Option::set(self::MODULE_ID, 'migration_task_id_map', json_encode($this->taskIdMap));
        Option::set(self::MODULE_ID, 'migration_completed_at', (string)time());
    }

    private function getResult()
    {
        return [
            'stats'   => $this->stats,
            'log'     => $this->log,
            'taskMap' => $this->taskIdMap,
        ];
    }
}
