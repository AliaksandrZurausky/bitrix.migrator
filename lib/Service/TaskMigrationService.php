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
    private $companyMapCache = [];
    private $contactMapCache = [];
    private $dealMapCache    = [];
    private $leadMapCache    = [];

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

    public function setCrmMapCaches(array $companyMap, array $contactMap, array $dealMap, array $leadMap)
    {
        $this->companyMapCache = $companyMap;
        $this->contactMapCache = $contactMap;
        $this->dealMapCache = $dealMap;
        $this->leadMapCache = $leadMap;
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

            // 2. Pre-build user map cache (skipped if already set by MigrationService)
            $this->buildUserMapCache();
            $this->addLog('Кэш пользователей: ' . count($this->userMapCache) . ' маппингов');

            // 3. Pre-build group map cache (skipped if already set by MigrationService)
            $this->buildGroupMapCache();
            $this->addLog('Кэш групп: ' . count($this->groupMapCache) . ' маппингов');

            // 4. Fetch all task IDs from cloud
            $this->checkStop();
            $this->saveStatus('running', 'Получение списка задач из облака...');
            $taskIds = $this->fetchAllTaskIds();
            $this->stats['total'] = count($taskIds);
            $this->addLog('Найдено задач в облаке: ' . count($taskIds));

            if (empty($taskIds)) {
                $this->saveStatus('completed', 'Нет задач для миграции');
                return $this->getResult();
            }

            // 5. Migrate each task (first pass)
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

            // 6. Second pass: link parent tasks
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
        $description = $task['description'] ?? $task['DESCRIPTION'] ?? '';
        if (!empty($description)) {
            $description = $this->migrateBase64Images($description);
        }

        $fields = [
            'TITLE'          => $title,
            'DESCRIPTION'    => $description,
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

        // Chat messages with files (sent directly to task chat, not as formal comments)
        $this->migrateChatFiles($cloudTaskId, $boxTaskId, $task);

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
        $boxAuditors = [];
        if (!empty($auditors) && is_array($auditors)) {
            $boxAuditors = $this->mapUserList($auditors);
        }

        // Add admin (user 1) as auditor if task has files — ensures REST API access to files
        if (!empty($fileIds) && !in_array(1, $boxAuditors)) {
            $boxAuditors[] = 1;
            $this->addLog('  Добавлен админ как наблюдатель (файлы есть)');
        }

        if (!empty($boxAuditors)) {
            try {
                $this->boxAPI->updateTask($boxTaskId, ['AUDITORS' => $boxAuditors]);
            } catch (\Throwable $e) {
                $this->addLog('  Наблюдатели #' . $cloudTaskId . ': ' . $e->getMessage());
            }
        }

        usleep(333000); // rate limit between tasks
    }

    // =========================================================================
    // User mapping
    // =========================================================================

    private function buildUserMapCache()
    {
        if (!empty($this->userMapCache)) return; // already set by MigrationService

        $cloudUsers = $this->cloudAPI->getUsers();
        $boxUsers   = $this->boxAPI->getUsers();

        $boxEmailMap = [];
        foreach ($boxUsers as $u) {
            $email = strtolower(trim($u['EMAIL'] ?? ''));
            if ($email) $boxEmailMap[$email] = (int)$u['ID'];
        }

        foreach ($cloudUsers as $u) {
            $email   = strtolower(trim($u['EMAIL'] ?? ''));
            $cloudId = (int)$u['ID'];
            if ($email && isset($boxEmailMap[$email])) {
                $this->userMapCache[$cloudId] = $boxEmailMap[$email];
            }
        }
    }

    private function buildGroupMapCache()
    {
        if (!empty($this->groupMapCache)) return; // already set by MigrationService

        $cloudGroups = $this->cloudAPI->getWorkgroups();
        $boxGroups   = $this->boxAPI->getWorkgroups();

        $boxByName = [];
        foreach ($boxGroups as $g) {
            $boxByName[mb_strtolower(trim($g['NAME'] ?? ''))] = (int)$g['ID'];
        }

        foreach ($cloudGroups as $g) {
            $cloudId   = (int)$g['ID'];
            $lowerName = mb_strtolower(trim($g['NAME'] ?? ''));
            if ($lowerName && isset($boxByName[$lowerName])) {
                $this->groupMapCache[$cloudId] = $boxByName[$lowerName];
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
        return $this->groupMapCache[$cloudGroupId] ?? 0;
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

                // Step 1: get attached object to find disk file ID (OBJECT_ID)
                // disk.attachedObject.get does NOT return DOWNLOAD_URL — only OBJECT_ID
                $attachInfo = $this->cloudAPI->getAttachedObject($attachId);
                usleep(200000);

                $diskFileId = (int)($attachInfo['OBJECT_ID'] ?? 0);
                $fileName   = $attachInfo['NAME'] ?? '';

                if ($diskFileId <= 0) {
                    $this->addLog('  Файл #' . $attachId . ': нет OBJECT_ID в attachedObject');
                    continue;
                }

                // Step 2: get file info with DOWNLOAD_URL via disk.file.get
                $fileInfo    = $this->cloudAPI->getDiskFile($diskFileId);
                usleep(200000);

                $downloadUrl = $fileInfo['DOWNLOAD_URL'] ?? '';
                if (empty($fileName)) {
                    $fileName = $fileInfo['NAME'] ?? ('file_' . $diskFileId);
                }

                if (empty($downloadUrl)) {
                    $this->addLog('  Файл #' . $attachId . ' (disk#' . $diskFileId . '): нет DOWNLOAD_URL');
                    continue;
                }

                // Step 3: download and upload to box disk folder
                $boxFileId = $this->downloadAndUploadFile($downloadUrl, $fileName);

                if ($boxFileId > 0) {
                    $boxFileIds[] = 'n' . $boxFileId; // 'n' prefix = new disk file ID for UF_TASK_WEBDAV_FILES
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

    private function mapCrmBindings(array $cloudBindings): array
    {
        $boxBindings = [];

        foreach ($cloudBindings as $binding) {
            // Format: "CO_20", "C_456", "D_789", "L_012"
            if (!preg_match('/^(CO|C|D|L)_(\d+)$/', $binding, $m)) continue;

            $prefix  = $m[1];
            $cloudId = (int)$m[2];

            $boxId = 0;
            switch ($prefix) {
                case 'CO': $boxId = $this->companyMapCache[$cloudId] ?? 0; break;
                case 'C':  $boxId = $this->contactMapCache[$cloudId] ?? 0; break;
                case 'D':  $boxId = $this->dealMapCache[$cloudId] ?? 0; break;
                case 'L':  $boxId = $this->leadMapCache[$cloudId] ?? 0; break;
            }

            if ($boxId > 0) {
                $boxBindings[] = $prefix . '_' . $boxId;
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
            if (empty($comments) || !is_array($comments)) {
                $this->addLog('  Комментарии: нет');
                return;
            }

            $this->addLog('  Комментарии: ' . count($comments));

            // Get chat ID for file attachments via im.disk.file.commit
            $chatId = 0;
            try {
                // Add webhook user (admin ID=1) as auditor to gain chat access
                $this->boxAPI->updateTask($boxTaskId, [
                    'AUDITORS' => [1],
                ]);
                sleep(1);
                $chatId = $this->boxAPI->getTaskChatId($boxTaskId);
            } catch (\Throwable $e) {
                // Can't get chatId — will use REST without files
            }

            foreach ($comments as $idx => $commentInfo) {
                $commentId = (int)($commentInfo['ID'] ?? $commentInfo['id'] ?? (is_numeric($commentInfo) ? $commentInfo : 0));
                if ($commentId <= 0) {
                    continue;
                }

                try {
                    $comment = $this->cloudAPI->getTaskComment($cloudTaskId, $commentId);
                    $text = $comment['POST_MESSAGE'] ?? $comment['postMessage'] ?? '';

                    // Map cloud author to box user
                    $cloudAuthorId = (int)($comment['AUTHOR_ID'] ?? $comment['authorId'] ?? 0);
                    $boxAuthorId = $this->userMapCache[$cloudAuthorId] ?? 0;
                    if ($boxAuthorId <= 0) {
                        $boxAuthorId = 1; // fallback to admin
                    }

                    // Process explicit file attachments (ATTACHED_OBJECTS)
                    $attachedObjects = $comment['ATTACHED_OBJECTS'] ?? $comment['attachedObjects'] ?? [];
                    $boxFileIds = [];
                    if (!empty($attachedObjects) && is_array($attachedObjects)) {
                        $boxFileIds = $this->migrateCommentFiles($attachedObjects);
                    }

                    if (empty($text) && empty($boxFileIds)) continue;

                    // Replace inline [DISK FILE ID=nXXX] tags and base64 images in text
                    $text = $this->migrateInlineDiskFiles($text);
                    $text = $this->migrateBase64Images($text);

                    // Send comment via REST
                    if ($chatId > 0 && !empty($boxFileIds)) {
                        // Files + chat → im.disk.file.commit (author = webhook user, but files attach)
                        try {
                            $cleanText = trim(preg_replace('/\[DISK FILE ID=n?\d+[^\]]*\]/i', '', $text));
                            $this->boxAPI->commitFilesToChat($chatId, $boxFileIds, $cleanText ?: '.');
                            $this->addLog('  Комментарий #' . $commentId . ' → im.disk.file.commit (' . count($boxFileIds) . ' файлов)');
                        } catch (\Throwable $e) {
                            $this->addLog('  Комментарий #' . $commentId . ' файлы не прикреплены: ' . $e->getMessage());
                            $fields = ['POST_MESSAGE' => $text ?: '.', 'AUTHOR_ID' => $boxAuthorId];
                            $this->boxAPI->addTaskComment($boxTaskId, $fields);
                            $this->addLog('  Комментарий #' . $commentId . ' → task.commentitem.add (автор=' . $boxAuthorId . ', без файлов)');
                        }
                    } else {
                        // Text only or no chat → task.commentitem.add with correct AUTHOR_ID
                        $fields = ['POST_MESSAGE' => $text ?: '.', 'AUTHOR_ID' => $boxAuthorId];
                        $this->boxAPI->addTaskComment($boxTaskId, $fields);
                        $this->addLog('  Комментарий #' . $commentId . ' → task.commentitem.add (автор=' . $boxAuthorId . ')');
                    }

                    usleep(333000);
                } catch (\Throwable $e) {
                    $this->addLog('  Комментарий #' . $commentId . ': ' . $e->getMessage());
                }
            }
        } catch (\Throwable $e) {
            $this->addLog('  Комментарии #' . $cloudTaskId . ': ' . $e->getMessage());
        }
    }

    private function migrateChatFiles($cloudTaskId, $boxTaskId, array $cloudTask = [])
    {
        try {
            $cloudChatId = $this->cloudAPI->getTaskChatId($cloudTaskId);
            if ($cloudChatId <= 0) return;

            $messages = $this->cloudAPI->getChatMessages($cloudChatId);
            if (empty($messages)) return;

            $fileMessages = array_filter($messages, fn($m) => !empty($m['params']['FILE_ID']));
            if (empty($fileMessages)) return;

            $this->addLog('  Файлы в чате: ' . count($fileMessages) . ' сообщений');

            $memberCloudIds = [];
            foreach (['createdBy', 'CREATED_BY', 'responsibleId', 'RESPONSIBLE_ID'] as $key) {
                $val = $cloudTask[$key] ?? null;
                if ($val) $memberCloudIds[] = (int)$val;
            }
            foreach (['accomplices', 'ACCOMPLICES', 'auditors', 'AUDITORS'] as $key) {
                $vals = $cloudTask[$key] ?? [];
                if (is_array($vals)) {
                    foreach ($vals as $v) $memberCloudIds[] = (int)$v;
                }
            }
            $memberCloudIds = array_unique(array_filter($memberCloudIds));

            $boxUserIds = [];
            foreach ($memberCloudIds as $cloudUid) {
                $boxUid = $this->userMapCache[$cloudUid] ?? null;
                if ($boxUid) $boxUserIds[] = (int)$boxUid;
            }
            $boxUserIds = array_unique(array_filter($boxUserIds));
            if (empty($boxUserIds)) $boxUserIds = [1];

            $boxChatId = BoxD7Service::createTaskChat($boxTaskId, 'Task #' . $boxTaskId, $boxUserIds);
            if ($boxChatId <= 0) {
                $this->addLog('  Не удалось создать чат задачи');
                return;
            }

            foreach ($fileMessages as $msg) {
                $fileIds = $msg['params']['FILE_ID'] ?? [];
                $text = $msg['text'] ?? '';

                foreach ($fileIds as $cloudFileId) {
                    try {
                        $fileInfo = $this->cloudAPI->getDiskFile($cloudFileId);
                        $downloadUrl = $fileInfo['DOWNLOAD_URL'] ?? '';
                        $fileName = $fileInfo['NAME'] ?? ('file_' . $cloudFileId);
                        if (empty($downloadUrl)) continue;

                        $tmpPath = tempnam(sys_get_temp_dir(), 'bx_mig_');
                        $ch = curl_init($downloadUrl);
                        $fp = fopen($tmpPath, 'wb');
                        curl_setopt_array($ch, [
                            CURLOPT_FILE => $fp,
                            CURLOPT_TIMEOUT => 120,
                            CURLOPT_FOLLOWLOCATION => true,
                            CURLOPT_SSL_VERIFYPEER => false,
                        ]);
                        curl_exec($ch);
                        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                        curl_close($ch);
                        fclose($fp);

                        if ($httpCode !== 200 || filesize($tmpPath) === 0) {
                            @unlink($tmpPath);
                            continue;
                        }

                        $pathinfo = pathinfo($fileName);
                        $uniqueName = $pathinfo['filename'] . '_' . time() . '_' . mt_rand(1000, 9999);
                        if (!empty($pathinfo['extension'])) $uniqueName .= '.' . $pathinfo['extension'];

                        $cloudSenderId = (int)($msg['senderId'] ?? 0);
                        $boxSenderId = $this->userMapCache[$cloudSenderId] ?? 1;

                        $msgId = BoxD7Service::sendFileToChatD7($boxChatId, $tmpPath, $uniqueName, $text, (int)$boxSenderId);
                        @unlink($tmpPath);

                        if ($msgId > 0) {
                            $this->addLog('  Файл чата "' . $fileName . '" -> msg #' . $msgId);
                        } else {
                            $this->addLog('  Файл чата "' . $fileName . '" -> не удалось');
                        }
                        $text = '';
                        usleep(333000);
                    } catch (\Throwable $e) {
                        $this->addLog('  Файл чата #' . $cloudFileId . ': ' . $e->getMessage());
                    }
                }
            }
        } catch (\Throwable $e) {
            $this->addLog('  Чат файлы: ' . $e->getMessage());
        }
    }

    /**
     * Find all [DISK FILE ID=nXXX] tags in comment text, download from cloud,
     * upload to box disk, and replace cloud IDs with box IDs.
     */
    private function migrateInlineDiskFiles(string $text): string
    {
        // Match [DISK FILE ID=nXXX] or [DISK FILE ID=XXX] with optional extra attrs (width, height, etc.)
        // e.g. [disk file id=n27096 width=432 height=600]
        if (!preg_match_all('/\[DISK FILE ID=n?(\d+)([^\]]*)\]/i', $text, $matches, PREG_SET_ORDER)) {
            return $text;
        }

        foreach ($matches as $match) {
            $cloudFileId = (int)$match[1];
            $extraAttrs  = $match[2]; // e.g. " width=432 height=600"
            $original    = $match[0];

            try {
                $fileInfo    = $this->cloudAPI->getDiskFile($cloudFileId);
                $downloadUrl = $fileInfo['DOWNLOAD_URL'] ?? '';
                $fileName    = $fileInfo['NAME'] ?? ('file_' . $cloudFileId);

                if (empty($downloadUrl)) {
                    $this->addLog('  Инлайн файл #' . $cloudFileId . ': нет DOWNLOAD_URL');
                    continue;
                }

                $boxFileId = $this->downloadAndUploadFile($downloadUrl, $fileName);
                if ($boxFileId > 0) {
                    $replacement = '[DISK FILE ID=n' . $boxFileId . $extraAttrs . ']';
                    $text = str_replace($original, $replacement, $text);
                    $this->addLog('  Инлайн файл "' . $fileName . '" → box disk ID ' . $boxFileId);
                }

                usleep(200000);
            } catch (\Throwable $e) {
                $this->addLog('  Инлайн файл #' . $cloudFileId . ': ' . $e->getMessage());
            }
        }

        return $text;
    }

    /**
     * Find all base64-encoded images in text (e.g. pasted inline images),
     * upload to box disk folder, replace with [DISK FILE ID=nXXX].
     */
    private function migrateBase64Images(string $text): string
    {
        if (strpos($text, 'base64,') === false) {
            return $text;
        }

        // Replace <img src="data:image/...;base64,..."> tags first
        $text = preg_replace_callback(
            '/<img[^>]+src=["\']?(data:image\/[^;]+;base64,[A-Za-z0-9+\/\r\n]+=*)["\']?[^>]*>/i',
            function ($m) {
                $fileId = $this->uploadBase64Image($m[1]);
                return $fileId > 0 ? '[DISK FILE ID=n' . $fileId . ']' : $m[0];
            },
            $text
        );

        // Replace any remaining bare data URIs
        $text = preg_replace_callback(
            '/data:(image\/[^;]+);base64,([A-Za-z0-9+\/\r\n]+=*)/i',
            function ($m) {
                $fileId = $this->uploadBase64Image($m[0]);
                return $fileId > 0 ? '[DISK FILE ID=n' . $fileId . ']' : $m[0];
            },
            $text
        );

        return $text;
    }

    private function uploadBase64Image(string $dataUri): int
    {
        if (!preg_match('/data:(image\/[^;]+);base64,([A-Za-z0-9+\/\r\n]+=*)/i', $dataUri, $m)) {
            return 0;
        }

        $mimeType   = $m[1];
        $base64Data = preg_replace('/\s+/', '', $m[2]);
        $decoded    = base64_decode($base64Data, true);

        if ($decoded === false || $decoded === '') {
            return 0;
        }

        $extMap = [
            'image/jpeg' => 'jpg', 'image/jpg' => 'jpg', 'image/png' => 'png',
            'image/gif'  => 'gif', 'image/webp' => 'webp', 'image/bmp' => 'bmp',
            'image/svg+xml' => 'svg', 'image/tiff' => 'tiff',
        ];
        $ext      = $extMap[strtolower($mimeType)] ?? 'bin';
        $fileName = 'img_' . uniqid() . '_' . mt_rand(1000, 9999) . '.' . $ext;

        try {
            // Upload via D7 (working, reliable)
            $tmpPath = tempnam(sys_get_temp_dir(), 'bx_img_');
            file_put_contents($tmpPath, $decoded);

            $fileId = BoxD7Service::uploadFileToFolder($this->migrationFolderId, $tmpPath, $fileName);
            @unlink($tmpPath);

            if ($fileId > 0) {
                $this->addLog('  Base64 изображение (' . $mimeType . ') → box disk ID ' . $fileId);
            }
            return $fileId ?? 0;
        } catch (\Throwable $e) {
            $this->addLog('  Base64 изображение: ' . $e->getMessage());
            return 0;
        }
    }

    /**
     * Migrate comment file attachments. Returns plain integer disk object IDs (no "n" prefix).
     */
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
                    $boxFileIds[] = $boxFileId; // plain integer for im.disk.file.commit
                    $this->addLog('  Файл комментария "' . $fileName . '" → disk ID ' . $boxFileId);
                }
            } catch (\Throwable $e) {
                $this->addLog('  Файл комментария #' . ($obj['FILE_ID'] ?? $obj['fileId'] ?? '?') . ': ' . $e->getMessage());
            }
        }

        return $boxFileIds;
    }

    /**
     * Download file from URL and upload to box migration folder via D7.
     * Uses curl for downloading, D7 for uploading (which is reliable).
     * Returns box disk file ID or 0.
     */
    private function downloadAndUploadFile(string $downloadUrl, string $fileName): int
    {
        $tmpPath = tempnam(sys_get_temp_dir(), 'bx_mig_');

        try {
            // Download to temp file
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

            // Generate unique name to avoid duplicates
            $pathinfo = pathinfo($fileName);
            $uniqueName = $pathinfo['filename'] . '_' . time() . '_' . mt_rand(1000, 9999);
            if (!empty($pathinfo['extension'])) {
                $uniqueName .= '.' . $pathinfo['extension'];
            }

            // Upload via D7 (working method)
            $fileId = BoxD7Service::uploadFileToFolder($this->migrationFolderId, $tmpPath, $uniqueName);
            return $fileId ?? 0;

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
        $params = ['select' => ['ID']];

        // Incremental mode: filter by CREATED_DATE > last migration timestamp
        if (!empty($this->plan['settings']['incremental'])) {
            $lastTs = Option::get(self::MODULE_ID, 'last_migration_timestamp', '');
            if (!empty($lastTs)) {
                $params['filter'] = ['>CREATED_DATE' => $lastTs];
            }
        }

        // tasks.task.list returns result.tasks (nested array), not result directly
        $allTasks = $this->cloudAPI->fetchAll('tasks.task.list', $params, 'tasks');
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
