<?php

namespace BitrixMigrator\Integration;

class CloudAPI
{
    private $webhookUrl;

    public function __construct($webhookUrl)
    {
        $this->webhookUrl = rtrim($webhookUrl, '/');
    }

    /**
     * Append the webhook auth token to a download URL.
     * Bitrix24 cloud disk download URLs require ?auth=<token> to authorize the request,
     * otherwise they return 401/redirect to login page.
     */
    public function authorizeUrl(string $url): string
    {
        if (empty($url)) return $url;
        // Webhook URL format: https://domain.bitrix24.ru/rest/1/SECRET_TOKEN
        // Extract the SECRET_TOKEN segment
        $parts = explode('/', $this->webhookUrl);
        $token = end($parts);
        if (empty($token) || $token === 'rest') return $url;
        $sep = (strpos($url, '?') !== false) ? '&' : '?';
        return $url . $sep . 'auth=' . urlencode($token);
    }

    /**
     * Returns cloud portal base URL (e.g. https://domain.bitrix24.ru).
     * Extracted from webhookUrl: https://domain.bitrix24.ru/rest/1/xxx → https://domain.bitrix24.ru
     */
    public function getPortalBaseUrl(): string
    {
        $parts = parse_url($this->webhookUrl);
        return ($parts['scheme'] ?? 'https') . '://' . ($parts['host'] ?? '');
    }

    /**
     * Make API request with JSON body, retries on HTTP 429
     */
    private function request($method, $params = [], $attempt = 0)
    {
        $url = $this->webhookUrl . '/' . $method . '.json';

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST           => true,
            CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
            CURLOPT_POSTFIELDS     => json_encode($params),
            CURLOPT_TIMEOUT        => 60,
            CURLOPT_HEADER         => true,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
        ]);

        $rawResponse = curl_exec($ch);
        $httpCode    = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $headerSize  = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
        $error       = curl_error($ch);
        curl_close($ch);

        if ($error) {
            throw new \Exception('cURL error: ' . $error);
        }

        // Rate limit — wait and retry (max 5 attempts)
        if ($httpCode === 429) {
            if ($attempt >= 5) {
                throw new \Exception('Rate limit exceeded after 5 retries');
            }
            $headers  = substr($rawResponse, 0, $headerSize);
            $retryAfter = 2;
            if (preg_match('/Retry-After:\s*(\d+)/i', $headers, $m)) {
                $retryAfter = (int)$m[1];
            }
            sleep($retryAfter);
            return $this->request($method, $params, $attempt + 1);
        }

        if ($httpCode !== 200) {
            $body = substr($rawResponse, $headerSize);
            $decoded = json_decode($body, true);
            $detail = '';
            if (is_array($decoded)) {
                $detail = ': ' . ($decoded['error_description'] ?? $decoded['error'] ?? $body);
            }
            throw new \Exception('HTTP error: ' . $httpCode . $detail);
        }

        $body = substr($rawResponse, $headerSize);
        $data = json_decode($body, true);
        if (!is_array($data)) {
            throw new \Exception('Bad JSON response');
        }

        if (isset($data['error'])) {
            throw new \Exception($data['error_description'] ?? $data['error']);
        }

        return $data;
    }

    /**
     * Get all currencies with full data
     */
    public function getCurrencies(): array
    {
        $result = $this->request('crm.currency.list');
        return $result['result'] ?? [];
    }

    /**
     * Get available currencies (returns set of CURRENCY codes)
     */
    public function getCurrencyCodes(): array
    {
        $codes = [];
        foreach ($this->getCurrencies() as $item) {
            $code = $item['CURRENCY'] ?? '';
            if ($code) $codes[$code] = true;
        }
        return $codes;
    }

    /**
     * Add a currency
     */
    public function addCurrency(array $fields)
    {
        $result = $this->request('crm.currency.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    /**
     * Get entity count (uses 'total' from first request)
     */
    public function getCount($method, $params = [])
    {
        $params['start'] = 0;
        $result = $this->request($method, $params);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Fetch all entities with pagination.
     *
     * @param string      $method    REST API method
     * @param array       $params    Request params
     * @param string|null $resultKey When the API wraps items in a sub-key of result
     *                               (e.g. 'tasks' for tasks.task.list which returns result.tasks)
     */
    public function fetchAll($method, $params = [], $resultKey = null)
    {
        $items = [];
        $this->fetchBatched($method, $params, function ($batch) use (&$items) {
            // Use foreach instead of array_push(...) — PHP 8.1+ rejects string-keyed unpacking
            foreach ($batch as $item) {
                $items[] = $item;
            }
        }, $resultKey);
        return $items;
    }

    /**
     * Paginated fetch with callback per batch — avoids loading everything into memory.
     * Callback receives array of items for each page (typically 50 items).
     */
    public function fetchBatched($method, $params, callable $callback, $resultKey = null): int
    {
        $next = 0;
        $page = 0;
        $total = 0;
        $maxPages = 10000;

        do {
            $page++;
            if ($page > $maxPages) {
                throw new \Exception("fetchBatched exceeded {$maxPages} pages for {$method}");
            }

            $params['start'] = $next;
            $result = $this->request($method, $params);

            if (isset($result['result']) && is_array($result['result'])) {
                $data = $resultKey ? ($result['result'][$resultKey] ?? []) : $result['result'];
                if (is_array($data) && !empty($data)) {
                    $callback($data);
                    $total += count($data);
                }
            }

            $newNext = $result['next'] ?? null;
            if ($newNext !== null && $newNext === $next) break;
            $next = $newNext;

            if ($next !== null) {
                usleep(500000);
            }
        } while ($next !== null);

        return $total;
    }

    /**
     * Get departments (structure)
     */
    public function getDepartments()
    {
        return $this->fetchAll('department.get');
    }

    /**
     * Get users count
     */
    public function getUsersCount()
    {
        return $this->getCount('user.get', ['USER_TYPE' => 'employee']);
    }

    /**
     * Get companies count
     */
    public function getCompaniesCount()
    {
        return $this->getCount('crm.company.list');
    }

    /**
     * Get contacts count
     */
    public function getContactsCount()
    {
        return $this->getCount('crm.contact.list');
    }

    /**
     * Get deals count
     */
    public function getDealsCount()
    {
        return $this->getCount('crm.deal.list');
    }

    /**
     * Get leads count
     */
    public function getLeadsCount()
    {
        return $this->getCount('crm.lead.list');
    }

    /**
     * Get tasks count
     */
    public function getTasksCount()
    {
        // tasks.task.list returns total at top level, same as getCount reads
        return $this->getCount('tasks.task.list');
    }

    /**
     * Get all intranet users including dismissed (inactive).
     * user.get with USER_TYPE=employee returns only active by default,
     * so we make a second call with ACTIVE=false for dismissed users.
     */
    public function getUsers()
    {
        $active   = $this->fetchAll('user.get', ['USER_TYPE' => 'employee', 'ACTIVE' => true]);
        $inactive = $this->fetchAll('user.get', ['USER_TYPE' => 'employee', 'ACTIVE' => false]);
        // Deduplicate by ID just in case
        $byId = [];
        foreach (array_merge($active, $inactive) as $u) {
            $id = (int)($u['ID'] ?? 0);
            if ($id > 0) $byId[$id] = $u;
        }
        return array_values($byId);
    }

    /**
     * Get all companies
     */
    public function getCompanies($select = ['ID', 'TITLE'], array $filter = [])
    {
        $params = ['select' => $select];
        if (!empty($filter)) $params['filter'] = $filter;
        return $this->fetchAll('crm.company.list', $params);
    }

    /**
     * Get all contacts
     */
    public function getContacts($select = ['ID', 'NAME', 'LAST_NAME'], array $filter = [])
    {
        $params = ['select' => $select];
        if (!empty($filter)) $params['filter'] = $filter;
        return $this->fetchAll('crm.contact.list', $params);
    }

    /**
     * Get all deals
     */
    public function getDeals($select = ['ID', 'TITLE'], array $filter = [])
    {
        $params = ['select' => $select];
        if (!empty($filter)) $params['filter'] = $filter;
        return $this->fetchAll('crm.deal.list', $params);
    }

    /**
     * Get all leads
     */
    public function getLeads($select = ['ID', 'TITLE'], array $filter = [])
    {
        $params = ['select' => $select];
        if (!empty($filter)) $params['filter'] = $filter;
        return $this->fetchAll('crm.lead.list', $params);
    }

    /**
     * Get all tasks
     */
    public function getTasks($select = ['ID', 'TITLE'], array $filter = [])
    {
        $params = ['select' => $select];
        if (!empty($filter)) $params['filter'] = $filter;
        // tasks.task.list returns result.tasks (nested), not result directly
        return $this->fetchAll('tasks.task.list', $params, 'tasks');
    }

    /**
     * Get deal pipeline categories (custom funnels).
     * Tries crm.dealcategory.list first, falls back to crm.category.list (newer API).
     */
    public function getDealCategories()
    {
        try {
            $result     = $this->request('crm.dealcategory.list');
            $categories = $result['result']['categories'] ?? $result['result'] ?? [];
            return is_array($categories) ? array_values($categories) : [];
        } catch (\Exception $e) {
            // Fallback for newer Bitrix24 that removed crm.dealcategory.*
            $result     = $this->request('crm.category.list', ['entityTypeId' => 2]);
            $categories = $result['result']['categories'] ?? $result['result'] ?? [];
            return is_array($categories) ? array_values($categories) : [];
        }
    }

    /**
     * Fetch all deal-related statuses in one request and return them grouped by ENTITY_ID.
     * Keys: 'DEAL_STAGE' (default pipeline), 'DEAL_STAGE_N' (custom pipeline N).
     */
    public function getAllDealStagesGrouped(): array
    {
        $all = $this->fetchAll('crm.status.list', []);

        $grouped = [];
        foreach ($all as $status) {
            $entityId = $status['ENTITY_ID'] ?? '';
            // Default pipeline: 'DEAL_STAGE'; custom: 'DEAL_STAGE_N' (N = category ID)
            if ($entityId === 'DEAL_STAGE'
                || (strlen($entityId) > 11 && strncmp($entityId, 'DEAL_STAGE_', 11) === 0)
            ) {
                $grouped[$entityId][] = $status;
            }
        }

        return $grouped;
    }

    /**
     * Get CRM entity field schema (e.g. crm.deal.fields)
     */
    public function getFields($method)
    {
        $result = $this->request($method);
        return is_array($result['result']) ? $result['result'] : [];
    }

    /**
     * Get default deal category info (id=0 for default pipeline)
     */
    public function getDealCategoryById($id)
    {
        try {
            $result = $this->request('crm.dealcategory.get', ['ID' => (int)$id]);
            return $result['result'] ?? [];
        } catch (\Exception $e) {
            return [];
        }
    }

    /**
     * Get fields for a smart process entity type
     */
    public function getSmartProcessFields($entityTypeId)
    {
        $result = $this->request('crm.item.fields', ['entityTypeId' => (int)$entityTypeId]);
        $fields = $result['result']['fields'] ?? $result['result'] ?? [];
        return is_array($fields) ? $fields : [];
    }

    /**
     * Get workgroups count
     */
    public function getWorkgroupsCount()
    {
        return $this->getCount('sonet_group.get');
    }

    /**
     * Get all workgroups (sonet groups)
     */
    public function getWorkgroups()
    {
        return $this->fetchAll('sonet_group.get', []);
    }

    /**
     * Get all smart process types (crm.type.list)
     */
    public function getSmartProcessTypes()
    {
        $result = $this->request('crm.type.list');
        $types  = $result['result']['types'] ?? $result['result'] ?? [];
        return is_array($types) ? array_values($types) : [];
    }

    /**
     * Get item count for a smart process entity type
     */
    public function getSmartProcessCount($entityTypeId)
    {
        return $this->getCount('crm.item.list', ['entityTypeId' => (int)$entityTypeId]);
    }

    // =========================================================================
    // Task migration helpers
    // =========================================================================

    /**
     * Get single task by ID with all fields
     */
    public function getTask($taskId)
    {
        $result = $this->request('tasks.task.get', ['taskId' => (int)$taskId, 'select' => ['*', 'UF_*']]);
        return $result['result']['task'] ?? [];
    }

    /**
     * Create a task
     */
    public function createTask($fields)
    {
        $result = $this->request('tasks.task.add', ['fields' => $fields]);
        return $result['result']['task'] ?? $result['result'] ?? [];
    }

    /**
     * Update task fields
     */
    public function updateTask($taskId, $fields)
    {
        return $this->request('tasks.task.update', ['taskId' => (int)$taskId, 'fields' => $fields]);
    }

    /**
     * Complete a task
     */
    public function completeTask($taskId)
    {
        return $this->request('tasks.task.complete', ['taskId' => (int)$taskId]);
    }

    /**
     * Defer a task
     */
    public function deferTask($taskId)
    {
        return $this->request('tasks.task.defer', ['taskId' => (int)$taskId]);
    }

    public function deleteTask($taskId)
    {
        return $this->request('tasks.task.delete', ['taskId' => (int)$taskId]);
    }

    /**
     * Get task checklist items
     */
    public function getTaskChecklists($taskId)
    {
        $result = $this->request('task.checklistitem.getlist', ['TASKID' => (int)$taskId]);
        return $result['result'] ?? [];
    }

    /**
     * Add task checklist item
     */
    public function addTaskChecklistItem($taskId, $fields)
    {
        return $this->request('task.checklistitem.add', ['TASKID' => (int)$taskId, 'FIELDS' => $fields]);
    }

    /**
     * Get task comment list (IDs)
     */
    public function getTaskComments($taskId)
    {
        $result = $this->request('task.commentitem.getlist', ['TASKID' => (int)$taskId]);
        return $result['result'] ?? [];
    }

    /**
     * Get single task comment
     */
    public function getTaskComment($taskId, $commentId)
    {
        $result = $this->request('task.commentitem.get', ['TASKID' => (int)$taskId, 'ITEMID' => (int)$commentId]);
        return $result['result'] ?? [];
    }

    /**
     * Add task comment (legacy, pre-25.700.0)
     */
    public function addTaskComment($taskId, $fields)
    {
        return $this->request('task.commentitem.add', ['TASKID' => (int)$taskId, 'FIELDS' => $fields]);
    }

    /**
     * Get task chat ID (tasks 25.700.0+)
     */
    public function getTaskChatId(int $taskId): int
    {
        $result = $this->request('tasks.task.get', ['taskId' => $taskId, 'select' => ['CHAT_ID']]);
        $task   = $result['result']['task'] ?? $result['result'] ?? $result;
        return (int)($task['chatId'] ?? $task['CHAT_ID'] ?? 0);
    }

    /**
     * Send text message to task chat via im.message.add (works on all box versions)
     */
    public function sendTaskChatMessage(int $chatId, string $text): bool
    {
        $result = $this->request('im.message.add', [
            'DIALOG_ID' => 'chat' . $chatId,
            'MESSAGE'   => $text,
        ]);
        return !empty($result['result'] ?? $result);
    }

    /**
     * Commit disk files to chat (tasks 25.700.0+)
     * $fileIds — plain integer disk object IDs (no "n" prefix)
     */
    public function commitFilesToChat(int $chatId, array $fileIds, string $message = ''): array
    {
        $params = ['CHAT_ID' => $chatId, 'FILE_ID' => array_values($fileIds)];
        if ($message !== '') $params['MESSAGE'] = $message;
        $result = $this->request('im.disk.file.commit', $params);
        return $result ?? [];
    }

    /**
     * Get chat messages for a task chat (im.dialog.messages.get).
     * Returns messages that may contain FILE_ID params (files sent directly to chat).
     */
    public function getChatMessages(int $chatId, int $limit = 50): array
    {
        $result = $this->request('im.dialog.messages.get', [
            'DIALOG_ID' => 'chat' . $chatId,
            'LIMIT' => $limit,
        ]);
        return $result['result']['messages'] ?? [];
    }

    /**
     * Get disk attached object info (download URL, name, etc.)
     */
    public function getAttachedObject($id)
    {
        $result = $this->request('disk.attachedObject.get', ['id' => (int)$id]);
        return $result['result'] ?? [];
    }

    /**
     * Get disk file info
     */
    public function getDiskFile($id)
    {
        $result = $this->request('disk.file.get', ['id' => (int)$id]);
        return $result['result'] ?? [];
    }

    /**
     * Download file by URL (returns binary content)
     */
    public function downloadFile($url)
    {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_TIMEOUT        => 120,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
        ]);
        $content = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error || $httpCode !== 200) {
            throw new \Exception('File download error: ' . ($error ?: 'HTTP ' . $httpCode));
        }

        return $content;
    }

    /**
     * Get list of disk storages
     */
    public function getStorages()
    {
        return $this->fetchAll('disk.storage.getlist', []);
    }

    /**
     * Get folder children (files and subfolders)
     */
    public function getFolderChildren($folderId)
    {
        return $this->fetchAll('disk.folder.getchildren', ['id' => (int)$folderId]);
    }

    /**
     * Create subfolder inside a folder
     */
    public function createSubfolder($folderId, $name)
    {
        $result = $this->request('disk.folder.addsubfolder', [
            'id'   => (int)$folderId,
            'data' => ['NAME' => $name],
        ]);
        return $result['result'] ?? [];
    }

    /**
     * Upload file to a disk folder (multipart/form-data)
     */
    public function uploadFileToFolder($folderId, $filename, $fileContent)
    {
        $url = $this->webhookUrl . '/disk.folder.uploadfile.json';

        $boundary = '----MigratorBoundary' . uniqid();
        $body  = '--' . $boundary . "\r\n";
        $body .= "Content-Disposition: form-data; name=\"id\"\r\n\r\n" . (int)$folderId . "\r\n";
        $body .= '--' . $boundary . "\r\n";
        $body .= "Content-Disposition: form-data; name=\"data[NAME]\"\r\n\r\n" . $filename . "\r\n";
        $body .= '--' . $boundary . "\r\n";
        $body .= "Content-Disposition: form-data; name=\"fileContent\"; filename=\"" . $filename . "\"\r\n";
        $body .= "Content-Type: application/octet-stream\r\n\r\n";
        $body .= $fileContent . "\r\n";
        $body .= '--' . $boundary . "--\r\n";

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST           => true,
            CURLOPT_HTTPHEADER     => ['Content-Type: multipart/form-data; boundary=' . $boundary],
            CURLOPT_POSTFIELDS     => $body,
            CURLOPT_TIMEOUT        => 120,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error) {
            throw new \Exception('Upload cURL error: ' . $error);
        }
        if ($httpCode !== 200) {
            throw new \Exception('Upload HTTP error: ' . $httpCode);
        }

        $data = json_decode($response, true);
        if (!is_array($data)) {
            throw new \Exception('Upload: bad JSON response');
        }
        if (isset($data['error'])) {
            throw new \Exception($data['error_description'] ?? $data['error']);
        }

        return $data['result'] ?? [];
    }

    /**
     * Find user by email on this portal
     */
    public function getUserByEmail($email)
    {
        $result = $this->request('user.get', ['filter' => ['EMAIL' => $email]]);
        $users = $result['result'] ?? [];
        return !empty($users) ? $users[0] : null;
    }

    /**
     * Find workgroup by name
     */
    public function getGroupByName($name)
    {
        // sonet_group.get doesn't support reliable filtering, fetch all and search
        $groups = $this->getWorkgroups();
        foreach ($groups as $g) {
            if (mb_strtolower(trim($g['NAME'] ?? '')) === mb_strtolower(trim($name))) {
                return $g;
            }
        }
        return null;
    }

    /**
     * Find CRM entity on box by title/name
     */
    public function findCrmEntityByTitle($entityType, $title)
    {
        $methodMap = [
            'C'  => ['crm.contact.list',  null],
            'D'  => ['crm.deal.list',    'TITLE'],
            'L'  => ['crm.lead.list',    'TITLE'],
            'CO' => ['crm.company.list', 'TITLE'],
        ];

        $config = $methodMap[$entityType] ?? null;
        if (!$config) return null;

        $method    = $config[0];
        $titleField = $config[1];

        if ($entityType === 'C') {
            // Contacts don't have TITLE, search by NAME + LAST_NAME is complex
            // Try searching with a generic approach
            $result = $this->request($method, ['filter' => ['%NAME' => $title], 'select' => ['ID', 'NAME', 'LAST_NAME']]);
        } else {
            $result = $this->request($method, ['filter' => [$titleField => $title], 'select' => ['ID', $titleField]]);
        }

        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    /**
     * Get single CRM entity by type and ID
     */
    public function getCrmEntity($entityType, $id)
    {
        $methodMap = [
            'C'  => 'crm.contact.get',
            'D'  => 'crm.deal.get',
            'L'  => 'crm.lead.get',
            'CO' => 'crm.company.get',
        ];

        $method = $methodMap[$entityType] ?? null;
        if (!$method) return null;

        $result = $this->request($method, ['ID' => (int)$id]);
        return $result['result'] ?? [];
    }

    // =========================================================================
    // Department management
    // =========================================================================

    public function addDepartment($fields)
    {
        $result = $this->request('department.add', $fields);
        return $result['result'] ?? null;
    }

    public function deleteDepartment($id)
    {
        $result = $this->request('department.delete', ['ID' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function updateDepartment($id, $fields)
    {
        $fields['ID'] = (int)$id;
        $result = $this->request('department.update', $fields);
        return $result['result'] ?? false;
    }

    // =========================================================================
    // User management
    // =========================================================================

    public function inviteUser($fields)
    {
        $result = $this->request('user.add', $fields);
        return $result['result'] ?? null;
    }

    public function updateUser($id, $fields)
    {
        $fields['ID'] = (int)$id;
        $result = $this->request('user.update', $fields);
        return $result['result'] ?? false;
    }

    // =========================================================================
    // CRM Company
    // =========================================================================

    public function getCompany($id)
    {
        $result = $this->request('crm.company.get', ['ID' => (int)$id]);
        return $result['result'] ?? [];
    }

    public function addCompany($fields)
    {
        $result = $this->request('crm.company.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function deleteCompany($id)
    {
        $result = $this->request('crm.company.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function updateCompany($id, $fields)
    {
        return $this->request('crm.company.update', ['id' => (int)$id, 'fields' => $fields]);
    }

    public function findCompanyByField($field, $value)
    {
        $result = $this->request('crm.company.list', [
            'filter' => [$field => $value],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    // =========================================================================
    // CRM Contact
    // =========================================================================

    public function getContact($id)
    {
        $result = $this->request('crm.contact.get', ['ID' => (int)$id]);
        return $result['result'] ?? [];
    }

    public function addContact($fields)
    {
        $result = $this->request('crm.contact.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function deleteContact($id)
    {
        $result = $this->request('crm.contact.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function updateContact($id, $fields)
    {
        return $this->request('crm.contact.update', ['id' => (int)$id, 'fields' => $fields]);
    }

    public function findContactByField($field, $value)
    {
        $result = $this->request('crm.contact.list', [
            'filter' => [$field => $value],
            'select' => ['ID', 'NAME', 'LAST_NAME'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    // =========================================================================
    // CRM Deal
    // =========================================================================

    public function getDeal($id)
    {
        $result = $this->request('crm.deal.get', ['ID' => (int)$id]);
        return $result['result'] ?? [];
    }

    public function addDeal($fields)
    {
        $result = $this->request('crm.deal.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function deleteDeal($id)
    {
        $result = $this->request('crm.deal.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function updateDeal($id, $fields)
    {
        return $this->request('crm.deal.update', ['id' => (int)$id, 'fields' => $fields]);
    }

    // =========================================================================
    // CRM Lead
    // =========================================================================

    public function getLead($id)
    {
        $result = $this->request('crm.lead.get', ['ID' => (int)$id]);
        return $result['result'] ?? [];
    }

    public function addLead($fields)
    {
        $result = $this->request('crm.lead.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function deleteLead($id)
    {
        $result = $this->request('crm.lead.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function updateLead($id, $fields)
    {
        return $this->request('crm.lead.update', ['id' => (int)$id, 'fields' => $fields]);
    }

    // =========================================================================
    // CRM Deal Categories (Pipelines)
    // =========================================================================

    public function addDealCategory($fields)
    {
        $result = $this->request('crm.dealcategory.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function deleteDealCategory($id)
    {
        $result = $this->request('crm.dealcategory.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function updateDealCategory($id, $fields)
    {
        $result = $this->request('crm.dealcategory.update', ['id' => (int)$id, 'fields' => $fields]);
        return $result['result'] ?? false;
    }

    /**
     * Get the default (system, ID=0) deal category — its name lives in a separate option,
     * not in the dealcategory.list response.
     */
    public function getDefaultDealCategory()
    {
        $result = $this->request('crm.dealcategory.default.get', []);
        return $result['result'] ?? [];
    }

    public function getDealCategoryStages($categoryId)
    {
        $result = $this->request('crm.dealcategory.stage.list', ['id' => (int)$categoryId]);
        return $result['result'] ?? [];
    }

    public function addDealCategoryStage($fields)
    {
        $result = $this->request('crm.status.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    // =========================================================================
    // CRM Status (stages)
    // =========================================================================

    public function getStatuses()
    {
        return $this->fetchAll('crm.status.list', []);
    }

    public function getStatusesByEntityId(string $entityId): array
    {
        return $this->fetchAll('crm.status.list', ['filter' => ['ENTITY_ID' => $entityId]]);
    }

    public function addStatus($fields)
    {
        $result = $this->request('crm.status.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function updateStatus(int $id, array $fields)
    {
        $result = $this->request('crm.status.update', ['id' => $id, 'fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function deleteStatus($id)
    {
        $result = $this->request('crm.status.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    // =========================================================================
    // CRM Custom Fields (userfields)
    // =========================================================================

    public function addUserfield($entityType, $fields)
    {
        $methodMap = [
            'deal'    => 'crm.deal.userfield.add',
            'contact' => 'crm.contact.userfield.add',
            'company' => 'crm.company.userfield.add',
            'lead'    => 'crm.lead.userfield.add',
        ];
        $method = $methodMap[$entityType] ?? null;
        if (!$method) throw new \Exception("Unknown entity type for userfield: $entityType");

        $result = $this->request($method, ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function getUserfields($entityType)
    {
        $methodMap = [
            'deal'    => 'crm.deal.userfield.list',
            'contact' => 'crm.contact.userfield.list',
            'company' => 'crm.company.userfield.list',
            'lead'    => 'crm.lead.userfield.list',
        ];
        $method = $methodMap[$entityType] ?? null;
        if (!$method) return [];

        return $this->fetchAll($method, []);
    }

    public function deleteUserfield($entityType, $id)
    {
        $methodMap = [
            'deal'    => 'crm.deal.userfield.delete',
            'contact' => 'crm.contact.userfield.delete',
            'company' => 'crm.company.userfield.delete',
            'lead'    => 'crm.lead.userfield.delete',
        ];
        $method = $methodMap[$entityType] ?? null;
        if (!$method) throw new \Exception("Unknown entity type for userfield delete: $entityType");

        $result = $this->request($method, ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    // =========================================================================
    // Workgroup (sonet_group) management
    // =========================================================================

    public function createWorkgroup($fields)
    {
        $result = $this->request('sonet_group.create', $fields);
        return $result['result'] ?? null;
    }

    public function deleteWorkgroup($id)
    {
        $result = $this->request('sonet_group.delete', ['GROUP_ID' => (int)$id]);
        return $result['result'] ?? false;
    }

    /**
     * Get members of a workgroup.
     * Returns array of items with USER_ID, ROLE (A=admin, E=member, K=moderator).
     */
    public function getWorkgroupMembers($groupId)
    {
        return $this->fetchAll('sonet_group.user.get', ['ID' => (int)$groupId]);
    }

    /**
     * Add user to a workgroup.
     * ROLE: A=admin, E=member (employee), K=moderator.
     */
    public function addWorkgroupMember($groupId, $userId, $role = 'E')
    {
        $result = $this->request('sonet_group.user.add', [
            'GROUP_ID' => (int)$groupId,
            'USER_ID'  => (int)$userId,
            'ROLE'     => $role,
        ]);
        return $result['result'] ?? false;
    }

    // =========================================================================
    // Smart Processes
    // =========================================================================

    public function addSmartProcessType($fields)
    {
        $result = $this->request('crm.type.add', ['fields' => $fields]);
        return $result['result']['type'] ?? $result['result'] ?? null;
    }

    /**
     * Get userfields for a smart process by entityId (e.g. "CRM_128")
     * Uses userfieldconfig.list to get field configs with IDs for deletion
     */
    public function getSmartProcessUserfields($entityId)
    {
        // moduleId is required; filter key casing follows Bitrix uppercase convention
        return $this->fetchAll('userfieldconfig.list', [
            'moduleId' => 'crm',
            'filter'   => ['ENTITY_ID' => $entityId],
        ]);
    }

    /**
     * Delete a userfield config by ID
     */
    public function deleteSmartProcessUserfield($id)
    {
        $result = $this->request('userfieldconfig.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function getSmartProcessItems($entityTypeId, $select = ['*', 'uf_*'], array $filter = [])
    {
        $params = [
            'entityTypeId' => (int)$entityTypeId,
            'select' => $select,
        ];
        if (!empty($filter)) $params['filter'] = $filter;
        return $this->fetchAll('crm.item.list', $params);
    }

    public function deleteSmartProcessItem($entityTypeId, $id)
    {
        $result = $this->request('crm.item.delete', [
            'entityTypeId' => (int)$entityTypeId,
            'id' => (int)$id,
        ]);
        return $result['result'] ?? false;
    }

    public function deleteSmartProcessType($id)
    {
        $result = $this->request('crm.type.delete', ['id' => (int)$id]);
        return $result['result'] ?? false;
    }

    public function addSmartProcessItem($entityTypeId, $fields)
    {
        $result = $this->request('crm.item.add', [
            'entityTypeId' => (int)$entityTypeId,
            'fields' => $fields,
        ]);
        return $result['result']['item'] ?? $result['result'] ?? null;
    }

    // =========================================================================
    // CRM Timeline / Activities
    // =========================================================================

    public function getActivities($entityTypeId, $entityId)
    {
        return $this->fetchAll('crm.activity.list', [
            'filter' => [
                'OWNER_TYPE_ID' => (int)$entityTypeId,
                'OWNER_ID' => (int)$entityId,
            ],
            'select' => ['*'],
        ]);
    }

    public function addActivity($fields)
    {
        $result = $this->request('crm.activity.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function getTimelineComments($entityTypeId, $entityId)
    {
        // crm.timeline.comment.list requires ENTITY_TYPE as string, not numeric
        $typeMap = [1 => 'lead', 2 => 'deal', 3 => 'contact', 4 => 'company', 31 => 'quote'];
        $entityType = $typeMap[$entityTypeId] ?? '';
        if (empty($entityType)) return [];

        return $this->fetchAll('crm.timeline.comment.list', [
            'filter' => [
                'ENTITY_TYPE' => $entityType,
                'ENTITY_ID' => (int)$entityId,
            ],
        ]);
    }

    public function addTimelineComment($fields)
    {
        $result = $this->request('crm.timeline.comment.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    /**
     * Get full timeline comment by ID — includes FILES which list method omits.
     */
    public function getTimelineComment($id)
    {
        $result = $this->request('crm.timeline.comment.get', ['id' => (int)$id]);
        return $result['result'] ?? [];
    }

    // =========================================================================
    // CRM Contact-Company binding
    // =========================================================================

    public function getContactCompanyItems($contactId)
    {
        $result = $this->request('crm.contact.company.items.get', ['id' => (int)$contactId]);
        return $result['result'] ?? [];
    }

    public function setContactCompanyItems($contactId, $items)
    {
        return $this->request('crm.contact.company.items.set', [
            'id' => (int)$contactId,
            'items' => $items,
        ]);
    }

    // =========================================================================
    // CRM Deal-Contact binding
    // =========================================================================

    public function getDealContactItems($dealId)
    {
        $result = $this->request('crm.deal.contact.items.get', ['id' => (int)$dealId]);
        return $result['result'] ?? [];
    }

    public function setDealContactItems($dealId, $items)
    {
        return $this->request('crm.deal.contact.items.set', [
            'id' => (int)$dealId,
            'items' => $items,
        ]);
    }

    // =========================================================================
    // CRM Requisites (for duplicate detection by INN/UNP)
    // =========================================================================

    public function getRequisites($entityTypeId, $entityId)
    {
        return $this->fetchAll('crm.requisite.list', [
            'filter' => [
                'ENTITY_TYPE_ID' => (int)$entityTypeId,
                'ENTITY_ID' => (int)$entityId,
            ],
        ]);
    }

    public function addRequisite($fields)
    {
        $result = $this->request('crm.requisite.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function getBankDetails($requisiteId)
    {
        return $this->fetchAll('crm.requisite.bankdetail.list', [
            'filter' => ['ENTITY_ID' => (int)$requisiteId],
        ]);
    }

    /**
     * Get addresses attached to a requisite via crm.address.list.
     * Requisite addresses have ENTITY_TYPE_ID=8 (Requisite), ENTITY_ID=requisite_id.
     */
    public function getRequisiteAddresses($requisiteId)
    {
        return $this->fetchAll('crm.address.list', [
            'filter' => [
                'ENTITY_TYPE_ID' => 8, // Requisite
                'ENTITY_ID' => (int)$requisiteId,
            ],
        ]);
    }

    public function addBankDetail($fields)
    {
        $result = $this->request('crm.requisite.bankdetail.add', ['fields' => $fields]);
        return $result['result'] ?? null;
    }

    public function findRequisiteByInn($inn)
    {
        $result = $this->request('crm.requisite.list', [
            'filter' => ['RQ_INN' => $inn],
            'select' => ['ID', 'ENTITY_TYPE_ID', 'ENTITY_ID'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    // =========================================================================
    // CRM multifield (phone, email) helpers
    // =========================================================================

    public function findCompanyByPhone($phone)
    {
        $result = $this->request('crm.company.list', [
            'filter' => ['PHONE' => $phone],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findCompanyByEmail($email)
    {
        $result = $this->request('crm.company.list', [
            'filter' => ['EMAIL' => $email],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findContactByPhone($phone)
    {
        $result = $this->request('crm.contact.list', [
            'filter' => ['PHONE' => $phone],
            'select' => ['ID', 'NAME', 'LAST_NAME'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findContactByEmail($email)
    {
        $result = $this->request('crm.contact.list', [
            'filter' => ['EMAIL' => $email],
            'select' => ['ID', 'NAME', 'LAST_NAME'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findContactByName($name, $lastName)
    {
        $result = $this->request('crm.contact.list', [
            'filter' => ['NAME' => $name, 'LAST_NAME' => $lastName],
            'select' => ['ID', 'NAME', 'LAST_NAME'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findDealByTitle($title)
    {
        $result = $this->request('crm.deal.list', [
            'filter' => ['TITLE' => $title],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findLeadByTitle($title)
    {
        $result = $this->request('crm.lead.list', [
            'filter' => ['TITLE' => $title],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findLeadByPhone($phone)
    {
        $result = $this->request('crm.lead.list', [
            'filter' => ['PHONE' => $phone],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }

    public function findLeadByEmail($email)
    {
        $result = $this->request('crm.lead.list', [
            'filter' => ['EMAIL' => $email],
            'select' => ['ID', 'TITLE'],
        ]);
        $items = $result['result'] ?? [];
        return !empty($items) ? $items[0] : null;
    }
}
