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
            throw new \Exception('HTTP error: ' . $httpCode);
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
     * Get entity count (uses 'total' from first request)
     */
    public function getCount($method, $params = [])
    {
        $params['start'] = 0;
        $result = $this->request($method, $params);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Fetch all entities with pagination
     */
    public function fetchAll($method, $params = [])
    {
        $items = [];
        $next = 0;

        do {
            $params['start'] = $next;
            $result = $this->request($method, $params);

            if (isset($result['result']) && is_array($result['result'])) {
                $items = array_merge($items, $result['result']);
            }

            $next = $result['next'] ?? null;
            
            if ($next !== null) {
                usleep(600000); // 0.6s delay between paginated requests (~1.6 req/s)
            }
        } while ($next !== null);

        return $items;
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
        return $this->getCount('user.get');
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
        return $this->getCount('tasks.task.list');
    }

    /**
     * Get all users (no API-side filter — filter active/inactive in PHP after).
     * user.get returns all user fields; field selection is not supported.
     */
    public function getUsers()
    {
        return $this->fetchAll('user.get', []);
    }

    /**
     * Get all companies
     */
    public function getCompanies($select = ['ID', 'TITLE'])
    {
        return $this->fetchAll('crm.company.list', ['select' => $select]);
    }

    /**
     * Get all contacts
     */
    public function getContacts($select = ['ID', 'NAME', 'LAST_NAME'])
    {
        return $this->fetchAll('crm.contact.list', ['select' => $select]);
    }

    /**
     * Get all deals
     */
    public function getDeals($select = ['ID', 'TITLE'])
    {
        return $this->fetchAll('crm.deal.list', ['select' => $select]);
    }

    /**
     * Get all leads
     */
    public function getLeads($select = ['ID', 'TITLE'])
    {
        return $this->fetchAll('crm.lead.list', ['select' => $select]);
    }

    /**
     * Get all tasks
     */
    public function getTasks($select = ['ID', 'TITLE'])
    {
        return $this->fetchAll('tasks.task.list', ['select' => $select]);
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
     * Add task comment
     */
    public function addTaskComment($taskId, $fields)
    {
        return $this->request('task.commentitem.add', ['TASKID' => (int)$taskId, 'FIELDS' => $fields]);
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
            'C'  => ['crm.company.list', 'TITLE'],
            'D'  => ['crm.deal.list',    'TITLE'],
            'L'  => ['crm.lead.list',    'TITLE'],
            'CO' => ['crm.contact.list',  null],
        ];

        $config = $methodMap[$entityType] ?? null;
        if (!$config) return null;

        $method    = $config[0];
        $titleField = $config[1];

        if ($entityType === 'CO') {
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
            'C'  => 'crm.company.get',
            'D'  => 'crm.deal.get',
            'L'  => 'crm.lead.get',
            'CO' => 'crm.contact.get',
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

    public function addStatus($fields)
    {
        $result = $this->request('crm.status.add', ['fields' => $fields]);
        return $result['result'] ?? null;
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

    // =========================================================================
    // Smart Processes
    // =========================================================================

    public function addSmartProcessType($fields)
    {
        $result = $this->request('crm.type.add', ['fields' => $fields]);
        return $result['result']['type'] ?? $result['result'] ?? null;
    }

    public function getSmartProcessItems($entityTypeId, $select = ['*', 'uf_*'])
    {
        return $this->fetchAll('crm.item.list', [
            'entityTypeId' => (int)$entityTypeId,
            'select' => $select,
        ]);
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
        return $this->fetchAll('crm.timeline.comment.list', [
            'filter' => [
                'ENTITY_TYPE_ID' => (int)$entityTypeId,
                'ENTITY_ID' => (int)$entityId,
            ],
        ]);
    }

    public function addTimelineComment($fields)
    {
        $result = $this->request('crm.timeline.comment.add', ['fields' => $fields]);
        return $result['result'] ?? null;
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
