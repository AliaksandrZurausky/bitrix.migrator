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
                usleep(320000); // 0.32s delay between requests
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
}
