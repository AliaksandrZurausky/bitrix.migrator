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
     * Get all users
     */
    public function getUsers($select = ['ID', 'NAME', 'LAST_NAME', 'EMAIL'])
    {
        return $this->fetchAll('user.get', ['select' => $select]);
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
     * Get deal pipeline categories
     */
    public function getDealCategories()
    {
        $result = $this->request('crm.dealcategory.list');
        $categories = $result['result']['categories'] ?? $result['result'] ?? [];
        return is_array($categories) ? $categories : [];
    }

    /**
     * Get stages for a deal pipeline category (id=0 for default)
     */
    public function getDealCategoryStages($categoryId)
    {
        $result = $this->request('crm.dealcategory.stages', ['id' => (int)$categoryId]);
        return is_array($result['result']) ? $result['result'] : [];
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
     * Get workgroups count
     */
    public function getWorkgroupsCount()
    {
        return $this->getCount('sonet_group.get');
    }
}
