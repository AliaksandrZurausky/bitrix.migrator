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
     * Make API request with JSON body
     */
    private function request($method, $params = [])
    {
        $url = $this->webhookUrl . '/' . $method . '.json';

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_POST           => true,
            CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
            CURLOPT_POSTFIELDS     => json_encode($params),
            CURLOPT_TIMEOUT        => 60,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error) {
            throw new \Exception('cURL error: ' . $error);
        }

        if ($httpCode !== 200) {
            throw new \Exception('HTTP error: ' . $httpCode);
        }

        $data = json_decode($response, true);
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
}
