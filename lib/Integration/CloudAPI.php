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
     * Make API request
     */
    private function request($method, $params = [])
    {
        $url = $this->webhookUrl . '/' . $method . '.json';

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
        curl_setopt($ch, CURLOPT_TIMEOUT, 60);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);

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
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \Exception('JSON decode error: ' . json_last_error_msg());
        }

        if (isset($data['error'])) {
            throw new \Exception($data['error_description'] ?? 'API error');
        }

        return $data['result'] ?? $data;
    }

    /**
     * Get users count
     */
    public function getUsersCount()
    {
        $result = $this->request('user.get', ['start' => -1]);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Get companies count
     */
    public function getCompaniesCount()
    {
        $result = $this->request('crm.company.list', ['start' => -1]);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Get contacts count
     */
    public function getContactsCount()
    {
        $result = $this->request('crm.contact.list', ['start' => -1]);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Get deals count
     */
    public function getDealsCount()
    {
        $result = $this->request('crm.deal.list', ['start' => -1]);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Get leads count
     */
    public function getLeadsCount()
    {
        $result = $this->request('crm.lead.list', ['start' => -1]);
        return (int)($result['total'] ?? 0);
    }

    /**
     * Get tasks count
     */
    public function getTasksCount()
    {
        $result = $this->request('tasks.task.list', ['start' => -1]);
        return (int)($result['total'] ?? 0);
    }
}
