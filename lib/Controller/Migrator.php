<?php

namespace Bitrix\Migrator\Controller;

use Bitrix\Main\Engine\Controller;
use Bitrix\Main\Engine\ActionFilter;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Error;

class Migrator extends Controller
{
    /**
     * Configure action filters
     */
    public function configureActions()
    {
        return [
            'saveConnection' => [
                'prefilters' => [
                    new ActionFilter\Authentication(),
                    new ActionFilter\HttpMethod([ActionFilter\HttpMethod::METHOD_POST]),
                    new ActionFilter\Csrf(),
                ],
            ],
            'checkConnection' => [
                'prefilters' => [
                    new ActionFilter\Authentication(),
                    new ActionFilter\HttpMethod([ActionFilter\HttpMethod::METHOD_POST]),
                    new ActionFilter\Csrf(),
                ],
            ],
            'startDryRun' => [
                'prefilters' => [
                    new ActionFilter\Authentication(),
                    new ActionFilter\HttpMethod([ActionFilter\HttpMethod::METHOD_POST]),
                    new ActionFilter\Csrf(),
                ],
            ],
        ];
    }

    /**
     * Save connection settings
     */
    public function saveConnectionAction($webhookUrl)
    {
        $webhookUrl = trim($webhookUrl);

        if (empty($webhookUrl)) {
            $this->addError(new Error('Empty webhook URL'));
            return ['success' => false, 'error' => 'Empty webhook URL'];
        }

        // Validate URL
        if (!filter_var($webhookUrl, FILTER_VALIDATE_URL)) {
            $this->addError(new Error('Invalid URL format'));
            return ['success' => false, 'error' => 'Invalid URL format'];
        }

        // Validate that it's a Bitrix24 webhook URL
        if (!preg_match('#^https?://[^/]+/rest/\d+/[a-zA-Z0-9]+/?$#', $webhookUrl)) {
            $this->addError(new Error('Invalid webhook format. Expected: https://portal.bitrix24.ru/rest/1/abc123/'));
            return ['success' => false, 'error' => 'Invalid webhook format'];
        }

        // Save to options
        Option::set('bitrix_migrator', 'webhook_url', rtrim($webhookUrl, '/'));

        return ['success' => true];
    }

    /**
     * Check connection to cloud Bitrix24
     */
    public function checkConnectionAction($webhookUrl)
    {
        $webhookUrl = trim($webhookUrl);

        if (empty($webhookUrl)) {
            return ['success' => false, 'error' => 'Empty webhook URL'];
        }

        // Build REST API URL
        $apiUrl = rtrim($webhookUrl, '/') . '/user.current.json';

        try {
            // Make request
            $response = $this->makeRequest($apiUrl);

            if (isset($response['result']) && isset($response['result']['ID'])) {
                // Connection successful
                Option::set('bitrix_migrator', 'connection_status', 'success');
                return ['success' => true, 'user' => $response['result']];
            } else {
                // Connection failed
                Option::set('bitrix_migrator', 'connection_status', 'error');
                $error = $response['error_description'] ?? 'Unknown error';
                return ['success' => false, 'error' => $error];
            }
        } catch (\Exception $e) {
            Option::set('bitrix_migrator', 'connection_status', 'error');
            return ['success' => false, 'error' => $e->getMessage()];
        }
    }

    /**
     * Start dry run process
     */
    public function startDryRunAction()
    {
        $webhookUrl = Option::get('bitrix_migrator', 'webhook_url', '');

        if (empty($webhookUrl)) {
            return ['success' => false, 'error' => 'Connection not configured'];
        }

        // TODO: Implement dry run logic
        // For now, just return success
        return ['success' => true, 'message' => 'Dry run started (stub)'];
    }

    /**
     * Make HTTP request to REST API
     */
    private function makeRequest($url)
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 30);
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

        return $data;
    }
}
