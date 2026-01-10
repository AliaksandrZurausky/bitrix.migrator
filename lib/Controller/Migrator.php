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
    public function saveConnectionAction($cloudUrl, $cloudWebhook)
    {
        $cloudUrl = trim($cloudUrl);
        $cloudWebhook = trim($cloudWebhook);

        if (empty($cloudUrl) || empty($cloudWebhook)) {
            $this->addError(new Error('Empty URL or webhook'));
            return ['success' => false, 'error' => 'Empty URL or webhook'];
        }

        // Validate URL
        if (!filter_var($cloudUrl, FILTER_VALIDATE_URL)) {
            $this->addError(new Error('Invalid URL format'));
            return ['success' => false, 'error' => 'Invalid URL format'];
        }

        // Save to options
        Option::set('bitrix_migrator', 'cloud_url', $cloudUrl);
        Option::set('bitrix_migrator', 'cloud_webhook', $cloudWebhook);

        return ['success' => true];
    }

    /**
     * Check connection to cloud Bitrix24
     */
    public function checkConnectionAction($cloudUrl, $cloudWebhook)
    {
        $cloudUrl = trim($cloudUrl);
        $cloudWebhook = trim($cloudWebhook);

        if (empty($cloudUrl) || empty($cloudWebhook)) {
            return ['success' => false, 'error' => 'Empty URL or webhook'];
        }

        // Build REST API URL
        $apiUrl = rtrim($cloudUrl, '/') . '/rest/' . $cloudWebhook . '/user.current.json';

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
        $cloudUrl = Option::get('bitrix_migrator', 'cloud_url', '');
        $cloudWebhook = Option::get('bitrix_migrator', 'cloud_webhook', '');

        if (empty($cloudUrl) || empty($cloudWebhook)) {
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
