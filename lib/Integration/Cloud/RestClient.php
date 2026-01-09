<?php

namespace BitrixMigrator\Integration\Cloud;

use Bitrix\Main\Config\Option;
use BitrixMigrator\Config\Module;

final class RestClient
{
    private string $webhookUrl;
    private int $maxRetries = 3;

    public function __construct(?string $webhookUrl = null)
    {
        $this->webhookUrl = $webhookUrl ?? Option::get(Module::ID, 'CLOUD_WEBHOOK_URL', '');

        if (empty($this->webhookUrl)) {
            throw new \RuntimeException('Cloud webhook URL is not configured');
        }
    }

    public function call(string $method, array $params = []): array
    {
        $url = rtrim($this->webhookUrl, '/') . '/' . $method . '.json';

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
        curl_setopt($ch, CURLOPT_TIMEOUT, 30);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error) {
            throw new \RuntimeException('cURL error: ' . $error);
        }

        if ($httpCode !== 200) {
            throw new \RuntimeException('HTTP error: ' . $httpCode);
        }

        $data = json_decode($response, true);
        if (!is_array($data)) {
            throw new \RuntimeException('Invalid JSON response');
        }

        if (isset($data['error'])) {
            throw new \RuntimeException('API error: ' . ($data['error_description'] ?? $data['error']));
        }

        return $data['result'] ?? [];
    }

    public function callWithRetry(string $method, array $params = []): array
    {
        $attempt = 0;
        $lastException = null;

        while ($attempt < $this->maxRetries) {
            try {
                return $this->call($method, $params);
            } catch (\Exception $e) {
                $lastException = $e;
                $attempt++;
                if ($attempt < $this->maxRetries) {
                    sleep(2 * $attempt);
                }
            }
        }

        throw $lastException;
    }

    public function batch(array $calls): array
    {
        $cmd = [];
        foreach ($calls as $key => $call) {
            $cmd[$key] = $call['method'] . '?' . http_build_query($call['params'] ?? []);
        }

        return $this->call('batch', ['cmd' => $cmd]);
    }
}
