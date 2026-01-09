<?php

namespace BitrixMigrator\Integration;

class CloudAPI
{
    private $webhookUrl;

    public function __construct($webhookUrl = null)
    {
        $this->webhookUrl = $webhookUrl;
    }

    public function testConnection()
    {
        // TODO: Проверить соединение с облаком
        return true;
    }
}
