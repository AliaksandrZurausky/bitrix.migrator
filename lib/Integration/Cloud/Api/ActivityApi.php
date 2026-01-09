<?php

namespace BitrixMigrator\Integration\Cloud\Api;

use BitrixMigrator\Integration\Cloud\RestClient;

final class ActivityApi
{
    private RestClient $client;

    public function __construct(RestClient $client)
    {
        $this->client = $client;
    }

    public function getList(array $filter = [], int $start = 0): array
    {
        return $this->client->call('crm.activity.list', [
            'filter' => $filter,
            'select' => ['*'],
            'start' => $start,
        ]);
    }
}
