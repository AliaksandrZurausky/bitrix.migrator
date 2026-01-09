<?php

namespace BitrixMigrator\Integration\Cloud\Api;

use BitrixMigrator\Integration\Cloud\RestClient;

final class DealApi
{
    private RestClient $client;

    public function __construct(RestClient $client)
    {
        $this->client = $client;
    }

    public function getList(array $filter = [], int $start = 0): array
    {
        return $this->client->call('crm.deal.list', [
            'filter' => $filter,
            'select' => ['*', 'UF_*'],
            'start' => $start,
        ]);
    }

    public function get(int $id): array
    {
        return $this->client->call('crm.deal.get', ['id' => $id]);
    }
}
