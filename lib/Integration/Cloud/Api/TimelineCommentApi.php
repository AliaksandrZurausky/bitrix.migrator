<?php

namespace BitrixMigrator\Integration\Cloud\Api;

use BitrixMigrator\Integration\Cloud\RestClient;

final class TimelineCommentApi
{
    private RestClient $client;

    public function __construct(RestClient $client)
    {
        $this->client = $client;
    }

    public function getList(array $filter = []): array
    {
        return $this->client->call('crm.timeline.comment.list', [
            'filter' => $filter,
            'order' => ['ID' => 'ASC'],
        ]);
    }
}
