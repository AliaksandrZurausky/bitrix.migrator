<?php

namespace BitrixMigrator\Service\Import;

use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Integration\Cloud\Api\TimelineCommentApi;
use BitrixMigrator\Integration\Cloud\Api\ActivityApi;
use BitrixMigrator\Writer\Box\TimelineWriter;
use BitrixMigrator\Repository\Hl\LogRepository;

final class TimelineImporter
{
    private RestClient $cloudClient;
    private TimelineWriter $timelineWriter;
    private LogRepository $logRepo;

    public function __construct(
        RestClient $cloudClient,
        TimelineWriter $timelineWriter,
        LogRepository $logRepo
    ) {
        $this->cloudClient = $cloudClient;
        $this->timelineWriter = $timelineWriter;
        $this->logRepo = $logRepo;
    }

    public function import(array $task): void
    {
        $entityType = $task['UF_ENTITY_TYPE'];
        $cloudId = $task['UF_CLOUD_ID'];
        $boxId = $task['UF_BOX_ID'];

        $entityTypeId = $this->timelineWriter->getEntityTypeIdByName($entityType);

        // Импорт комментариев
        $this->importComments($entityTypeId, $cloudId, $boxId);

        // Импорт активностей (звонки, письма и т.д.)
        $this->importActivities($entityTypeId, $cloudId, $boxId);

        $this->logRepo->info('Timeline migrated successfully', 'TimelineImporter', [
            'entity' => $entityType,
            'cloud_id' => $cloudId,
            'box_id' => $boxId,
        ]);
    }

    private function importComments(int $entityTypeId, string $cloudId, int $boxId): void
    {
        $commentApi = new TimelineCommentApi($this->cloudClient);

        try {
            $comments = $commentApi->getList([
                'ENTITY_ID' => $cloudId,
                'ENTITY_TYPE' => $this->getCloudEntityTypeName($entityTypeId),
            ]);

            foreach ($comments as $comment) {
                try {
                    $this->timelineWriter->createComment(
                        $entityTypeId,
                        $boxId,
                        $comment['COMMENT'] ?? '',
                        $comment['AUTHOR_ID'] ?? 1,
                        null
                    );
                } catch (\Exception $e) {
                    $this->logRepo->warning('Failed to create comment', 'TimelineImporter', [
                        'error' => $e->getMessage(),
                        'comment_id' => $comment['ID'] ?? null,
                    ]);
                }
            }
        } catch (\Exception $e) {
            $this->logRepo->warning('Failed to fetch comments', 'TimelineImporter', [
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function importActivities(int $entityTypeId, string $cloudId, int $boxId): void
    {
        $activityApi = new ActivityApi($this->cloudClient);

        try {
            $activities = $activityApi->getList([
                'OWNER_ID' => $cloudId,
                'OWNER_TYPE_ID' => $entityTypeId,
            ]);

            foreach ($activities as $activity) {
                try {
                    $this->timelineWriter->createActivity(
                        $entityTypeId,
                        $boxId,
                        $activity
                    );
                } catch (\Exception $e) {
                    $this->logRepo->warning('Failed to create activity', 'TimelineImporter', [
                        'error' => $e->getMessage(),
                        'activity_id' => $activity['ID'] ?? null,
                    ]);
                }
            }
        } catch (\Exception $e) {
            $this->logRepo->warning('Failed to fetch activities', 'TimelineImporter', [
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function getCloudEntityTypeName(int $entityTypeId): string
    {
        $map = [
            \CCrmOwnerType::Deal => 'DEAL',
            \CCrmOwnerType::Contact => 'CONTACT',
            \CCrmOwnerType::Company => 'COMPANY',
            \CCrmOwnerType::Lead => 'LEAD',
        ];

        return $map[$entityTypeId] ?? '';
    }
}
