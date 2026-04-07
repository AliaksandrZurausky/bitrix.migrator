<?php

namespace BitrixMigrator\Service;

use BitrixMigrator\Integration\CloudAPI;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;

class DryRunService
{
    const MODULE_ID = 'bitrix_migrator';

    /**
     * Full portal analysis: departments, users, CRM, tasks, workgroups, pipelines, custom fields.
     * Results saved to Options for fast retrieval.
     */
    public static function analyze(): array
    {
        if (!Loader::includeModule('highloadblock')) {
            throw new \Exception('Module highloadblock is required');
        }

        $cloudWebhookUrl = Option::get(self::MODULE_ID, 'cloud_webhook_url', '');
        if (empty($cloudWebhookUrl)) {
            throw new \Exception('Cloud webhook URL not configured');
        }

        $cloudAPI = new CloudAPI($cloudWebhookUrl);

        $boxWebhookUrl = Option::get(self::MODULE_ID, 'box_webhook_url', '');
        $boxAPI = !empty($boxWebhookUrl) ? new CloudAPI($boxWebhookUrl) : null;

        $data = ['timestamp' => time()];

        // --- 1. Departments ---
        try {
            $cloudDepts = $cloudAPI->getDepartments();
            $data['departments'] = [
                'list'      => $cloudDepts,
                'count'     => count($cloudDepts),
                'max_depth' => self::calcMaxDepth($cloudDepts),
            ];

            if ($boxAPI) {
                $boxDepts    = $boxAPI->getDepartments();
                $boxNames    = array_column($boxDepts, 'NAME');
                $conflicting = array_filter($cloudDepts, static function ($d) use ($boxNames) {
                    return in_array($d['NAME'], $boxNames, true);
                });
                $data['departments']['box_count']            = count($boxDepts);
                $data['departments']['name_conflicts_count'] = count($conflicting);
            }
        } catch (\Exception $e) {
            $data['departments'] = ['error' => $e->getMessage(), 'count' => 0, 'list' => []];
        }

        // --- 2. Users ---
        try {
            $allCloudUsers = $cloudAPI->getUsers();
            $activeUsers   = array_values(array_filter($allCloudUsers, static function ($u) {
                return ($u['ACTIVE'] ?? '') === 'Y' || $u['ACTIVE'] === true;
            }));

            // Compact list for department modal (minimal fields only).
            $compactActive = array_map(static function ($u) {
                $deptIds = $u['UF_DEPARTMENT'] ?? [];
                if (!is_array($deptIds)) {
                    $deptIds = [$deptIds];
                }
                return [
                    'ID'             => (int)($u['ID'] ?? 0),
                    'NAME'           => $u['NAME'] ?? '',
                    'LAST_NAME'      => $u['LAST_NAME'] ?? '',
                    'SECOND_NAME'    => $u['SECOND_NAME'] ?? '',
                    'EMAIL'          => $u['EMAIL'] ?? '',
                    'WORK_POSITION'  => $u['WORK_POSITION'] ?? '',
                    'WORK_PHONE'     => $u['WORK_PHONE'] ?? '',
                    'PERSONAL_PHOTO' => $u['PERSONAL_PHOTO'] ?? '',
                    'UF_DEPARTMENT'  => array_values(array_map('intval', $deptIds)),
                ];
            }, $activeUsers);

            $data['users'] = [
                'cloud_count'        => count($allCloudUsers),
                'cloud_active_count' => count($activeUsers),
                'all_active_list'    => $compactActive,
            ];

            if ($boxAPI) {
                $allBoxUsers    = $boxAPI->getUsers();
                $boxActiveUsers = array_filter($allBoxUsers, static function ($u) {
                    return ($u['ACTIVE'] ?? '') === 'Y' || $u['ACTIVE'] === true;
                });
                $boxEmails = array_filter(array_map(static function ($u) {
                    return strtolower(trim($u['EMAIL'] ?? ''));
                }, $boxActiveUsers));

                $newUsers     = array_values(array_filter($activeUsers, static function ($u) use ($boxEmails) {
                    return !in_array(strtolower(trim($u['EMAIL'] ?? '')), $boxEmails, true);
                }));
                $matchedUsers = array_values(array_filter($activeUsers, static function ($u) use ($boxEmails) {
                    return in_array(strtolower(trim($u['EMAIL'] ?? '')), $boxEmails, true);
                }));

                $data['users']['box_count']        = count($allBoxUsers);
                $data['users']['box_active_count']  = count($boxActiveUsers);
                $data['users']['new_count']         = count($newUsers);
                $data['users']['matched_count']     = count($matchedUsers);
                $data['users']['new_list']          = $newUsers;
            }

            // Build department name map for UI
            if (!empty($data['departments']['list'])) {
                $deptNameMap = [];
                foreach ($data['departments']['list'] as $dept) {
                    $deptNameMap[(string)$dept['ID']] = $dept['NAME'] ?? '';
                }
                $data['users']['dept_name_map'] = $deptNameMap;
            }
        } catch (\Exception $e) {
            $data['users'] = ['error' => $e->getMessage(), 'cloud_count' => 0, 'cloud_active_count' => 0];
        }

        // --- 3. CRM counts ---
        try {
            $companiesTotal   = $cloudAPI->getCount('crm.company.list');
            // "My Company" system entries are included in the total; subtract them.
            $myCompaniesCount = $cloudAPI->getCount('crm.company.list', ['filter' => ['IS_MY_COMPANY' => 'Y']]);
            $data['crm'] = [
                'companies'          => $companiesTotal,
                'companies_my'       => $myCompaniesCount,
                'companies_regular'  => max(0, $companiesTotal - $myCompaniesCount),
                'contacts'           => $cloudAPI->getCount('crm.contact.list'),
                'deals'              => $cloudAPI->getCount('crm.deal.list'),
                'leads'              => $cloudAPI->getCount('crm.lead.list'),
            ];
        } catch (\Exception $e) {
            $data['crm'] = ['error' => $e->getMessage()];
        }

        // --- 4. Tasks ---
        try {
            $data['tasks'] = ['count' => $cloudAPI->getCount('tasks.task.list')];
        } catch (\Exception $e) {
            $data['tasks'] = ['error' => $e->getMessage(), 'count' => 0];
        }

        // --- 5. Workgroups ---
        try {
            $wgList = $cloudAPI->getWorkgroups();
            $wgListMapped = array_map(static function ($wg) {
                return [
                    'ID'          => $wg['ID'] ?? 0,
                    'NAME'        => $wg['NAME'] ?? '',
                    'DESCRIPTION' => $wg['DESCRIPTION'] ?? '',
                    'ACTIVE'      => $wg['ACTIVE'] ?? 'Y',
                    'OPENED'      => $wg['OPENED'] ?? 'N',
                ];
            }, $wgList);

            $data['workgroups'] = [
                'count' => count($wgListMapped),
                'list'  => $wgListMapped,
            ];

            // Match workgroups by name with box
            if ($boxAPI) {
                try {
                    $boxWgList = $boxAPI->getWorkgroups();
                    $boxWgNames = array_map(static function ($wg) {
                        return mb_strtolower(trim($wg['NAME'] ?? ''));
                    }, $boxWgList);

                    $matched = [];
                    $newWg   = [];
                    foreach ($wgListMapped as $wg) {
                        $nameLC = mb_strtolower(trim($wg['NAME']));
                        if (in_array($nameLC, $boxWgNames, true)) {
                            $matched[] = $wg;
                        } else {
                            $newWg[] = $wg;
                        }
                    }

                    $data['workgroups']['box_count']     = count($boxWgList);
                    $data['workgroups']['matched_count']  = count($matched);
                    $data['workgroups']['matched_list']   = $matched;
                    $data['workgroups']['new_count']      = count($newWg);
                    $data['workgroups']['new_list']       = $newWg;
                } catch (\Exception $e) {
                    // box workgroups not critical
                }
            }
        } catch (\Exception $e) {
            $data['workgroups'] = ['error' => $e->getMessage(), 'count' => 0, 'list' => []];
        }

        // --- 6. Deal pipelines + stages ---
        // One crm.status.list call fetches all statuses; we group by ENTITY_ID in PHP.
        try {
            $categories   = $cloudAPI->getDealCategories();
            $stagesByKey  = $cloudAPI->getAllDealStagesGrouped();

            $defaultStages = $stagesByKey['DEAL_STAGE'] ?? [];

            // Get default pipeline name from API
            $defaultName = 'Общая';
            try {
                $defaultCat = $cloudAPI->getDealCategoryById(0);
                if (!empty($defaultCat['NAME'])) {
                    $defaultName = $defaultCat['NAME'];
                }
            } catch (\Exception $e) {}

            $pipelines     = [[
                'id'           => 0,
                'name'         => $defaultName,
                'stages'       => $defaultStages,
                'stages_count' => count($defaultStages),
            ]];

            foreach ($categories as $cat) {
                $catId       = (int)($cat['ID'] ?? 0);
                $key         = 'DEAL_STAGE_' . $catId;
                $stages      = $stagesByKey[$key] ?? [];
                $pipelines[] = [
                    'id'           => $catId,
                    'name'         => $cat['NAME'] ?? '',
                    'stages'       => $stages,
                    'stages_count' => count($stages),
                ];
            }

            $data['pipelines'] = [
                'list'  => $pipelines,
                'count' => count($pipelines),
            ];
        } catch (\Exception $e) {
            $data['pipelines'] = ['error' => $e->getMessage(), 'count' => 0, 'list' => []];
        }

        // --- 7. CRM custom fields ---
        try {
            $fieldMethods = [
                'deal'    => 'crm.deal.fields',
                'contact' => 'crm.contact.fields',
                'company' => 'crm.company.fields',
                'lead'    => 'crm.lead.fields',
            ];
            $customFields = [];
            foreach ($fieldMethods as $entity => $method) {
                $fields = $cloudAPI->getFields($method);
                $custom = array_filter($fields, static function ($key) {
                    return strncmp($key, 'UF_', 3) === 0;
                }, ARRAY_FILTER_USE_KEY);
                $customFields[$entity] = array_map(static function ($f) {
                    return [
                        'title' => $f['formLabel'] ?? $f['title'] ?? '',
                        'type'  => $f['type'] ?? '',
                    ];
                }, $custom);
            }
            $data['crm_custom_fields'] = $customFields;
        } catch (\Exception $e) {
            $data['crm_custom_fields'] = ['error' => $e->getMessage()];
        }

        // --- 8. Smart processes ---
        try {
            $types         = $cloudAPI->getSmartProcessTypes();
            $smartProcesses = [];
            foreach ($types as $type) {
                $entityTypeId     = (int)($type['entityTypeId'] ?? $type['id'] ?? 0);
                $count            = 0;
                if ($entityTypeId > 0) {
                    try {
                        $count = $cloudAPI->getSmartProcessCount($entityTypeId);
                    } catch (\Exception $e) {
                        // count not critical
                    }
                }
                // Fetch custom fields for this smart process
                $spCustomFields = [];
                if ($entityTypeId > 0) {
                    try {
                        $allSpFields = $cloudAPI->getSmartProcessFields($entityTypeId);
                        $customOnly = array_filter($allSpFields, static function ($key) {
                            return strncmp($key, 'uf', 2) === 0 || strncmp($key, 'UF_', 3) === 0;
                        }, ARRAY_FILTER_USE_KEY);
                        $spCustomFields = array_map(static function ($f) {
                            return [
                                'title' => $f['formLabel'] ?? $f['title'] ?? '',
                                'type'  => $f['type'] ?? '',
                            ];
                        }, $customOnly);
                    } catch (\Exception $e) {
                        // fields not critical
                    }
                }

                $smartProcesses[] = [
                    'id'            => $type['id']    ?? 0,
                    'entityTypeId'  => $entityTypeId,
                    'title'         => $type['title'] ?? '',
                    'count'         => $count,
                    'custom_fields' => $spCustomFields,
                ];
            }
            $data['smart_processes'] = [
                'list'  => $smartProcesses,
                'count' => count($smartProcesses),
            ];
        } catch (\Exception $e) {
            $data['smart_processes'] = ['error' => $e->getMessage(), 'count' => 0, 'list' => []];
        }

        // --- Save results ---
        Option::set(self::MODULE_ID, 'dryrun_result_json',   json_encode($data, JSON_UNESCAPED_UNICODE));
        Option::set(self::MODULE_ID, 'dryrun_status',        'completed');
        Option::set(self::MODULE_ID, 'dryrun_completed_at',  (string)time());
        Option::set(self::MODULE_ID, 'dryrun_error',         '');

        return $data;
    }

    /**
     * Calculate maximum hierarchy depth for a flat list of departments.
     */
    private static function calcMaxDepth(array $departments): int
    {
        if (empty($departments)) {
            return 0;
        }

        $idMap    = [];
        foreach ($departments as $dept) {
            $idMap[(string)$dept['ID']] = $dept;
        }

        $maxDepth = 0;
        foreach ($departments as $dept) {
            $depth   = 1;
            $current = $dept;
            $visited = [];
            while (
                !empty($current['PARENT'])
                && isset($idMap[(string)$current['PARENT']])
                && !in_array((string)$current['ID'], $visited, true)
            ) {
                $visited[] = (string)$current['ID'];
                $current   = $idMap[(string)$current['PARENT']];
                $depth++;
            }
            if ($depth > $maxDepth) {
                $maxDepth = $depth;
            }
        }

        return $maxDepth;
    }
}
