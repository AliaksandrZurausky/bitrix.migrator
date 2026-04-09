<?php

namespace BitrixMigrator\Service;

use Bitrix\Main\Loader;

/**
 * Direct D7 operations on box server.
 * Bypasses REST API — works via Bitrix core classes.
 * Available only when CLI runs on the box server.
 *
 * Implemented via D7: Users, Companies, Contacts, Deals, Leads.
 * Still via REST: Departments, Pipelines, CRM fields, Workgroups, Smart Processes, Cleanup.
 */
class BoxD7Service
{
    private static $employeeGroupId = null;

    private static function getEmployeeGroupId(): int
    {
        if (self::$employeeGroupId !== null) return self::$employeeGroupId;

        // Try to find group by string_id 'EMPLOYEES' (standard Bitrix24 group)
        $res = \CGroup::GetList('', '', ['STRING_ID' => 'EMPLOYEES']);
        if ($row = $res->Fetch()) {
            self::$employeeGroupId = (int)$row['ID'];
            return self::$employeeGroupId;
        }

        // Fallback: search by name
        $res = \CGroup::GetList('', '', ['NAME' => 'Сотрудники']);
        if ($row = $res->Fetch()) {
            self::$employeeGroupId = (int)$row['ID'];
            return self::$employeeGroupId;
        }

        self::$employeeGroupId = 0;
        return 0;
    }

    /**
     * Flush Bitrix internal caches that accumulate during bulk CRM operations.
     * Without this, CUserTypeManager holds all 700+ UF field defs in memory
     * and SearchContentBuilder reloads them for every entity — causes OOM.
     */
    public static function flushCrmCaches(): void
    {
        // CUserTypeManager caches all UF field definitions per entity
        if (isset($GLOBALS['USER_FIELD_MANAGER'])) {
            $GLOBALS['USER_FIELD_MANAGER']->CleanCache();
        }
        // Bitrix managed cache accumulates tag data
        try {
            $app = \Bitrix\Main\Application::getInstance();
            $app->getManagedCache()->cleanAll();
        } catch (\Throwable $e) {}
    }

    private static function ensureCrmLoaded(): void
    {
        if (!Loader::includeModule('crm')) {
            throw new \Exception('CRM module not loaded');
        }
    }

    // =========================================================================
    // Users
    // =========================================================================

    /**
     * Create user via CUser::Add (no invitation email by default).
     *
     * @param array $fields  User fields (EMAIL, NAME, LAST_NAME, UF_DEPARTMENT, etc.)
     * @param bool  $sendInvite  Whether to send invitation email after creation
     * @return int  New user ID
     * @throws \Exception
     */
    public static function createUser(array $fields, bool $sendInvite = false): int
    {
        $email = trim($fields['EMAIL'] ?? '');
        if (empty($email)) {
            throw new \Exception('EMAIL is required');
        }

        $password = bin2hex(random_bytes(16));

        $userFields = [
            'LOGIN'            => $email,
            'EMAIL'            => $email,
            'PASSWORD'         => $password,
            'CONFIRM_PASSWORD' => $password,
            'NAME'             => $fields['NAME'] ?? '',
            'LAST_NAME'        => $fields['LAST_NAME'] ?? '',
            'SECOND_NAME'      => $fields['SECOND_NAME'] ?? '',
            'WORK_POSITION'    => $fields['WORK_POSITION'] ?? '',
            'PERSONAL_PHONE'   => $fields['PERSONAL_PHONE'] ?? '',
            'PERSONAL_MOBILE'  => $fields['PERSONAL_MOBILE'] ?? '',
            'WORK_PHONE'       => $fields['WORK_PHONE'] ?? '',
            'ACTIVE'           => ($fields['ACTIVE'] ?? 'Y') === 'N' ? 'N' : 'Y',
            'LID'              => (defined('SITE_ID') ? SITE_ID : 's1'),
        ];

        // Backdate registration if cloud date is provided (e.g. DATE_REGISTER from user.get)
        if (!empty($fields['DATE_REGISTER'])) {
            $reg = $fields['DATE_REGISTER'];
            // Convert ISO "2020-03-15T10:00:00+03:00" -> "15.03.2020"
            if (preg_match('/^(\d{4})-(\d{2})-(\d{2})/', $reg, $m)) {
                $userFields['DATE_REGISTER'] = $m[3] . '.' . $m[2] . '.' . $m[1];
            } else {
                $userFields['DATE_REGISTER'] = $reg;
            }
        }

        // UF_DEPARTMENT is required for employee status (vs visitor/extranet).
        // If empty → user appears as extranet. Default to root department [1].
        if (!empty($fields['UF_DEPARTMENT'])) {
            $userFields['UF_DEPARTMENT'] = $fields['UF_DEPARTMENT'];
        } else {
            $userFields['UF_DEPARTMENT'] = [1];
        }
        if (!empty($fields['PERSONAL_GENDER'])) {
            $userFields['PERSONAL_GENDER'] = $fields['PERSONAL_GENDER'];
        }
        if (!empty($fields['PERSONAL_BIRTHDAY'])) {
            // CUser::Add expects date in site format (DD.MM.YYYY), input is YYYY-MM-DD
            $bday = $fields['PERSONAL_BIRTHDAY'];
            if (preg_match('/^(\d{4})-(\d{2})-(\d{2})/', $bday, $m)) {
                $bday = $m[3] . '.' . $m[2] . '.' . $m[1];
            }
            $userFields['PERSONAL_BIRTHDAY'] = $bday;
        }
        if (!empty($fields['PERSONAL_PHOTO'])) {
            $photo = self::downloadPhoto($fields['PERSONAL_PHOTO']);
            if ($photo) {
                $userFields['PERSONAL_PHOTO'] = $photo;
            }
        }

        $user = new \CUser;
        $id = $user->Add($userFields);

        if (!$id) {
            throw new \Exception('CUser::Add error: ' . $user->LAST_ERROR);
        }

        // Add user to "Employees" group — required for portal access
        $employeeGroupId = self::getEmployeeGroupId();
        if ($employeeGroupId > 0) {
            \CUser::SetUserGroup((int)$id, array_merge(
                \CUser::GetUserGroup((int)$id),
                [$employeeGroupId]
            ));
        }

        if ($sendInvite) {
            self::sendInvitation((int)$id, $email);
        }

        return (int)$id;
    }

    /**
     * Update user via CUser::Update.
     */
    public static function updateUser(int $id, array $fields): bool
    {
        $user = new \CUser;
        $result = $user->Update($id, $fields);
        if (!$result) {
            throw new \Exception('CUser::Update error: ' . $user->LAST_ERROR);
        }
        return true;
    }

    /**
     * Disable email notifications for all users on the box.
     * Handles both "simple" and "expert" notification schemes.
     * Returns count of processed users.
     */
    public static function disableEmailNotificationsForAll(): int
    {
        if (!Loader::includeModule('im')) {
            throw new \Exception('IM module not loaded');
        }

        $simpleParams = [
            'notifySchemeSendEmail' => false,
        ];

        $expertParams = [
            'email|bizproc|activity'                => false,
            'email|blog|post'                       => false,
            'email|blog|post_mail'                  => false,
            'email|blog|comment'                    => false,
            'email|blog|mention'                    => false,
            'email|blog|mention_comment'            => false,
            'email|blog|share'                      => false,
            'email|blog|share2users'                => false,
            'email|blog|broadcast_post'             => false,
            'email|blog|grat'                       => false,
            'email|blog|moderate_post'              => false,
            'email|blog|moderate_comment'           => false,
            'email|blog|published_post'             => false,
            'email|blog|published_comment'          => false,
            'email|calendar|invite'                 => false,
            'email|calendar|reminder'               => false,
            'email|calendar|change'                 => false,
            'email|calendar|info'                   => false,
            'email|calendar|event_comment'          => false,
            'email|calendar|delete_location'        => false,
            'email|crm|incoming_email'              => false,
            'email|crm|post'                        => false,
            'email|crm|mention'                     => false,
            'email|crm|webform'                     => false,
            'email|crm|callback'                    => false,
            'email|crm|changeAssignedBy'            => false,
            'email|crm|changeObserver'              => false,
            'email|crm|changeStage'                 => false,
            'email|crm|merge'                       => false,
            'email|crm|other'                       => false,
            'email|crm|pingTodoActivity'            => false,
            'email|disk|files'                      => false,
            'email|disk|deletion'                   => false,
            'email|forum|comment'                   => false,
            'email|im|message'                      => false,
            'email|im|chat'                         => false,
            'email|im|openChat'                     => false,
            'email|im|like'                         => false,
            'email|im|mention'                      => false,
            'email|im|default'                      => false,
            'email|main|rating_vote'                => false,
            'email|main|rating_vote_mentioned'      => false,
            'email|imopenlines|rating_client'       => false,
            'email|imopenlines|rating_head'         => false,
            'email|intranet|security_otp'           => false,
            'email|mail|new_message'                => false,
            'email|photogallery|comment'            => false,
            'email|sender|group_prepared'           => false,
            'email|socialnetwork|invite_group'      => false,
            'email|socialnetwork|invite_group_btn'  => false,
            'email|socialnetwork|inout_group'       => false,
            'email|socialnetwork|moderators_group'  => false,
            'email|socialnetwork|owner_group'       => false,
            'email|socialnetwork|sonet_group_event' => false,
            'email|socialnetwork|inout_user'        => false,
            'email|tasks|comment'                   => false,
            'email|tasks|reminder'                  => false,
            'email|tasks|manage'                    => false,
            'email|tasks|task_assigned'             => false,
            'email|tasks|task_expired_soon'         => false,
            'email|timeman|entry'                   => false,
            'email|timeman|entry_comment'           => false,
            'email|timeman|entry_approve'           => false,
            'email|timeman|report'                  => false,
            'email|timeman|report_comment'          => false,
            'email|timeman|report_approve'          => false,
            'email|vote|voting'                     => false,
            'email|wiki|comment'                    => false,
        ];

        $count = 0;
        $users = \CUser::GetList();
        while ($row = $users->Fetch()) {
            $userId       = (int)$row['ID'];
            $userSettings = \CIMSettings::Get($userId);
            $scheme       = $userSettings['settings']['notifyScheme'] ?? 'simple';

            if ($scheme === 'expert') {
                foreach ($expertParams as $key => $value) {
                    $userSettings['notify'][$key] = $value;
                }
                \CIMSettings::Set('notify', $userSettings['notify'], $userId);
            } else {
                foreach ($simpleParams as $key => $value) {
                    $userSettings['settings'][$key] = $value;
                }
                \CIMSettings::Set('settings', $userSettings['settings'], $userId);
            }

            $count++;
        }

        return $count;
    }

    // =========================================================================
    // Companies
    // =========================================================================

    public static function createCompany(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmCompany(false);
        $id = $obj->Add($fields, false, ['DISABLE_USER_FIELD_CHECK' => true]);
        if (!$id) {
            throw new \Exception('CCrmCompany::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        self::flushCrmCaches();
        return (int)$id;
    }

    public static function updateCompany(int $id, array $fields): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmCompany(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmCompany::Update error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        return true;
    }

    public static function deleteCompany(int $id): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmCompany(false);
        $result = $obj->Delete($id, ['REGISTER_SONET_EVENT' => false]);
        if (!$result) {
            throw new \Exception('CCrmCompany::Delete error for ID=' . $id);
        }
        return true;
    }

    // =========================================================================
    // Contacts
    // =========================================================================

    public static function createContact(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmContact(false);
        $id = $obj->Add($fields, false, ['DISABLE_USER_FIELD_CHECK' => true]);
        if (!$id) {
            throw new \Exception('CCrmContact::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        self::flushCrmCaches();
        return (int)$id;
    }

    public static function updateContact(int $id, array $fields): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmContact(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmContact::Update error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        return true;
    }

    public static function deleteContact(int $id): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmContact(false);
        $result = $obj->Delete($id, ['REGISTER_SONET_EVENT' => false]);
        if (!$result) {
            throw new \Exception('CCrmContact::Delete error for ID=' . $id);
        }
        return true;
    }

    /**
     * Link contact to companies (replaces crm.contact.company.items.set).
     */
    public static function setContactCompanies(int $contactId, array $companyIds): void
    {
        self::ensureCrmLoaded();
        if (empty($companyIds)) return;
        \Bitrix\Crm\Binding\ContactCompanyTable::bindCompanyIDs($contactId, $companyIds);
    }

    // =========================================================================
    // Deals
    // =========================================================================

    public static function createDeal(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmDeal(false);
        $id = $obj->Add($fields, false, ['DISABLE_USER_FIELD_CHECK' => true]);
        if (!$id) {
            throw new \Exception('CCrmDeal::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        self::flushCrmCaches();
        return (int)$id;
    }

    public static function updateDeal(int $id, array $fields): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmDeal(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmDeal::Update error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        return true;
    }

    public static function deleteDeal(int $id): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmDeal(false);
        $result = $obj->Delete($id, ['REGISTER_SONET_EVENT' => false]);
        if (!$result) {
            throw new \Exception('CCrmDeal::Delete error for ID=' . $id);
        }
        return true;
    }

    /**
     * Link deal to contacts (replaces crm.deal.contact.items.set).
     */
    public static function setDealContacts(int $dealId, array $contactIds): void
    {
        self::ensureCrmLoaded();
        if (empty($contactIds)) return;
        \Bitrix\Crm\Binding\DealContactTable::bindContactIDs($dealId, $contactIds);
    }

    // =========================================================================
    // Leads
    // =========================================================================

    public static function createLead(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmLead(false);
        $id = $obj->Add($fields, false, ['DISABLE_USER_FIELD_CHECK' => true]);
        if (!$id) {
            throw new \Exception('CCrmLead::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        self::flushCrmCaches();
        return (int)$id;
    }

    public static function updateLead(int $id, array $fields): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmLead(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmLead::Update error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
        return true;
    }

    public static function deleteLead(int $id): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmLead(false);
        $result = $obj->Delete($id, ['REGISTER_SONET_EVENT' => false]);
        if (!$result) {
            throw new \Exception('CCrmLead::Delete error for ID=' . $id);
        }
        return true;
    }

    // =========================================================================
    // Workgroups (sonet groups) via D7
    // =========================================================================

    private static function ensureSocialnetworkLoaded(): void
    {
        if (!Loader::includeModule('socialnetwork')) {
            throw new \Exception('Socialnetwork module not loaded');
        }
    }

    /**
     * Get ALL workgroups including secret (VISIBLE=N) via D7.
     * REST sonet_group.get does NOT return secret groups unless user is a member.
     */
    public static function getAllWorkgroups(): array
    {
        self::ensureSocialnetworkLoaded();
        return \Bitrix\Socialnetwork\WorkgroupTable::getList([
            'select' => ['ID', 'NAME', 'VISIBLE', 'OPENED', 'OWNER_ID'],
        ])->fetchAll();
    }

    /**
     * Delete workgroup via CSocNetGroup::Delete (no permission checks).
     */
    public static function deleteWorkgroup(int $id): bool
    {
        self::ensureSocialnetworkLoaded();
        $result = \CSocNetGroup::Delete($id);
        if (!$result) {
            throw new \Exception('CSocNetGroup::Delete error for ID=' . $id);
        }
        return true;
    }

    // =========================================================================
    // D7 ID listers + D7 deleters for cleanup (no raw SQL, no REST rate limit)
    // =========================================================================

    /**
     * Generic D7 ID lister via DataManager::getList(['select' => ['ID']]).
     * ORM quotes reserved words and handles dialect differences automatically.
     */
    private static function listIdsViaDataManager(string $dataManagerClass, string $idField = 'ID'): array
    {
        $ids = [];
        $rows = $dataManagerClass::getList(['select' => [$idField]])->fetchAll();
        foreach ($rows as $r) {
            $ids[] = (int)$r[$idField];
        }
        return $ids;
    }

    public static function listCompanyIds(): array
    {
        self::ensureCrmLoaded();
        return self::listIdsViaDataManager(\Bitrix\Crm\CompanyTable::class);
    }

    public static function listContactIds(): array
    {
        self::ensureCrmLoaded();
        return self::listIdsViaDataManager(\Bitrix\Crm\ContactTable::class);
    }

    public static function listDealIds(): array
    {
        self::ensureCrmLoaded();
        return self::listIdsViaDataManager(\Bitrix\Crm\DealTable::class);
    }

    public static function listLeadIds(): array
    {
        self::ensureCrmLoaded();
        return self::listIdsViaDataManager(\Bitrix\Crm\LeadTable::class);
    }

    public static function listTaskIds(): array
    {
        if (!Loader::includeModule('tasks')) {
            throw new \Exception('Tasks module not loaded');
        }
        return self::listIdsViaDataManager(\Bitrix\Tasks\Internals\TaskTable::class);
    }

    public static function listDealCategoryIds(): array
    {
        self::ensureCrmLoaded();
        return self::listIdsViaDataManager(\Bitrix\Crm\Category\Entity\DealCategoryTable::class);
    }

    /**
     * List non-system deal stage IDs via D7 StatusTable::getList().
     * Filter: ENTITY_ID is 'DEAL_STAGE' (default pipeline) or 'DEAL_STAGE_*'
     * (custom categories), and SYSTEM != 'Y'. ORM handles the `SYSTEM`
     * reserved-word quoting automatically.
     */
    public static function listDealStageIds(): array
    {
        self::ensureCrmLoaded();
        $rows = \Bitrix\Crm\StatusTable::getList([
            'select' => ['ID', 'ENTITY_ID', 'SYSTEM'],
        ])->fetchAll();
        $ids = [];
        foreach ($rows as $r) {
            if (($r['SYSTEM'] ?? 'N') === 'Y') continue;
            $eid = (string)($r['ENTITY_ID'] ?? '');
            if ($eid === 'DEAL_STAGE' || strncmp($eid, 'DEAL_STAGE_', 11) === 0) {
                $ids[] = (int)$r['ID'];
            }
        }
        return $ids;
    }

    /**
     * List all dynamic smart process types via D7 TypeTable::getList().
     * Returns rows with an extra `is_system` flag derived from CODE (system
     * types carry codes prefixed with 'BX_' — SmartInvoice='BX_SMART_INVOICE',
     * SmartDocument='BX_SMART_DOCUMENT', SmartB2eDocument='BX_SMART_B2E_DOC').
     *
     * @return array<int, array{row_id:int, entity_type_id:int, code:string, title:string, is_system:bool}>
     */
    public static function listSmartProcessTypes(): array
    {
        self::ensureCrmLoaded();
        $rows = \Bitrix\Crm\Model\Dynamic\TypeTable::getList([
            'select' => ['ID', 'ENTITY_TYPE_ID', 'CODE', 'TITLE'],
        ])->fetchAll();
        $types = [];
        foreach ($rows as $r) {
            $code = (string)($r['CODE'] ?? '');
            $types[] = [
                'row_id'         => (int)$r['ID'],
                'entity_type_id' => (int)$r['ENTITY_TYPE_ID'],
                'code'           => $code,
                'title'          => (string)($r['TITLE'] ?? ''),
                'is_system'      => $code !== '' && strncmp($code, 'BX_', 3) === 0,
            ];
        }
        return $types;
    }

    /**
     * List all item IDs for a given smart process entityTypeId via the CRM
     * Service Container factory (pure D7). Returns empty array if the type
     * is not registered or the factory is not available.
     */
    public static function listSmartProcessItemIds(int $entityTypeId): array
    {
        self::ensureCrmLoaded();
        $factory = \Bitrix\Crm\Service\Container::getInstance()->getFactory($entityTypeId);
        if (!$factory) return [];
        $items = $factory->getItems(['select' => ['ID']]);
        $ids = [];
        foreach ($items as $item) {
            $ids[] = (int)$item->getId();
        }
        return $ids;
    }

    /**
     * Delete deal category (pipeline) via D7 \Bitrix\Crm\Category\DealCategory::delete.
     * Automatically removes associated stages via eraseStages() internals.
     * Throws on dependency failure (category still has deals).
     */
    public static function deleteDealCategory(int $id): bool
    {
        self::ensureCrmLoaded();
        try {
            \Bitrix\Crm\Category\DealCategory::delete($id);
        } catch (\Throwable $e) {
            throw new \Exception('DealCategory::delete error for ID=' . $id . ': ' . $e->getMessage(), 0, $e);
        }
        return true;
    }

    /**
     * Delete a CRM status (deal stage, lead status, etc.) by numeric ID via CCrmStatus.
     */
    public static function deleteCrmStatus(int $id): bool
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmStatus('');
        $result = $obj->Delete($id);
        if (!$result) {
            throw new \Exception('CCrmStatus::Delete error for ID=' . $id);
        }
        return true;
    }

    /**
     * Delete task via CTaskItem::delete (D7-ish, uses admin ID=1 context).
     */
    public static function deleteTask(int $id): bool
    {
        if (!Loader::includeModule('tasks')) {
            throw new \Exception('Tasks module not loaded');
        }
        // CTaskItem requires an executive user id — use admin (1).
        $task = \CTaskItem::getInstance($id, 1);
        $task->delete(['SKIP_NOT_FOUND_ERROR' => true]);
        return true;
    }

    /**
     * Delete a smart process item via the CRM Factory's delete operation.
     */
    public static function deleteSmartProcessItem(int $entityTypeId, int $id): bool
    {
        self::ensureCrmLoaded();
        $factory = \Bitrix\Crm\Service\Container::getInstance()->getFactory($entityTypeId);
        if (!$factory) {
            throw new \Exception("No factory for entityTypeId=$entityTypeId");
        }
        $item = $factory->getItem($id);
        if (!$item) return false; // already gone
        $op = $factory->getDeleteOperation($item);
        $op->disableCheckAccess();
        if (method_exists($op, 'disableBizProc'))     $op->disableBizProc();
        if (method_exists($op, 'disableAutomation'))  $op->disableAutomation();
        $result = $op->launch();
        if (!$result->isSuccess()) {
            throw new \Exception('DeleteOperation failed for entityTypeId=' . $entityTypeId . ' id=' . $id . ': ' . implode('; ', $result->getErrorMessages()));
        }
        return true;
    }

    /**
     * Delete a smart process type (dynamic CRM type) via TypeTable.
     * `$typeRowId` is the row ID in b_crm_type (NOT entityTypeId).
     */
    public static function deleteSmartProcessType(int $typeRowId): bool
    {
        self::ensureCrmLoaded();
        $result = \Bitrix\Crm\Model\Dynamic\TypeTable::delete($typeRowId);
        if (!$result->isSuccess()) {
            throw new \Exception('TypeTable::delete error for row ID=' . $typeRowId . ': ' . implode('; ', $result->getErrorMessages()));
        }
        return true;
    }

    /**
     * Create workgroup via D7 (CSocNetGroup::createGroup).
     * REST sonet_group.create ignores OWNER_ID unless caller is admin.
     * D7 bypasses that restriction.
     *
     * Roles: A=owner, E=moderator, K=member.
     *
     * @param array $fields  NAME, DESCRIPTION, VISIBLE, OPENED, PROJECT, SITE_ID
     * @param int   $ownerId Box user ID who will own the group
     * @return int New group ID
     */
    public static function createWorkgroup(array $fields, int $ownerId): int
    {
        self::ensureSocialnetworkLoaded();

        // SUBJECT_ID is required by CSocNetGroup::createGroup.
        // Use provided value or fall back to first available subject (ID=1).
        $subjectId = (int)($fields['SUBJECT_ID'] ?? 0);
        if ($subjectId <= 0) {
            $subjectRow = \Bitrix\Socialnetwork\WorkgroupSubjectTable::getList([
                'select' => ['ID'],
                'order'  => ['ID' => 'ASC'],
                'limit'  => 1,
            ])->fetch();
            $subjectId = $subjectRow ? (int)$subjectRow['ID'] : 1;
        }

        $arFields = [
            'NAME'            => $fields['NAME'] ?? '',
            'DESCRIPTION'     => $fields['DESCRIPTION'] ?? '',
            'VISIBLE'         => $fields['VISIBLE'] ?? 'Y',
            'OPENED'          => $fields['OPENED'] ?? 'Y',
            'PROJECT'         => $fields['PROJECT'] ?? 'N',
            'SUBJECT_ID'      => $subjectId,
            'INITIATE_PERMS'  => $fields['INITIATE_PERMS'] ?? \SONET_ROLES_MODERATOR,
            'SPAM_PERMS'      => $fields['SPAM_PERMS'] ?? \SONET_ROLES_MODERATOR,
            'ACTIVE'          => 'Y',
            'SITE_ID'         => [defined('SITE_ID') ? SITE_ID : 's1'],
        ];

        // Group image/avatar (CFile-compatible array from downloadPhoto)
        if (!empty($fields['IMAGE_ID']) && is_array($fields['IMAGE_ID'])) {
            $arFields['IMAGE_ID'] = $fields['IMAGE_ID'];
        }

        $groupId = \CSocNetGroup::createGroup($ownerId, $arFields, false);

        if (!$groupId || $groupId <= 0) {
            global $APPLICATION;
            $err = ($APPLICATION && method_exists($APPLICATION, 'GetException') && $APPLICATION->GetException())
                ? $APPLICATION->GetException()->GetString()
                : 'unknown error';
            throw new \Exception('CSocNetGroup::createGroup error: ' . $err);
        }

        // Activate disk (files) feature — same as REST sonet_group.create does
        \CSocNetFeatures::SetFeature(\SONET_ENTITY_GROUP, $groupId, 'files', true);

        return (int)$groupId;
    }

    /**
     * Add user to workgroup with proper role via CSocNetUserToGroup::Add (UPSERT).
     * REST sonet_group.user.add ignores ROLE and always sets K (member).
     * CSocNetUserToGroup::Add uses MERGE — if user already exists, updates their role.
     *
     * Roles: A=owner, E=moderator, K=member.
     */
    public static function addWorkgroupMember(int $groupId, int $userId, string $role = \SONET_ROLES_USER): bool
    {
        self::ensureSocialnetworkLoaded();

        // CSocNetUserToGroup::Add uses MERGE (upsert on USER_ID+GROUP_ID).
        // If user already exists — their ROLE gets updated. No need to check first.
        $id = \CSocNetUserToGroup::Add([
            'USER_ID'              => $userId,
            'GROUP_ID'             => $groupId,
            'ROLE'                 => $role,
            'INITIATED_BY_TYPE'    => 'G',
            'INITIATED_BY_USER_ID' => $userId,
            'SEND_MAIL'            => 'N',
        ], true); // skipCheckFields=true to avoid INITIATED_BY validation

        return (bool)$id;
    }

    /**
     * Transfer group ownership to another user via CSocNetUserToGroup::SetOwner.
     * User must already be a member before calling this.
     */
    public static function setWorkgroupOwner(int $groupId, int $userId): bool
    {
        self::ensureSocialnetworkLoaded();
        return (bool)\CSocNetUserToGroup::SetOwner($userId, $groupId);
    }

    // =========================================================================
    // Disk
    // =========================================================================

    /**
     * Upload a local temp file to a disk folder via D7.
     * Returns box disk file ID, or null on failure.
     */
    /**
     * Upload a file to a folder via CFile + addFile.
     * Returns Disk OBJECT ID (b_disk_object), suitable for 'n' . $id prefix in UF fields.
     *
     * NOTE: For files to be attachable to CRM timeline comments, the calling code MUST:
     *   1. Authorize a user via $USER->Authorize(1)  — required for FileUserType::canRead
     *   2. Call FileUserType::setValueForAllowEdit('CRM_TIMELINE', true) before CommentEntry::create
     */
    public static function uploadFileToFolder(int $folderId, string $tmpPath, string $fileName): ?int
    {
        if (!Loader::includeModule('disk')) return null;

        $folder = \Bitrix\Disk\Folder::loadById($folderId);
        if (!$folder) return null;

        $cfileArr = \CFile::MakeFileArray($tmpPath);
        if (!$cfileArr) return null;

        $savedFileId = \CFile::SaveFile($cfileArr, 'disk');
        if (!$savedFileId) return null;

        $diskFile = $folder->addFile(
            [
                'NAME' => $fileName,
                'FILE_ID' => $savedFileId,
                'SIZE' => filesize($tmpPath),
                'CREATED_BY' => 1,
            ],
            [],
            true // generateUniqueName
        );

        if (!$diskFile) {
            \CFile::Delete($savedFileId);
            return null;
        }

        return $diskFile->getId();
    }

    /**
     * Find or create migration folder on shared disk (Общий диск) via D7.
     * Returns folder ID.
     */
    public static function getOrCreateMigrationFolder(string $folderName): int
    {
        if (!Loader::includeModule('disk')) {
            throw new \Exception('Disk module not loaded');
        }

        $driver = \Bitrix\Disk\Driver::getInstance();
        $siteId = defined('SITE_ID') ? SITE_ID : 's1';

        $storage = $driver->getStorageByCommonId('shared_files_' . $siteId);
        if (!$storage && $siteId !== 's1') {
            $storage = $driver->getStorageByCommonId('shared_files_s1');
        }

        if (!$storage) {
            throw new \Exception('Общий диск не найден (shared_files_' . $siteId . ')');
        }

        $rootFolder = $storage->getRootObject();
        if (!$rootFolder) {
            throw new \Exception('Корневая папка общего диска не найдена');
        }

        $existing = $rootFolder->getChild([
            'NAME' => $folderName,
            'TYPE' => \Bitrix\Disk\Internals\ObjectTable::TYPE_FOLDER,
        ]);

        if ($existing) {
            return $existing->getId();
        }

        $folder = $rootFolder->addSubFolder(['NAME' => $folderName], [], false);
        if (!$folder) {
            throw new \Exception('Не удалось создать папку "' . $folderName . '" на общем диске');
        }

        return $folder->getId();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Send portal invitation email to an existing user.
     */
    private static function sendInvitation(int $userId, string $email): void
    {
        if (Loader::includeModule('intranet')) {
            try {
                \Bitrix\Intranet\Invitation::reinviteUser(
                    defined('SITE_ID') ? SITE_ID : 's1',
                    $userId
                );
            } catch (\Exception $e) {
                // Non-critical — user created, invitation just didn't send
            }
        }
    }

    /**
     * Download image from URL and return CFile-compatible array for CUser::Add / CSocNetGroup::createGroup.
     */
    public static function downloadPhoto(string $url): ?array
    {
        try {
            $tmpFile = tempnam(sys_get_temp_dir(), 'bx_photo_');
            $ch = curl_init($url);
            $fp = fopen($tmpFile, 'wb');
            curl_setopt_array($ch, [
                CURLOPT_FILE           => $fp,
                CURLOPT_TIMEOUT        => 15,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_SSL_VERIFYPEER => false,
            ]);
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);
            fclose($fp);

            if ($httpCode !== 200 || filesize($tmpFile) === 0) {
                @unlink($tmpFile);
                return null;
            }

            $ext = pathinfo(parse_url($url, PHP_URL_PATH), PATHINFO_EXTENSION) ?: 'jpg';

            return [
                'name'     => 'photo.' . $ext,
                'tmp_name' => $tmpFile,
                'type'     => mime_content_type($tmpFile),
                'size'     => filesize($tmpFile),
            ];
        } catch (\Throwable $e) {
            return null;
        }
    }

    // =========================================================================
    // Tasks Comments (D7)
    // =========================================================================

    /**
     * Add task comment with files via D7 (CTaskComments).
     * Works directly with Bitrix classes, bypassing REST API limitations.
     *
     * @param int $taskId Box task ID
     * @param int $userId User ID (author)
     * @param string $text Comment text
     * @param array $fileIds Disk file IDs to attach (array of integers)
     * @return int Comment ID or 0 on failure
     */
    /**
     * Add task comment with files via D7 (CTaskCommentItem::add).
     * Uses the newer D7 path which correctly handles AUTHOR_ID
     * by passing it to Forum\Comments\Feed constructor.
     *
     * @param int $taskId Box task ID
     * @param int $authorId User ID (comment author)
     * @param string $text Comment text
     * @param array $fileIds Disk file IDs to attach (array of integers)
     * @return int Comment ID or 0 on failure
     */
    public static function addTaskCommentWithFiles(int $taskId, int $userId, string $text, array $fileIds = []): int
    {
        try {
            if (!Loader::includeModule('tasks')) {
                throw new \Exception('tasks module failed to load');
            }
            if (!Loader::includeModule('forum')) {
                throw new \Exception('forum module failed to load');
            }
            if (!Loader::includeModule('disk')) {
                throw new \Exception('disk module failed to load');
            }

            // Ensure global $USER is initialized (required by ForumAddMessage)
            global $USER;
            if (!is_object($USER) || !$USER->IsAuthorized()) {
                if (!is_object($USER)) {
                    $USER = new \CUser();
                }
                $USER->Authorize($userId);
            }

            // Allow attaching files owned by other users
            \Bitrix\Disk\Uf\FileUserType::setValueForAllowEdit("FORUM_MESSAGE", true);

            $fields = [
                'POST_MESSAGE' => $text,
                'AUTHOR_ID'    => $userId,
            ];

            if (!empty($fileIds)) {
                // Plain integer Disk object IDs (b_disk_object.ID), no 'n' prefix
                $fields['UF_FORUM_MESSAGE_DOC'] = array_map('intval', $fileIds);
            }

            // Get tasks forum ID
            $forumId = \CTasksTools::getForumIdForIntranet();
            if (!$forumId) {
                throw new \Exception('Tasks forum not configured (getForumIdForIntranet=0)');
            }

            // Use Forum\Comments\Feed::addComment() which skips canAdd() permission check
            $feed = new \Bitrix\Forum\Comments\Feed(
                $forumId,
                [
                    'type' => 'TK',
                    'id' => $taskId,
                    'xml_id' => "TASK_{$taskId}",
                ],
                $userId
            );

            // Copy UF fields to $GLOBALS (required by EditFormAddFields)
            foreach ($fields as $key => $value) {
                if (strpos($key, 'UF_') === 0) {
                    $GLOBALS[$key] = $value;
                }
            }

            $addResult = $feed->addComment($fields);

            if (!$addResult || empty($addResult['ID'])) {
                $errors = $feed->getErrors();
                $errMsgs = [];
                if ($errors) {
                    foreach ($errors as $err) {
                        $errMsgs[] = $err->getMessage();
                    }
                }
                throw new \Exception('Feed::addComment failed: ' . ($errMsgs ? implode('; ', $errMsgs) : 'no ID returned'));
            }

            $commentId = (int)$addResult['ID'];

            return (int)$commentId;
        } catch (\Throwable $e) {
            throw new \Exception('D7 addTaskCommentWithFiles failed: ' . $e->getMessage() . ' at ' . $e->getFile() . ':' . $e->getLine(), 0, $e);
        }
    }

    /**
     * Create IM chat for a task via ChatFactory (D7).
     * Returns real IM chat ID or 0 on failure.
     */
    public static function createTaskChat(int $taskId, string $title, array $userIds = [1]): int
    {
        if (!Loader::includeModule('im')) return 0;

        $factory = \Bitrix\Im\V2\Chat\ChatFactory::getInstance();
        $result = $factory->addUniqueChat([
            'TITLE' => $title,
            'SKIP_ADD_MESSAGE' => 'Y',
            'TYPE' => 'X',
            'ENTITY_TYPE' => 'TASKS_TASK',
            'ENTITY_ID' => $taskId,
            'USERS' => $userIds,
            'AUTHOR_ID' => $userIds[0] ?? 1,
        ]);

        if ($result->isSuccess()) {
            return $result->getChatId();
        }

        // Try to find existing
        $row = \Bitrix\Im\Model\ChatTable::getList([
            'filter' => ['=ENTITY_TYPE' => 'TASKS_TASK', '=ENTITY_ID' => (string)$taskId],
            'select' => ['ID'],
        ])->fetch();
        return $row ? (int)$row['ID'] : 0;
    }

    /**
     * Upload file to user's personal storage and send to IM chat via D7.
     * Bypasses nginx upload limits entirely.
     *
     * @param int $chatId Real IM chat ID
     * @param string $tmpPath Path to temp file on server
     * @param string $fileName Original file name
     * @param string $text Optional message text
     * @param int $userId User who sends the file
     * @return int Message ID or 0 on failure
     */
    /**
     * Update the AUTHOR_ID of a forum message directly (for task comments
     * created through task.comment.add which always assigns webhook user as
     * author). Uses direct D7 Connection::queryExecute — safe because we only
     * touch the AUTHOR_ID column of a known ID.
     */
    public static function setForumMessageAuthor(int $forumMessageId, int $authorId): void
    {
        if ($forumMessageId <= 0 || $authorId <= 0) return;
        $conn = \Bitrix\Main\Application::getConnection();
        $conn->queryExecute(
            'UPDATE b_forum_message SET AUTHOR_ID = ' . (int)$authorId
            . ' WHERE ID = ' . (int)$forumMessageId
        );
    }

    /**
     * Attach disk objects to a forum message (task comment) via UF_FORUM_MESSAGE_DOC.
     * This is the D7-safe way to add files to forum comments, because REST
     * `task.commentitem.add` doesn't accept UF fields. Bitrix's
     * USER_FIELD_MANAGER->Update() takes disk-file values in the form ['n'.$diskObjectId]
     * and automatically creates the b_disk_attached_object rows with
     * ENTITY_TYPE='Bitrix\Disk\Uf\ForumMessageConnector'.
     *
     * @param int   $forumMessageId ID from b_forum_message (the task comment id)
     * @param int[] $diskObjectIds  Disk object IDs from b_disk_object (not attached_object)
     * @return bool true on success
     */
    public static function attachFilesToForumMessage(int $forumMessageId, array $diskObjectIds): bool
    {
        if (!Loader::includeModule('forum')) return false;
        if (empty($diskObjectIds)) return true;

        // USER_FIELD_MANAGER expects disk-file UF values as ['n'.$diskObjectId, ...]
        $values = array_map(fn($id) => 'n' . (int)$id, $diskObjectIds);

        global $USER_FIELD_MANAGER;
        if (!$USER_FIELD_MANAGER) return false;

        $USER_FIELD_MANAGER->Update('FORUM_MESSAGE', $forumMessageId, [
            'UF_FORUM_MESSAGE_DOC' => $values,
        ]);
        return true;
    }

    public static function sendFileToChatD7(int $chatId, string $tmpPath, string $fileName, string $text = '', int $userId = 1): int
    {
        if (!Loader::includeModule('disk') || !Loader::includeModule('im')) return 0;

        // Upload to user's personal storage
        $storage = \Bitrix\Disk\Driver::getInstance()->getStorageByUserId($userId);
        if (!$storage) return 0;

        $uploadFolder = $storage->getFolderForUploadedFiles();
        if (!$uploadFolder) return 0;

        $fileArray = \CFile::MakeFileArray($tmpPath);
        if (!$fileArray) return 0;

        $savedFileId = \CFile::SaveFile($fileArray, 'disk');
        if (!$savedFileId) return 0;

        $diskFile = $uploadFolder->addFile(
            [
                'NAME' => $fileName,
                'FILE_ID' => $savedFileId,
                'SIZE' => filesize($tmpPath),
                'CREATED_BY' => $userId,
            ],
            [],
            true
        );

        if (!$diskFile) return 0;

        // Send to chat
        $result = \CIMDisk::UploadFileFromDisk(
            $chatId,
            ['disk' . $diskFile->getId()],
            $text,
            [
                'USER_ID' => $userId,
                'SKIP_USER_CHECK' => true,
                'SYMLINK' => true,
            ]
        );

        if ($result && !empty($result['MESSAGE_ID'])) {
            return (int)$result['MESSAGE_ID'];
        }
        return 0;
    }

    // =========================================================================
    // CRM M:N Bindings (D7)
    // =========================================================================

    public static function bindContactCompany(int $contactId, int $companyId): void
    {
        if (!Loader::includeModule('crm')) return;
        \Bitrix\Crm\Binding\ContactCompanyTable::bindCompanyIDs($contactId, [$companyId]);
    }

    public static function bindDealContacts(int $dealId, array $contactIds): void
    {
        if (!Loader::includeModule('crm')) return;
        \Bitrix\Crm\Binding\DealContactTable::bindContactIDs($dealId, $contactIds);
    }

    // =========================================================================
    // CRM Requisites (D7)
    // =========================================================================

    /**
     * Rename the default deal pipeline (ID=0).
     * REST crm.dealcategory.update does NOT work for ID=0 (system virtual category).
     * The name is stored in b_option (module=crm, name=default_deal_category_name).
     */
    public static function setDefaultDealCategoryName(string $name): bool
    {
        $name = trim($name);
        if (empty($name)) return false;
        if (!\Bitrix\Main\Loader::includeModule('crm')) return false;

        // Use the official setter if available
        if (class_exists('\\Bitrix\\Crm\\Category\\DealCategory') &&
            method_exists('\\Bitrix\\Crm\\Category\\DealCategory', 'setDefaultCategoryName')) {
            \Bitrix\Crm\Category\DealCategory::setDefaultCategoryName($name);
        }

        // Always write directly to option as the source of truth
        \Bitrix\Main\Config\Option::set('crm', 'default_deal_category_name', $name, '');

        // Clear caches so UI reflects the change immediately
        try {
            if (isset($GLOBALS['CACHE_MANAGER'])) {
                $GLOBALS['CACHE_MANAGER']->clearByTag('crm_deal_category');
            }
            \Bitrix\Main\Application::getInstance()->getManagedCache()->clean('b_crm_deal_category');
        } catch (\Throwable $e) {}

        return true;
    }

    public static function addRequisite(array $fields): int
    {
        if (!Loader::includeModule('crm')) return 0;
        $requisite = new \Bitrix\Crm\EntityRequisite();
        $result = $requisite->add($fields);
        return $result->isSuccess() ? $result->getId() : 0;
    }

    public static function addBankDetail(array $fields): int
    {
        if (!Loader::includeModule('crm')) return 0;
        $bankDetail = new \Bitrix\Crm\EntityBankDetail();
        $result = $bankDetail->add($fields);
        return $result->isSuccess() ? $result->getId() : 0;
    }

    // =========================================================================
    // Smart Process Items (D7 Factory)
    // =========================================================================

    public static function addSmartItem(int $entityTypeId, array $fields): int
    {
        if (!Loader::includeModule('crm')) return 0;
        $factory = \Bitrix\Crm\Service\Container::getInstance()->getFactory($entityTypeId);
        if (!$factory) return 0;

        $item = $factory->createItem($fields);
        $op = $factory->getAddOperation($item);
        $op->disableCheckAccess();
        $op->disableAutomation();
        $result = $op->launch();
        return $result->isSuccess() ? $item->getId() : 0;
    }

    // =========================================================================
    // CRM User Fields (D7)
    // =========================================================================

    /**
     * Delete a CRM user field by ID via D7.
     * REST crm.*.userfield.delete often fails through external proxy.
     */
    public static function deleteUserfield(int $fieldId): bool
    {
        global $APPLICATION;
        if ($APPLICATION) {
            $APPLICATION->ResetException();
        }
        $userField = new \CUserTypeEntity();
        $ok = $userField->Delete($fieldId);
        if (!$ok) {
            $msg = 'unknown';
            if ($APPLICATION && ($ex = $APPLICATION->GetException())) {
                $msg = $ex->GetString();
                $APPLICATION->ResetException();
            }
            throw new \RuntimeException("CUserTypeEntity::Delete($fieldId) failed: $msg");
        }
        return true;
    }

    /**
     * Check if a UF field name is system/protected and should not be deleted.
     * These fields are managed by other modules and their deletion is vetoed.
     */
    public static function isProtectedUserfield(string $fieldName, string $xmlId = ''): bool
    {
        $protectedPrefixes = [
            'UF_CRM_BP_',
            'UF_CRM_WEBFORM_',
            'UF_CRM_TASK',
            'UF_CRM_ROBOT_',
            'UF_CRM_SMART_INVOICE_',
        ];
        foreach ($protectedPrefixes as $prefix) {
            if (strpos($fieldName, $prefix) === 0) return true;
        }
        if (!empty($xmlId)) {
            foreach (['BX_', 'WEBFORM_', 'BP_', 'RPA_', 'BIZPROC_'] as $xmlPrefix) {
                if (strpos($xmlId, $xmlPrefix) === 0) return true;
            }
        }
        return false;
    }

    /**
     * Get all CRM user fields for an entity type via D7.
     */
    public static function getUserfields(string $entityType): array
    {
        $entityIdMap = [
            'deal' => 'CRM_DEAL',
            'contact' => 'CRM_CONTACT',
            'company' => 'CRM_COMPANY',
            'lead' => 'CRM_LEAD',
        ];
        $entityId = $entityIdMap[$entityType] ?? '';
        if (empty($entityId)) return [];

        $result = [];
        $res = \CUserTypeEntity::GetList([], ['ENTITY_ID' => $entityId]);
        while ($row = $res->Fetch()) {
            $result[] = $row;
        }
        return $result;
    }

    /**
     * Set original dates on a CRM entity after creation.
     * CCrm*::Add() ignores DATE_CREATE — direct SQL UPDATE is the only way.
     */
    private static $allowedBackdateTables = [
        'b_crm_company', 'b_crm_contact', 'b_crm_deal', 'b_crm_lead',
        'b_crm_act', 'b_crm_timeline',
    ];

    public static function backdateEntity(string $table, int $id, string $dateCreate, string $dateModify = ''): void
    {
        // Whitelist table names to prevent SQL injection
        if (!in_array($table, self::$allowedBackdateTables, true)) {
            if (strpos($table, 'b_crm_dynamic_items_') === 0) {
                $suffix = substr($table, strlen('b_crm_dynamic_items_'));
                if (!ctype_digit($suffix)) return;
            } else {
                return;
            }
        }

        $ts = strtotime($dateCreate);
        if ($ts === false) return; // unparseable date — skip

        $conn = \Bitrix\Main\Application::getConnection();
        $helper = $conn->getSqlHelper();

        $dateCreate = date('Y-m-d H:i:s', $ts);
        if (empty($dateModify)) {
            $dateModify = $dateCreate;
        } else {
            $tsm = strtotime($dateModify);
            $dateModify = ($tsm !== false) ? date('Y-m-d H:i:s', $tsm) : $dateCreate;
        }

        $safeCreate = $helper->forSql($dateCreate);
        $safeModify = $helper->forSql($dateModify);
        $safeId = (int)$id;

        if ($table === 'b_crm_act') {
            $conn->queryExecute(
                "UPDATE {$table} SET CREATED = '{$safeCreate}', LAST_UPDATED = '{$safeModify}' WHERE ID = {$safeId}"
            );
        } elseif ($table === 'b_crm_timeline') {
            $conn->queryExecute(
                "UPDATE {$table} SET CREATED = '{$safeCreate}' WHERE ID = {$safeId}"
            );
        } elseif (strpos($table, 'b_crm_dynamic_items_') === 0) {
            $conn->queryExecute(
                "UPDATE {$table} SET CREATED_TIME = '{$safeCreate}', UPDATED_TIME = '{$safeModify}' WHERE ID = {$safeId}"
            );
        } else {
            $conn->queryExecute(
                "UPDATE {$table} SET DATE_CREATE = '{$safeCreate}', DATE_MODIFY = '{$safeModify}' WHERE ID = {$safeId}"
            );
        }
    }
}
