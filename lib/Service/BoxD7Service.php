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
            'ACTIVE'           => 'Y',
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

    // =========================================================================
    // Companies
    // =========================================================================

    public static function createCompany(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmCompany(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmCompany::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
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
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmContact::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
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

        $items = [];
        foreach ($companyIds as $i => $companyId) {
            $items[] = [
                'COMPANY_ID' => $companyId,
                'SORT'       => ($i + 1) * 10,
                'IS_PRIMARY' => $i === 0 ? 'Y' : 'N',
            ];
        }
        \CCrmContactCompany::RegisterRelationByContact($contactId, $items, false, ['REGISTER_SONET_EVENT' => false]);
    }

    // =========================================================================
    // Deals
    // =========================================================================

    public static function createDeal(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmDeal(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmDeal::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
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

        $items = [];
        foreach ($contactIds as $i => $contactId) {
            $items[] = [
                'CONTACT_ID' => $contactId,
                'SORT'       => ($i + 1) * 10,
                'IS_PRIMARY' => $i === 0 ? 'Y' : 'N',
            ];
        }
        \CCrmDealContact::RegisterRelation($dealId, $items, false, ['REGISTER_SONET_EVENT' => false]);
    }

    // =========================================================================
    // Leads
    // =========================================================================

    public static function createLead(array $fields): int
    {
        self::ensureCrmLoaded();
        $obj = new \CCrmLead(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmLead::Add error: ' . ($obj->LAST_ERROR ?? 'unknown error'));
        }
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

        $arFields = [
            'NAME'        => $fields['NAME'] ?? '',
            'DESCRIPTION' => $fields['DESCRIPTION'] ?? '',
            'VISIBLE'     => $fields['VISIBLE'] ?? 'Y',
            'OPENED'      => $fields['OPENED'] ?? 'Y',
            'PROJECT'     => $fields['PROJECT'] ?? 'N',
            'ACTIVE'      => 'Y',
            'SITE_ID'     => [defined('SITE_ID') ? SITE_ID : 's1'],
        ];

        $groupId = \CSocNetGroup::createGroup($ownerId, $arFields, false);

        if (!$groupId || $groupId <= 0) {
            global $APPLICATION;
            $err = ($APPLICATION && method_exists($APPLICATION, 'GetException') && $APPLICATION->GetException())
                ? $APPLICATION->GetException()->GetString()
                : 'unknown error';
            throw new \Exception('CSocNetGroup::createGroup error: ' . $err);
        }

        return (int)$groupId;
    }

    /**
     * Add user to workgroup with proper role via CSocNetUserToGroup::Add (UPSERT).
     * REST sonet_group.user.add ignores ROLE and always sets K (member).
     * CSocNetUserToGroup::Add uses MERGE — if user already exists, updates their role.
     *
     * Roles: A=owner, E=moderator, K=member.
     */
    public static function addWorkgroupMember(int $groupId, int $userId, string $role = 'K'): bool
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
    public static function uploadFileToFolder(int $folderId, string $tmpPath, string $fileName): ?int
    {
        if (!Loader::includeModule('disk')) return null;

        $folder = \Bitrix\Disk\Folder::loadById($folderId);
        if (!$folder) return null;

        $file = $folder->uploadFile(
            [
                'name'     => $fileName,
                'tmp_name' => $tmpPath,
                'type'     => mime_content_type($tmpPath) ?: 'application/octet-stream',
                'size'     => filesize($tmpPath),
            ],
            ['NAME' => $fileName, 'CREATED_BY' => 1],
            [],
            true // generateUniqueName
        );

        return $file ? $file->getId() : null;
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
     * Download photo from URL and return CFile-compatible array for CUser::Add.
     */
    private static function downloadPhoto(string $url): ?array
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
}
