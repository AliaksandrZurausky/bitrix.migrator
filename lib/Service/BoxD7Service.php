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

        if (!empty($fields['UF_DEPARTMENT'])) {
            $userFields['UF_DEPARTMENT'] = $fields['UF_DEPARTMENT'];
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
        $obj = new \CCrmCompany(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmCompany::Add error: ' . $obj->GetLastErrorMessage());
        }
        return (int)$id;
    }

    public static function updateCompany(int $id, array $fields): bool
    {
        $obj = new \CCrmCompany(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmCompany::Update error: ' . $obj->GetLastErrorMessage());
        }
        return true;
    }

    // =========================================================================
    // Contacts
    // =========================================================================

    public static function createContact(array $fields): int
    {
        $obj = new \CCrmContact(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmContact::Add error: ' . $obj->GetLastErrorMessage());
        }
        return (int)$id;
    }

    public static function updateContact(int $id, array $fields): bool
    {
        $obj = new \CCrmContact(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmContact::Update error: ' . $obj->GetLastErrorMessage());
        }
        return true;
    }

    /**
     * Link contact to companies (replaces crm.contact.company.items.set).
     */
    public static function setContactCompanies(int $contactId, array $companyIds): void
    {
        if (!Loader::includeModule('crm')) return;

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
        $obj = new \CCrmDeal(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmDeal::Add error: ' . $obj->GetLastErrorMessage());
        }
        return (int)$id;
    }

    public static function updateDeal(int $id, array $fields): bool
    {
        $obj = new \CCrmDeal(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmDeal::Update error: ' . $obj->GetLastErrorMessage());
        }
        return true;
    }

    /**
     * Link deal to contacts (replaces crm.deal.contact.items.set).
     */
    public static function setDealContacts(int $dealId, array $contactIds): void
    {
        if (!Loader::includeModule('crm')) return;

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
        $obj = new \CCrmLead(false);
        $id = $obj->Add($fields, true, true, ['DISABLE_USER_FIELD_CHECK' => false]);
        if (!$id) {
            throw new \Exception('CCrmLead::Add error: ' . $obj->GetLastErrorMessage());
        }
        return (int)$id;
    }

    public static function updateLead(int $id, array $fields): bool
    {
        $obj = new \CCrmLead(false);
        if (!$obj->Update($id, $fields, true, true)) {
            throw new \Exception('CCrmLead::Update error: ' . $obj->GetLastErrorMessage());
        }
        return true;
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
