<?php

use Bitrix\Main\Localization\Loc;
use Bitrix\Main\ModuleManager;
use Bitrix\Main\Config\Option;
use Bitrix\Main\EventManager;
use Bitrix\Main\Application;
use Bitrix\Main\IO\Directory;

Loc::loadMessages(__FILE__);

class bitrix_migrator extends CModule
{
    public $MODULE_ID = 'bitrix_migrator';
    public $MODULE_VERSION;
    public $MODULE_VERSION_DATE;
    public $MODULE_NAME;
    public $MODULE_DESCRIPTION;
    public $PARTNER_NAME = 'Aliaksandr Zurausky';
    public $PARTNER_URI = 'https://github.com/AliaksandrZurausky/bitrix.migrator';

    public function __construct()
    {
        if (file_exists(__DIR__ . '/version.php')) {
            include_once(__DIR__ . '/version.php');
            $this->MODULE_VERSION = $arModuleVersion['VERSION'];
            $this->MODULE_VERSION_DATE = $arModuleVersion['VERSION_DATE'];
            $this->MODULE_NAME = Loc::getMessage('BITRIX_MIGRATOR_NAME');
            $this->MODULE_DESCRIPTION = Loc::getMessage('BITRIX_MIGRATOR_DESCRIPTION');
        }
    }

    public function DoInstall()
    {
        global $APPLICATION;

        if (!CheckVersion(ModuleManager::getVersion('main'), '19.00.00')) {
            $APPLICATION->ThrowException(Loc::getMessage('BITRIX_MIGRATOR_MIN_VERSION_ERROR'));
            return false;
        }

        try {
            ModuleManager::registerModule($this->MODULE_ID);
            $this->InstallDB();
            $this->InstallEvents();
            $this->InstallFiles();

            $APPLICATION->IncludeAdminFile(
                Loc::getMessage('BITRIX_MIGRATOR_INSTALL_TITLE'),
                __DIR__ . '/install_success.php'
            );
        } catch (Exception $e) {
            $APPLICATION->ThrowException($e->getMessage());
            return false;
        }

        return true;
    }

    public function DoUninstall()
    {
        global $APPLICATION;

        try {
            $uninstallStep = $_REQUEST['uninstall_step'] ?? null;

            if ($uninstallStep !== 'final') {
                $APPLICATION->IncludeAdminFile(
                    Loc::getMessage('BITRIX_MIGRATOR_UNINSTALL_TITLE'),
                    __DIR__ . '/uninstall_confirm.php'
                );
                return true;
            }

            $deleteData = $_REQUEST['delete_data'] === 'Y';

            $this->UninstallEvents();
            $this->UninstallFiles();

            if ($deleteData) {
                $this->UninstallDB();
            }

            Option::delete($this->MODULE_ID);
            ModuleManager::unregisterModule($this->MODULE_ID);

            $APPLICATION->IncludeAdminFile(
                Loc::getMessage('BITRIX_MIGRATOR_UNINSTALL_TITLE'),
                __DIR__ . '/uninstall_success.php'
            );
        } catch (Exception $e) {
            $APPLICATION->ThrowException($e->getMessage());
            return false;
        }

        return true;
    }

    public function InstallDB()
    {
        if (!\Bitrix\Main\Loader::includeModule('highloadblock')) {
            throw new Exception(Loc::getMessage('BITRIX_MIGRATOR_HL_REQUIRED'));
        }

        $this->createStateTable();
        $this->createQueueTable();
        $this->createMapTable();
        $this->createLogsTable();
    }

    public function createStateTable()
    {
        $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
            'filter' => ['=NAME' => 'MigratorState']
        ])->fetch();

        if (!$hlblock) {
            $result = \Bitrix\Highloadblock\HighloadBlockTable::add([
                'NAME' => 'MigratorState',
                'TABLE_NAME' => 'b_migrator_state'
            ]);

            if (!$result->isSuccess()) {
                throw new Exception('Error creating MigratorState table: ' . implode(', ', $result->getErrorMessages()));
            }

            $blockId = $result->getId();
            $userTypeEntity = new \CUserTypeEntity();

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_STATUS',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_STATUS',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Статус миграции'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Статус'],
                'SETTINGS' => ['SIZE' => 50]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CURRENT_PHASE',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_CURRENT_PHASE',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Текущая фаза'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Фаза'],
                'SETTINGS' => ['SIZE' => 50]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_PAUSE_FLAG',
                'USER_TYPE_ID' => 'boolean',
                'XML_ID' => 'UF_PAUSE_FLAG',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Флаг паузы'],
                'LIST_COLUMN_LABEL' => ['ru' => 'На паузе']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_SETTINGS_JSON',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_SETTINGS_JSON',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Настройки (JSON)'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Настройки'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 5]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_LAST_ERROR',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_LAST_ERROR',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Последняя ошибка'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Ошибка'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 3]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_STARTED_AT',
                'USER_TYPE_ID' => 'datetime',
                'XML_ID' => 'UF_STARTED_AT',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Начало миграции'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Начало']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_UPDATED_AT',
                'USER_TYPE_ID' => 'datetime',
                'XML_ID' => 'UF_UPDATED_AT',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Обновлено'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Обновлено']
            ]);

            Option::set($this->MODULE_ID, 'state_hlblock_id', $blockId);
        }
    }

    public function createQueueTable()
    {
        $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
            'filter' => ['=NAME' => 'MigratorQueue']
        ])->fetch();

        if (!$hlblock) {
            $result = \Bitrix\Highloadblock\HighloadBlockTable::add([
                'NAME' => 'MigratorQueue',
                'TABLE_NAME' => 'b_migrator_queue'
            ]);

            if (!$result->isSuccess()) {
                throw new Exception('Error creating MigratorQueue table: ' . implode(', ', $result->getErrorMessages()));
            }

            $blockId = $result->getId();
            $userTypeEntity = new \CUserTypeEntity();

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_ENTITY_TYPE',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_ENTITY_TYPE',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Тип сущности'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Тип'],
                'SETTINGS' => ['SIZE' => 50]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CLOUD_ID',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_CLOUD_ID',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Cloud ID'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Cloud ID'],
                'SETTINGS' => ['SIZE' => 100]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_LOCAL_ID',
                'USER_TYPE_ID' => 'integer',
                'XML_ID' => 'UF_LOCAL_ID',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Local ID'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Local ID']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_STATUS',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_STATUS',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Статус'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Статус'],
                'SETTINGS' => ['SIZE' => 20]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_RETRIES',
                'USER_TYPE_ID' => 'integer',
                'XML_ID' => 'UF_RETRIES',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Попытки'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Попытки']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_LAST_ERROR',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_LAST_ERROR',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Последняя ошибка'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Ошибка'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 3]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_PRIORITY',
                'USER_TYPE_ID' => 'integer',
                'XML_ID' => 'UF_PRIORITY',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Приоритет'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Приоритет']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_DEPENDS_ON',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_DEPENDS_ON',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Зависит от'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Зависит от'],
                'SETTINGS' => ['SIZE' => 100]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_UPDATED_AT',
                'USER_TYPE_ID' => 'datetime',
                'XML_ID' => 'UF_UPDATED_AT',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Обновлено'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Обновлено']
            ]);

            Option::set($this->MODULE_ID, 'queue_hlblock_id', $blockId);
        }
    }

    public function createMapTable()
    {
        $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
            'filter' => ['=NAME' => 'MigratorMap']
        ])->fetch();

        if (!$hlblock) {
            $result = \Bitrix\Highloadblock\HighloadBlockTable::add([
                'NAME' => 'MigratorMap',
                'TABLE_NAME' => 'b_migrator_map'
            ]);

            if (!$result->isSuccess()) {
                throw new Exception('Error creating MigratorMap table: ' . implode(', ', $result->getErrorMessages()));
            }

            $blockId = $result->getId();
            $userTypeEntity = new \CUserTypeEntity();

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_ENTITY_TYPE',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_ENTITY_TYPE',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Тип сущности'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Тип'],
                'SETTINGS' => ['SIZE' => 50]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CLOUD_ID',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_CLOUD_ID',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Cloud ID'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Cloud ID'],
                'SETTINGS' => ['SIZE' => 100]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_LOCAL_ID',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_LOCAL_ID',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Local ID'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Local ID'],
                'SETTINGS' => ['SIZE' => 100]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CLOUD_URL',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_CLOUD_URL',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Cloud URL'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Cloud URL'],
                'SETTINGS' => ['SIZE' => 255]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_META_JSON',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_META_JSON',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Метаданные (JSON)'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Метаданные'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 3]
            ]);

            Option::set($this->MODULE_ID, 'map_hlblock_id', $blockId);
        }
    }

    public function createLogsTable()
    {
        $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
            'filter' => ['=NAME' => 'MigratorLogs']
        ])->fetch();

        if (!$hlblock) {
            $result = \Bitrix\Highloadblock\HighloadBlockTable::add([
                'NAME' => 'MigratorLogs',
                'TABLE_NAME' => 'b_migrator_logs'
            ]);

            if (!$result->isSuccess()) {
                throw new Exception('Error creating MigratorLogs table: ' . implode(', ', $result->getErrorMessages()));
            }

            $blockId = $result->getId();
            $userTypeEntity = new \CUserTypeEntity();

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_LEVEL',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_LEVEL',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Уровень'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Уровень'],
                'SETTINGS' => ['SIZE' => 20]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_MESSAGE',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_MESSAGE',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'Сообщение'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Сообщение'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 5]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CONTEXT',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_CONTEXT',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Контекст'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Контекст'],
                'SETTINGS' => ['SIZE' => 500]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_PAYLOAD',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_PAYLOAD',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Данные (JSON)'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Данные'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 3]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CREATED_AT',
                'USER_TYPE_ID' => 'datetime',
                'XML_ID' => 'UF_CREATED_AT',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Создано'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Создано']
            ]);

            Option::set($this->MODULE_ID, 'logs_hlblock_id', $blockId);
        }
    }

    public function UninstallDB()
    {
        if (!\Bitrix\Main\Loader::includeModule('highloadblock')) {
            return;
        }

        $tables = ['MigratorState', 'MigratorQueue', 'MigratorMap', 'MigratorLogs'];

        foreach ($tables as $tableName) {
            $hlblock = \Bitrix\Highloadblock\HighloadBlockTable::getList([
                'filter' => ['=NAME' => $tableName]
            ])->fetch();

            if ($hlblock) {
                try {
                    \Bitrix\Highloadblock\HighloadBlockTable::delete($hlblock['ID']);
                } catch (Exception $e) {
                    // Log but don't fail
                }
            }
        }
    }

    public function InstallEvents()
    {
        // Register agent
        if (class_exists('CAgent')) {
            \CAgent::AddAgent(
                "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
                $this->MODULE_ID,
                "N",
                60
            );
        }

        // Register admin menu
        EventManager::getInstance()->registerEventHandler(
            'main',
            'OnBuildGlobalMenu',
            $this->MODULE_ID,
            '\\BitrixMigrator\\EventHandlers',
            'OnBuildGlobalMenu'
        );
    }

    public function UninstallEvents()
    {
        if (class_exists('CAgent')) {
            \CAgent::RemoveModuleAgents($this->MODULE_ID);
        }

        EventManager::getInstance()->unRegisterEventHandler(
            'main',
            'OnBuildGlobalMenu',
            $this->MODULE_ID,
            '\\BitrixMigrator\\EventHandlers',
            'OnBuildGlobalMenu'
        );
    }

    public function InstallFiles()
    {
        $docRoot = Application::getDocumentRoot();
        $localAdminDir = $docRoot . '/local/admin/';
        
        // Create /local/admin/ if not exists
        if (!is_dir($localAdminDir)) {
            mkdir($localAdminDir, 0755, true);
        }
        
        // Copy admin page file to /local/admin/
        CopyDirFiles(
            __DIR__ . '/admin/bitrix_migrator.php',
            $localAdminDir . 'bitrix_migrator.php',
            true,
            true
        );
        
        // Copy language files to /local/admin/lang/
        $moduleDir = dirname(__DIR__);
        $languages = ['ru', 'en'];
        
        foreach ($languages as $lang) {
            $sourceLangDir = $moduleDir . '/lang/' . $lang . '/admin/';
            $targetLangDir = $localAdminDir . 'lang/' . $lang . '/';
            
            if (is_dir($sourceLangDir)) {
                if (!is_dir($targetLangDir)) {
                    mkdir($targetLangDir, 0755, true);
                }
                
                CopyDirFiles(
                    $sourceLangDir,
                    $targetLangDir,
                    true,
                    true
                );
            }
        }
    }

    public function UninstallFiles()
    {
        $docRoot = Application::getDocumentRoot();
        
        // Remove admin file from /local/admin/
        $adminFile = $docRoot . '/local/admin/bitrix_migrator.php';
        if (file_exists($adminFile)) {
            @unlink($adminFile);
        }
        
        // Remove language files from /local/admin/lang/
        $languages = ['ru', 'en'];
        foreach ($languages as $lang) {
            $langDir = $docRoot . '/local/admin/lang/' . $lang . '/';
            if (is_dir($langDir)) {
                $files = scandir($langDir);
                foreach ($files as $file) {
                    if (strpos($file, 'bitrix_migrator') === 0) {
                        @unlink($langDir . $file);
                    }
                }
            }
        }
    }
}