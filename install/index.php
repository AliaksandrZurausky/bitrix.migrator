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
            $this->InstallDB();
            $this->InstallEvents();
            ModuleManager::registerModule($this->MODULE_ID);

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

        $this->createQueueTable();
        $this->createLogsTable();
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
                'FIELD_NAME' => 'UF_ENTITY_ID',
                'USER_TYPE_ID' => 'integer',
                'XML_ID' => 'UF_ENTITY_ID',
                'MANDATORY' => 'Y',
                'EDIT_FORM_LABEL' => ['ru' => 'ID сущности'],
                'LIST_COLUMN_LABEL' => ['ru' => 'ID']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_STATUS',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_STATUS',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Статус'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Статус'],
                'SETTINGS' => ['SIZE' => 20]
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_CLOUD_ID',
                'USER_TYPE_ID' => 'integer',
                'XML_ID' => 'UF_CLOUD_ID',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Cloud ID'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Cloud ID']
            ]);

            $userTypeEntity->Add([
                'ENTITY_ID' => 'HLBLOCK_' . $blockId,
                'FIELD_NAME' => 'UF_ERROR_MSG',
                'USER_TYPE_ID' => 'string',
                'XML_ID' => 'UF_ERROR_MSG',
                'MANDATORY' => 'N',
                'EDIT_FORM_LABEL' => ['ru' => 'Сообщение об ошибке'],
                'LIST_COLUMN_LABEL' => ['ru' => 'Ошибка'],
                'SETTINGS' => ['SIZE' => 0, 'ROWS' => 3]
            ]);

            Option::set($this->MODULE_ID, 'queue_hlblock_id', $blockId);
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

            Option::set($this->MODULE_ID, 'logs_hlblock_id', $blockId);
        }
    }

    public function UninstallDB()
    {
        if (!\Bitrix\Main\Loader::includeModule('highloadblock')) {
            return;
        }

        $tables = ['MigratorQueue', 'MigratorLogs'];

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
        // Регистрируем агент
        if (class_exists('CAgent')) {
            \CAgent::AddAgent(
                "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
                $this->MODULE_ID,
                "N",
                60
            );
        }
    }

    public function UninstallEvents()
    {
        if (class_exists('CAgent')) {
            \CAgent::RemoveModuleAgents($this->MODULE_ID);
        }
    }
}
