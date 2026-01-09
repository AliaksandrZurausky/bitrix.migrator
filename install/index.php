<?php

use Bitrix\Main\Loader;
use Bitrix\Main\ModuleManager;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Application;
use Bitrix\Highloadblock\HighloadBlockTable;
use Bitrix\Main\UserFieldTable;

class bitrix_migrator extends CModule
{
    public $MODULE_ID = 'bitrix_migrator';
    public $MODULE_VERSION;
    public $MODULE_VERSION_DATE;
    public $MODULE_NAME = 'Bitrix Migrator';
    public $MODULE_DESCRIPTION = 'Миграция Bitrix24 «облако → коробка» с переносом таймлайна.';
    public $PARTNER_NAME = 'bitrix_migrator';
    public $PARTNER_URI = '';

    public function __construct()
    {
        $arModuleVersion = [];
        include __DIR__ . '/version.php';

        $this->MODULE_VERSION = $arModuleVersion['VERSION'] ?? '0.0.0';
        $this->MODULE_VERSION_DATE = $arModuleVersion['VERSION_DATE'] ?? date('Y-m-d');
    }

    public function DoInstall(): void
    {
        global $APPLICATION;

        if (!Loader::includeModule('highloadblock')) {
            $APPLICATION->ThrowException('Модуль highloadblock не установлен.');
            return;
        }

        ModuleManager::registerModule($this->MODULE_ID);
        $this->installHlBlocks();
        $this->installAgents();

        $APPLICATION->IncludeAdminFile(
            'Установка модуля ' . $this->MODULE_NAME,
            __DIR__ . '/step1.php'
        );
    }

    public function DoUninstall(): void
    {
        global $APPLICATION, $step;

        $step = (int)$step;
        if ($step < 2) {
            $APPLICATION->IncludeAdminFile(
                'Удаление модуля ' . $this->MODULE_NAME,
                __DIR__ . '/unstep1.php'
            );
        } else {
            $this->uninstallAgents();

            if ($_REQUEST['savedata'] !== 'Y') {
                $this->uninstallHlBlocks();
                Option::delete($this->MODULE_ID);
            }

            ModuleManager::unRegisterModule($this->MODULE_ID);

            $APPLICATION->IncludeAdminFile(
                'Удаление модуля ' . $this->MODULE_NAME,
                __DIR__ . '/unstep2.php'
            );
        }
    }

    private function installHlBlocks(): void
    {
        $blocks = [
            [
                'NAME' => 'BitrixMigratorQueue',
                'TABLE_NAME' => 'b_hlbd_bitrix_migrator_queue',
                'FIELDS' => [
                    'UF_TYPE' => ['USER_TYPE_ID' => 'string', 'MANDATORY' => 'Y', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_ENTITY_TYPE' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_CLOUD_ID' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_BOX_ID' => ['USER_TYPE_ID' => 'integer'],
                    'UF_STEP' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_STATUS' => ['USER_TYPE_ID' => 'string', 'MANDATORY' => 'Y', 'SETTINGS' => ['SIZE' => 20, 'DEFAULT_VALUE' => 'NEW']],
                    'UF_RETRY' => ['USER_TYPE_ID' => 'integer', 'SETTINGS' => ['DEFAULT_VALUE' => 0]],
                    'UF_NEXT_RUN_AT' => ['USER_TYPE_ID' => 'datetime'],
                    'UF_PAYLOAD' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 0]],
                    'UF_ERROR' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 0]],
                    'UF_CREATED_AT' => ['USER_TYPE_ID' => 'datetime', 'MANDATORY' => 'Y'],
                    'UF_UPDATED_AT' => ['USER_TYPE_ID' => 'datetime'],
                ],
            ],
            [
                'NAME' => 'BitrixMigratorMap',
                'TABLE_NAME' => 'b_hlbd_bitrix_migrator_map',
                'FIELDS' => [
                    'UF_ENTITY' => ['USER_TYPE_ID' => 'string', 'MANDATORY' => 'Y', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_CLOUD_ID' => ['USER_TYPE_ID' => 'string', 'MANDATORY' => 'Y', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_BOX_ID' => ['USER_TYPE_ID' => 'integer', 'MANDATORY' => 'Y'],
                    'UF_HASH' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 32]],
                    'UF_META' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 0]],
                    'UF_CREATED_AT' => ['USER_TYPE_ID' => 'datetime', 'MANDATORY' => 'Y'],
                ],
            ],
            [
                'NAME' => 'BitrixMigratorLog',
                'TABLE_NAME' => 'b_hlbd_bitrix_migrator_log',
                'FIELDS' => [
                    'UF_LEVEL' => ['USER_TYPE_ID' => 'string', 'MANDATORY' => 'Y', 'SETTINGS' => ['SIZE' => 20]],
                    'UF_SCOPE' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 100]],
                    'UF_MESSAGE' => ['USER_TYPE_ID' => 'string', 'MANDATORY' => 'Y', 'SETTINGS' => ['SIZE' => 0]],
                    'UF_CONTEXT' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 0]],
                    'UF_ENTITY' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_CLOUD_ID' => ['USER_TYPE_ID' => 'string', 'SETTINGS' => ['SIZE' => 50]],
                    'UF_BOX_ID' => ['USER_TYPE_ID' => 'integer'],
                    'UF_CREATED_AT' => ['USER_TYPE_ID' => 'datetime', 'MANDATORY' => 'Y'],
                ],
            ],
        ];

        foreach ($blocks as $blockData) {
            $existing = HighloadBlockTable::getList([
                'filter' => ['NAME' => $blockData['NAME']],
            ])->fetch();

            if ($existing) {
                Option::set($this->MODULE_ID, 'HL_' . $blockData['NAME'], $existing['ID']);
            } else {
                $result = HighloadBlockTable::add([
                    'NAME' => $blockData['NAME'],
                    'TABLE_NAME' => $blockData['TABLE_NAME'],
                ]);

                if ($result->isSuccess()) {
                    $hlBlockId = $result->getId();
                    Option::set($this->MODULE_ID, 'HL_' . $blockData['NAME'], $hlBlockId);

                    foreach ($blockData['FIELDS'] as $fieldName => $fieldConfig) {
                        $this->addUserField($hlBlockId, $fieldName, $fieldConfig);
                    }
                }
            }
        }
    }

    private function addUserField(int $hlBlockId, string $fieldName, array $config): void
    {
        $arField = [
            'ENTITY_ID' => 'HLBLOCK_' . $hlBlockId,
            'FIELD_NAME' => $fieldName,
            'USER_TYPE_ID' => $config['USER_TYPE_ID'],
            'MANDATORY' => $config['MANDATORY'] ?? 'N',
            'EDIT_FORM_LABEL' => ['ru' => $fieldName, 'en' => $fieldName],
            'LIST_COLUMN_LABEL' => ['ru' => $fieldName, 'en' => $fieldName],
            'LIST_FILTER_LABEL' => ['ru' => $fieldName, 'en' => $fieldName],
            'ERROR_MESSAGE' => ['ru' => '', 'en' => ''],
            'HELP_MESSAGE' => ['ru' => '', 'en' => ''],
        ];

        if (isset($config['SETTINGS'])) {
            $arField['SETTINGS'] = $config['SETTINGS'];
        }

        $oUserTypeEntity = new CUserTypeEntity();
        $oUserTypeEntity->Add($arField);
    }

    private function uninstallHlBlocks(): void
    {
        $blocks = ['BitrixMigratorQueue', 'BitrixMigratorMap', 'BitrixMigratorLog'];

        foreach ($blocks as $blockName) {
            $hlId = Option::get($this->MODULE_ID, 'HL_' . $blockName, 0);
            if ($hlId > 0) {
                HighloadBlockTable::delete($hlId);
            }
        }
    }

    private function installAgents(): void
    {
        \CAgent::AddAgent(
            "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
            $this->MODULE_ID,
            "N",
            60
        );
    }

    private function uninstallAgents(): void
    {
        \CAgent::RemoveAgent(
            "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
            $this->MODULE_ID
        );
    }
}
