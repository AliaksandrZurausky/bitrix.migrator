<?php

use Bitrix\Main\Loader;
use Bitrix\Main\ModuleManager;

class bitrix_migrator extends CModule
{
    public $MODULE_ID = 'bitrix_migrator';
    public $MODULE_VERSION;
    public $MODULE_VERSION_DATE;
    public $MODULE_NAME = 'Bitrix Migrator';
    public $MODULE_DESCRIPTION = 'Миграция Bitrix24 «облако → коробка» (каркас).';
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
        ModuleManager::registerModule($this->MODULE_ID);

        // На этом этапе ставим только каркас. HL-блоки, агенты и UI добавим следующими итерациями.
    }

    public function DoUninstall(): void
    {
        // Здесь будет удаление HL-блоков/агентов/настроек (когда они появятся).
        ModuleManager::unRegisterModule($this->MODULE_ID);
    }
}
