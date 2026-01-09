<?php

use Bitrix\Main\ModuleManager;
use Bitrix\Main\Loader;
use BitrixMigrator\Config\HlConfig;

class bitrix_migrator extends CModule
{
    public $MODULE_ID = 'bitrix_migrator';
    public $MODULE_NAME = 'Bitrix Migrator';
    public $MODULE_DESCRIPTION = 'Миграция данных из облако в коробку';
    public $MODULE_VERSION = '1.0.0';
    public $MODULE_VERSION_DATE = '2026-01-09';
    public $PARTNER_NAME = 'Aliaksandr Zurausky';
    public $PARTNER_URI = 'https://github.com/AliaksandrZurausky/bitrix.migrator';

    public function DoInstall()
    {
        global $APPLICATION;

        if (!check_bitrix_sessid()) {
            return false;
        }

        try {
            ModuleManager::registerModule($this->MODULE_ID);

            // Устанавливаем HL-блоки
            Loader::includeModule($this->MODULE_ID);
            $this->setupHighloadBlocks();

            // Регистрируем агенты
            $this->setupAgents();

            $APPLICATION->IncludeAdminFile(
                'Установка ' . $this->MODULE_NAME,
                __DIR__ . '/install/install_success.php'
            );
        } catch (\Exception $e) {
            $APPLICATION->ThrowException($e->getMessage());
            return false;
        }

        return true;
    }

    public function DoUninstall()
    {
        global $APPLICATION;

        if (!check_bitrix_sessid()) {
            return false;
        }

        try {
            // Удаляем агенты
            $this->removeAgents();

            // Опрос только для удаления HL (обычно рекомендуется оставлять данные)
            if (ФЛОЧ) {
                $this->removeHighloadBlocks();
            }

            ModuleManager::unregisterModule($this->MODULE_ID);

            $APPLICATION->IncludeAdminFile(
                'Удаление ' . $this->MODULE_NAME,
                __DIR__ . '/install/uninstall_success.php'
            );
        } catch (\Exception $e) {
            $APPLICATION->ThrowException($e->getMessage());
            return false;
        }

        return true;
    }

    private function setupHighloadBlocks(): void
    {
        // Все HL-блоки создаются в HlConfig
        // Ниже пока плацехолдер
    }

    private function removeHighloadBlocks(): void
    {
        // Опрос данные с HL
    }

    private function setupAgents(): void
    {
        // Регистрируем MigratorAgent
        \CAgent::AddAgent(
            "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
            $this->MODULE_ID,
            "N",
            60
        );
    }

    private function removeAgents(): void
    {
        // Удаляем агенты
        \CAgent::RemoveModuleAgents($this->MODULE_ID);
    }
}
