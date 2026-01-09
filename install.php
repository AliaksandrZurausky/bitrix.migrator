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

            // Копируем админ-файлы в /bitrix/admin/
            $this->copyAdminFiles();

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
            // Удаляем админ-файлы из /bitrix/admin/
            $this->removeAdminFiles();

            // Удаляем агенты
            $this->removeAgents();

            // Опрос только для удаления HL (обычно рекомендуется оставлять данные)
            // if (ФЛОЧ) {
            //     $this->removeHighloadBlocks();
            // }

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

    private function copyAdminFiles(): void
    {
        $sourceDir = __DIR__ . '/admin_install/';
        $targetDir = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/';

        if (!is_dir($sourceDir)) {
            return;
        }

        $files = [
            'bitrix_migrator.php',
            'menu.php',
            'queue_stat.php',
            'logs.php',
        ];

        foreach ($files as $file) {
            $source = $sourceDir . $file;
            $target = $targetDir . $file;

            if (file_exists($source)) {
                @copy($source, $target);
            }
        }

        // Копируем JS
        $jsSourceDir = __DIR__ . '/admin/js/';
        $jsTargetDir = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/js/';

        if (!is_dir($jsTargetDir)) {
            @mkdir($jsTargetDir, 0755, true);
        }

        if (file_exists($jsSourceDir . 'migrator.js')) {
            @copy($jsSourceDir . 'migrator.js', $jsTargetDir . 'bitrix_migrator.js');
        }
    }

    private function removeAdminFiles(): void
    {
        $adminDir = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/';

        $files = [
            'bitrix_migrator.php',
            'menu.php',
            'queue_stat.php',
            'logs.php',
        ];

        foreach ($files as $file) {
            $path = $adminDir . $file;
            if (file_exists($path)) {
                @unlink($path);
            }
        }

        // Удаляем JS
        $jsPath = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/js/bitrix_migrator.js';
        if (file_exists($jsPath)) {
            @unlink($jsPath);
        }
    }

    private function setupHighloadBlocks(): void
    {
        // Все HL-блоки создаются в HlConfig
        // Ниже пока плацехолдер
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
