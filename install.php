<?php

use Bitrix\Main\ModuleManager;
use Bitrix\Main\Loader;

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
            $this->installAdminFiles();

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

            // Удаляем админ-файлы
            $this->uninstallAdminFiles();

            // Опрос только для удаления HL
            // if ($DELETE_HL_BLOCKS) {
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

    private function installAdminFiles(): void
    {
        $sourceDir = __DIR__ . '/install/admin/';
        $adminDir = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/';

        if (!is_dir($sourceDir)) {
            return;
        }

        // Копируем сначала главные файлы
        $mainFiles = [
            'bitrix_migrator.php',
            'menu.php',
            'queue_stat.php',
            'logs.php',
        ];

        foreach ($mainFiles as $file) {
            $source = $sourceDir . $file;
            if (file_exists($source)) {
                $target = $adminDir . $file;
                @copy($source, $target);
            }
        }

        // Копируем JS
        $jsSourceDir = __DIR__ . '/admin/js/';
        $jsTargetDir = $adminDir . 'js/';

        if (!is_dir($jsTargetDir)) {
            @mkdir($jsTargetDir, 0755, true);
        }

        if (file_exists($jsSourceDir . 'migrator.js')) {
            @copy($jsSourceDir . 'migrator.js', $jsTargetDir . 'bitrix_migrator.js');
        }
    }

    private function uninstallAdminFiles(): void
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
        $jsPath = $adminDir . 'js/bitrix_migrator.js';
        if (file_exists($jsPath)) {
            @unlink($jsPath);
        }
    }

    private function setupHighloadBlocks(): void
    {
        // HL-блоки создаются в HlConfig
    }

    private function setupAgents(): void
    {
        \CAgent::AddAgent(
            "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
            $this->MODULE_ID,
            "N",
            60
        );
    }

    private function removeAgents(): void
    {
        \CAgent::RemoveModuleAgents($this->MODULE_ID);
    }
}
