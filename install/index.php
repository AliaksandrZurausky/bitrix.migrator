<?php

use Bitrix\Main\Localization\Loc;
use Bitrix\Main\ModuleManager;
use Bitrix\Main\Config\Option;

Loc::loadMessages(__FILE__);

class bitrix_migrator extends CModule
{
    public $MODULE_ID = 'bitrix_migrator';
    public $MODULE_NAME = 'Bitrix Migrator';
    public $MODULE_DESCRIPTION = 'Миграция данных из облака в коробку';
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

            // Копируем прокси-файлы в /bitrix/admin/
            $this->installAdminProxyFiles();

            // Регистрируем агентов
            $this->setupAgents();

            $APPLICATION->IncludeAdminFile(
                Loc::getMessage('INSTALL_TITLE'),
                __DIR__ . '/install_success.php'
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
            // Удаляем агентов
            $this->removeAgents();

            // Удаляем прокси-файлы
            $this->uninstallAdminProxyFiles();

            ModuleManager::unregisterModule($this->MODULE_ID);

            $APPLICATION->IncludeAdminFile(
                Loc::getMessage('UNINSTALL_TITLE'),
                __DIR__ . '/uninstall_success.php'
            );
        } catch (\Exception $e) {
            $APPLICATION->ThrowException($e->getMessage());
            return false;
        }

        return true;
    }

    /**
     * Создаёт прокси-файлы в /bitrix/admin/ вместо копирования исходников
     * Это стандартный подход в Битриксе - файл содержит только require основного файла
     */
    private function installAdminProxyFiles(): void
    {
        $adminDir = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/';
        $moduleAdminDir = __DIR__ . '/../admin/';

        // Прокси-файлы для админ страниц
        $proxyFiles = [
            'bitrix_migrator.php',
            'queue_stat.php',
            'logs.php',
        ];

        foreach ($proxyFiles as $file) {
            $targetPath = $adminDir . 'bitrix_migrator_' . $file;
            $sourceFile = $moduleAdminDir . $file;

            if (file_exists($sourceFile)) {
                $proxyContent = "<?php\nrequire_once(\$_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/bitrix_migrator/admin/" . $file . "');\n";
                @file_put_contents($targetPath, $proxyContent);
            }
        }

        // Прокси для menu.php - специальный случай
        $menuProxyPath = $adminDir . 'bitrix_migrator_menu.php';
        $menuSourcePath = $moduleAdminDir . 'menu.php';

        if (file_exists($menuSourcePath)) {
            $menuProxyContent = "<?php\nrequire_once(\$_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/bitrix_migrator/admin/menu.php');\n";
            @file_put_contents($menuProxyPath, $menuProxyContent);
        }
    }

    /**
     * Удаляет прокси-файлы из /bitrix/admin/
     */
    private function uninstallAdminProxyFiles(): void
    {
        $adminDir = $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/';

        $proxyFiles = [
            'bitrix_migrator_bitrix_migrator.php',
            'bitrix_migrator_queue_stat.php',
            'bitrix_migrator_logs.php',
            'bitrix_migrator_menu.php',
        ];

        foreach ($proxyFiles as $file) {
            $path = $adminDir . $file;
            if (file_exists($path)) {
                @unlink($path);
            }
        }
    }

    /**
     * Регистрирует агентов для периодического запуска
     */
    private function setupAgents(): void
    {
        if (class_exists('CAgent')) {
            \CAgent::AddAgent(
                "\\BitrixMigrator\\Agent\\MigratorAgent::run();",
                $this->MODULE_ID,
                "N",
                60
            );
        }
    }

    /**
     * Удаляет агентов модуля
     */
    private function removeAgents(): void
    {
        if (class_exists('CAgent')) {
            \CAgent::RemoveModuleAgents($this->MODULE_ID);
        }
    }
}
