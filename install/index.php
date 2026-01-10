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

    public function __construct()
    {
        $arModuleVersion = [];
        include(__DIR__ . '/version.php');

        $this->MODULE_VERSION = $arModuleVersion['VERSION'];
        $this->MODULE_VERSION_DATE = $arModuleVersion['VERSION_DATE'];
        $this->MODULE_NAME = Loc::getMessage('BITRIX_MIGRATOR_MODULE_NAME');
        $this->MODULE_DESCRIPTION = Loc::getMessage('BITRIX_MIGRATOR_MODULE_DESC');
        $this->PARTNER_NAME = Loc::getMessage('BITRIX_MIGRATOR_PARTNER_NAME');
        $this->PARTNER_URI = Loc::getMessage('BITRIX_MIGRATOR_PARTNER_URI');
    }

    public function DoInstall()
    {
        global $APPLICATION;

        if (CheckVersion(ModuleManager::getVersion('main'), '14.00.00')) {
            $this->InstallFiles();
            $this->InstallDB();
            $this->InstallEvents();

            ModuleManager::registerModule($this->MODULE_ID);

            $APPLICATION->IncludeAdminFile(
                Loc::getMessage('BITRIX_MIGRATOR_INSTALL_TITLE'),
                __DIR__ . '/step.php'
            );
        } else {
            $APPLICATION->ThrowException(
                Loc::getMessage('BITRIX_MIGRATOR_INSTALL_ERROR_VERSION')
            );
        }

        return true;
    }

    public function DoUninstall()
    {
        global $APPLICATION;

        $this->UnInstallEvents();
        $this->UnInstallDB();
        $this->UnInstallFiles();

        ModuleManager::unRegisterModule($this->MODULE_ID);

        $APPLICATION->IncludeAdminFile(
            Loc::getMessage('BITRIX_MIGRATOR_UNINSTALL_TITLE'),
            __DIR__ . '/unstep.php'
        );

        return true;
    }

    public function InstallDB()
    {
        return true;
    }

    public function UnInstallDB()
    {
        Option::delete($this->MODULE_ID);
        return true;
    }

    public function InstallEvents()
    {
        $eventManager = EventManager::getInstance();
        return true;
    }

    public function UnInstallEvents()
    {
        $eventManager = EventManager::getInstance();
        return true;
    }

    public function InstallFiles()
    {
        // Copy admin menu file
        CopyDirFiles(
            __DIR__ . '/admin/',
            Application::getDocumentRoot() . '/bitrix/admin/',
            true,
            true
        );

        // Copy AJAX files to /local/ajax/bitrix_migrator/
        $ajaxSourceDir = dirname(__DIR__) . '/ajax';
        $ajaxTargetDir = Application::getDocumentRoot() . '/local/ajax/' . $this->MODULE_ID;

        if (is_dir($ajaxSourceDir)) {
            if (!is_dir($ajaxTargetDir)) {
                Directory::createDirectory($ajaxTargetDir);
            }

            CopyDirFiles($ajaxSourceDir, $ajaxTargetDir, true, true);
        }

        return true;
    }

    public function UnInstallFiles()
    {
        // Remove admin menu file
        DeleteDirFiles(
            __DIR__ . '/admin/',
            Application::getDocumentRoot() . '/bitrix/admin/'
        );

        // Remove AJAX files
        $ajaxTargetDir = Application::getDocumentRoot() . '/local/ajax/' . $this->MODULE_ID;
        if (is_dir($ajaxTargetDir)) {
            Directory::deleteDirectory($ajaxTargetDir);
        }

        return true;
    }
}