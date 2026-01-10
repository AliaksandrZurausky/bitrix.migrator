<?php

namespace BitrixMigrator;

use Bitrix\Main\Localization\Loc;

Loc::loadMessages(__FILE__);

class EventHandlers
{
    public static function OnBuildGlobalMenu(&$aGlobalMenu, &$aModuleMenu)
    {
        $moduleId = 'bitrix_migrator';
        
        if (!\Bitrix\Main\Loader::includeModule($moduleId)) {
            return;
        }

        $aModuleMenu[] = [
            'parent_menu' => 'global_menu_services',
            'section' => $moduleId,
            'sort' => 100,
            'text' => Loc::getMessage('BITRIX_MIGRATOR_MENU_TEXT'),
            'title' => Loc::getMessage('BITRIX_MIGRATOR_MENU_TITLE'),
            'url' => 'bitrix_migrator.php',
            'icon' => 'sys_menu_icon',
            'page_icon' => 'sys_page_icon',
            'items_id' => $moduleId . '_menu',
            'items' => []
        ];
    }
}
