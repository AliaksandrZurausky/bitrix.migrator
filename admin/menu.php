<?php

use Bitrix\Main\Localization\Loc;

Loc::loadMessages(__FILE__);

$aMenu = [
    'parent_menu' => 'global_menu_settings',
    'sort' => 150,
    'text' => Loc::getMessage('MENU_TITLE'),
    'title' => Loc::getMessage('MENU_TOOLTIP'),
    'icon' => 'sys_icon',
    'page_icon' => 'sys_icon',
    'items_id' => 'menu_bitrix_migrator',
    'items' => [
        [
            'text' => Loc::getMessage('MENU_SETTINGS'),
            'url' => '/bitrix/admin/bitrix_migrator_bitrix_migrator.php?tab=settings',
            'more_url' => [
                '/bitrix/admin/bitrix_migrator_bitrix_migrator.php',
                '/bitrix/admin/bitrix_migrator_bitrix_migrator.php?tab=queue',
                '/bitrix/admin/bitrix_migrator_bitrix_migrator.php?tab=logs',
            ]
        ]
    ]
];

return $aMenu;
