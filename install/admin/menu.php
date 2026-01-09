<?php
/**
 * Меню Админа
 * Находится в /bitrix/admin/ и include-ит этот файл:
 * Location: /local/modules/bitrix_migrator/install/admin/menu.php
 */

$aMenu = [
    'parent_menu' => 'global_menu_settings',
    'sort' => 150,
    'text' => 'Bitrix Migrator',
    'title' => 'Migrator - Миграция данных',
    'icon' => 'sys_icon',
    'page_icon' => 'sys_icon',
    'items_id' => 'menu_bitrix_migrator',
    'items' => [
        [
            'text' => 'Настройки миграции',
            'url' => '/bitrix/admin/bitrix_migrator.php?tab=settings',
            'more_url' => [
                '/bitrix/admin/bitrix_migrator.php',
                '/bitrix/admin/bitrix_migrator.php?tab=queue',
                '/bitrix/admin/bitrix_migrator.php?tab=logs',
            ]
        ]
    ]
];

return $aMenu;
