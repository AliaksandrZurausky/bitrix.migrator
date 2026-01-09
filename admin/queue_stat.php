<?php

define('ADMIN_MODULE_NAME', 'bitrix_migrator');

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_admin.php');

use Bitrix\Main\Loader;
use Bitrix\Main\Web\Json;
use Bitrix\Main\Application;

Loader::includeModule('bitrix_migrator');

$request = Application::getInstance()->getContext()->getRequest();
header('Content-Type: application/json; charset=utf-8');

try {
    $queueRepo = new \BitrixMigrator\Repository\Hl\QueueRepository();
    
    // Получение статистики (приближенная реализация)
    // Нужно добавить методы в QueueRepository
    
    echo Json::encode([
        'success' => true,
        'data' => [
            'total' => 0,  // требуется getTotal()
            'completed' => 0,  // требуется getCompleted()
            'pending' => 0,  // требуется getPending()
            'errors' => 0,  // требуется getErrors()
        ]
    ]);
} catch (\Exception $e) {
    echo Json::encode([
        'success' => false,
        'error' => $e->getMessage()
    ]);
}

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php');
