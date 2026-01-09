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
    
    $data = [
        'total' => $queueRepo->getTotal(),
        'completed' => $queueRepo->getCompleted(),
        'pending' => $queueRepo->getPending(),
        'errors' => $queueRepo->getErrors(),
        'statsByType' => $queueRepo->getStatsByEntityType(),
    ];
    
    echo Json::encode([
        'success' => true,
        'data' => $data
    ]);
} catch (\Exception $e) {
    echo Json::encode([
        'success' => false,
        'error' => $e->getMessage()
    ]);
}

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php');
