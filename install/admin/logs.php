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
    $logRepo = new \BitrixMigrator\Repository\Hl\LogRepository();
    
    $limit = (int)$request->get('limit') ?: 50;
    $offset = (int)$request->get('offset') ?: 0;
    $level = $request->get('level') ?: null;
    
    $logs = $logRepo->getLogs($limit, $offset, $level);
    $total = $logRepo->getLogsCount($level);
    
    $data = [];
    foreach ($logs as $log) {
        $data[] = [
            'id' => $log['ID'],
            'date' => $log['DATE_CREATE']->toString() ?? '',
            'level' => $log['UF_LEVEL'],
            'scope' => $log['UF_SCOPE'] ?? '',
            'message' => $log['UF_MESSAGE'] ?? '',
            'entity' => $log['UF_ENTITY'] ?? '',
            'cloudId' => $log['UF_CLOUD_ID'] ?? '',
            'boxId' => $log['UF_BOX_ID'] ?? '',
        ];
    }
    
    echo Json::encode([
        'success' => true,
        'data' => $data,
        'total' => $total,
        'limit' => $limit,
        'offset' => $offset,
    ]);
} catch (\Exception $e) {
    echo Json::encode([
        'success' => false,
        'error' => $e->getMessage()
    ]);
}

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php');
