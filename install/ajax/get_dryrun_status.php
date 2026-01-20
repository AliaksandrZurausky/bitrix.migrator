<?php
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('DisableEventsCheck', true);

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

use Bitrix\Main\Application;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Loader;
use Bitrix\Highloadblock\HighloadBlockTable;

header('Content-Type: application/json');

if (!Loader::includeModule('bitrix_migrator') || !Loader::includeModule('highloadblock')) {
    echo json_encode(['success' => false, 'error' => 'Module not loaded']);
    die();
}

$request = Application::getInstance()->getContext()->getRequest();

if (!check_bitrix_sessid()) {
    echo json_encode(['success' => false, 'error' => 'Invalid session']);
    die();
}

$hlblockId = Option::get('bitrix_migrator', 'dryrun_hlblock_id', 0);

if (!$hlblockId) {
    echo json_encode(['success' => false, 'error' => 'HL block not found']);
    die();
}

try {
    $hlblock = HighloadBlockTable::getById($hlblockId)->fetch();
    if (!$hlblock) {
        throw new Exception('HL block not found');
    }

    $entity = HighloadBlockTable::compileEntity($hlblock);
    $entityClass = $entity->getDataClass();

    $result = $entityClass::getList([
        'select' => ['*'],
        'order' => ['ID' => 'DESC'],
        'limit' => 1
    ])->fetch();

    if ($result && !empty($result['UF_DATA_JSON'])) {
        $data = json_decode($result['UF_DATA_JSON'], true);
        echo json_encode([
            'success' => true,
            'result' => $data
        ]);
    } else {
        echo json_encode([
            'success' => true,
            'result' => null
        ]);
    }
} catch (Exception $e) {
    echo json_encode(['success' => false, 'error' => $e->getMessage()]);
}