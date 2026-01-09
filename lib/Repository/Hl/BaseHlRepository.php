<?php

namespace BitrixMigrator\Repository\Hl;

use Bitrix\Main\Loader;
use Bitrix\Main\Config\Option;
use Bitrix\Highloadblock\HighloadBlockTable;
use BitrixMigrator\Config\Module;

abstract class BaseHlRepository
{
    protected $dataClass;

    public function __construct(string $hlOptionKey)
    {
        Loader::includeModule('highloadblock');

        $hlBlockId = (int)Option::get(Module::ID, $hlOptionKey, 0);
        if ($hlBlockId <= 0) {
            throw new \RuntimeException('HL block not found for key: ' . $hlOptionKey);
        }

        $hlBlock = HighloadBlockTable::getById($hlBlockId)->fetch();
        if (!$hlBlock) {
            throw new \RuntimeException('HL block not found by ID: ' . $hlBlockId);
        }

        $entity = HighloadBlockTable::compileEntity($hlBlock);
        $this->dataClass = $entity->getDataClass();
    }

    protected function getDataClass()
    {
        return $this->dataClass;
    }
}
