<?php

use Bitrix\Main\Loader;
use Bitrix\Main\Config\Configuration;

Loader::registerAutoloadClasses(
    'bitrix_migrator',
    array(
        'BitrixMigrator\\Agent\\MigratorAgent' => 'lib/Agent/MigratorAgent.php',
        'BitrixMigrator\\Service\\StateService' => 'lib/Service/StateService.php',
        'BitrixMigrator\\Service\\QueueService' => 'lib/Service/QueueService.php',
        'BitrixMigrator\\Service\\MapService' => 'lib/Service/MapService.php',
        'BitrixMigrator\\Service\\LogService' => 'lib/Service/LogService.php',
        'BitrixMigrator\\Integration\\CloudAPI' => 'lib/Integration/CloudAPI.php',
        'BitrixMigrator\\EventHandlers' => 'lib/EventHandlers.php',
        'BitrixMigrator\\Controller\\Migrator' => 'lib/Controller/Migrator.php',
        'Bitrix\\Migrator\\Controller\\Migrator' => 'lib/Controller/Migrator.php',
    )
);

// Register REST API controllers configuration
$config = Configuration::getInstance('bitrix_migrator');
if (!$config->get('controllers')) {
    $config->add('controllers', [
        'value' => [
            'defaultNamespace' => '\\Bitrix\\Migrator\\Controller',
        ],
        'readonly' => false,
    ]);
}