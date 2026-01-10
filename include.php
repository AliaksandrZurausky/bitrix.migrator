<?php

use Bitrix\Main\Loader;

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
        'Bitrix\\Migrator\\Controller\\Migrator' => 'lib/Controller/Migrator.php',
    )
);
