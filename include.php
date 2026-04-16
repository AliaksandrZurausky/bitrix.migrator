<?php

use Bitrix\Main\Loader;

Loader::registerAutoloadClasses(
    'bitrix_migrator',
    array(
        'BitrixMigrator\\Agent\\MigratorAgent' => 'lib/Agent/MigratorAgent.php',
        'BitrixMigrator\\Agent\\DryRunAgent' => 'lib/Agent/DryRunAgent.php',
        'BitrixMigrator\\Service\\StateService' => 'lib/Service/StateService.php',
        'BitrixMigrator\\Service\\QueueService' => 'lib/Service/QueueService.php',
        'BitrixMigrator\\Service\\MapService' => 'lib/Service/MapService.php',
        'BitrixMigrator\\Service\\LogService' => 'lib/Service/LogService.php',
        'BitrixMigrator\\Service\\DryRunService' => 'lib/Service/DryRunService.php',
        'BitrixMigrator\\Service\\MigrationStoppedException' => 'lib/Service/MigrationStoppedException.php',
        'BitrixMigrator\\Service\\TaskMigrationService' => 'lib/Service/TaskMigrationService.php',
        'BitrixMigrator\\Service\\MigrationService' => 'lib/Service/MigrationService.php',
        'BitrixMigrator\\Service\\BoxD7Service' => 'lib/Service/BoxD7Service.php',
        'BitrixMigrator\\Service\\RepairService' => 'lib/Service/RepairService.php',
        'BitrixMigrator\\Integration\\CloudAPI' => 'lib/Integration/CloudAPI.php',
        'BitrixMigrator\\EventHandlers' => 'lib/EventHandlers.php',
        'Bitrix\\Migrator\\Controller\\Migrator' => 'lib/Controller/Migrator.php',
    )
);
