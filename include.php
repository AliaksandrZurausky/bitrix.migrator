<?php

use Bitrix\Main\Loader;

Loader::registerAutoLoadClasses(
    'bitrix_migrator',
    [
        'BitrixMigrator\\Agent\\MigratorAgent' => 'lib/Agent/MigratorAgent.php',
        'BitrixMigrator\\Config\\Module' => 'lib/Config/Module.php',
        'BitrixMigrator\\Service\\TaskRunner' => 'lib/Service/TaskRunner.php',
    ]
);
