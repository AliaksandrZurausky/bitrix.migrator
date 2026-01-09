<?php

use Bitrix\Main\Loader;

Loader::registerAutoloadClasses(
    'bitrix_migrator',
    array(
        'BitrixMigrator\Agent\MigratorAgent' => 'lib/Agent/MigratorAgent.php',
        'BitrixMigrator\Service\QueueService' => 'lib/Service/QueueService.php',
        'BitrixMigrator\Service\LogService' => 'lib/Service/LogService.php',
        'BitrixMigrator\Integration\CloudAPI' => 'lib/Integration/CloudAPI.php',
    )
);
