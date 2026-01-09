<?php

use Bitrix\Main\Loader;

Loader::registerAutoLoadClasses(
    'bitrix_migrator',
    [
        // Agent
        'BitrixMigrator\\Agent\\MigratorAgent' => 'lib/Agent/MigratorAgent.php',
        
        // Config
        'BitrixMigrator\\Config\\Module' => 'lib/Config/Module.php',
        'BitrixMigrator\\Config\\HlConfig' => 'lib/Config/HlConfig.php',
        
        // Domain
        'BitrixMigrator\\Domain\\Enum\\EntityType' => 'lib/Domain/Enum/EntityType.php',
        'BitrixMigrator\\Domain\\Enum\\TaskType' => 'lib/Domain/Enum/TaskType.php',
        'BitrixMigrator\\Domain\\Enum\\TaskStatus' => 'lib/Domain/Enum/TaskStatus.php',
        
        // Repository
        'BitrixMigrator\\Repository\\Hl\\BaseHlRepository' => 'lib/Repository/Hl/BaseHlRepository.php',
        'BitrixMigrator\\Repository\\Hl\\QueueRepository' => 'lib/Repository/Hl/QueueRepository.php',
        'BitrixMigrator\\Repository\\Hl\\MapRepository' => 'lib/Repository/Hl/MapRepository.php',
        'BitrixMigrator\\Repository\\Hl\\LogRepository' => 'lib/Repository/Hl/LogRepository.php',
        
        // Integration
        'BitrixMigrator\\Integration\\Cloud\\RestClient' => 'lib/Integration/Cloud/RestClient.php',
        'BitrixMigrator\\Integration\\Cloud\\Api\\DealApi' => 'lib/Integration/Cloud/Api/DealApi.php',
        'BitrixMigrator\\Integration\\Cloud\\Api\\ContactApi' => 'lib/Integration/Cloud/Api/ContactApi.php',
        'BitrixMigrator\\Integration\\Cloud\\Api\\CompanyApi' => 'lib/Integration/Cloud/Api/CompanyApi.php',
        'BitrixMigrator\\Integration\\Cloud\\Api\\LeadApi' => 'lib/Integration/Cloud/Api/LeadApi.php',
        'BitrixMigrator\\Integration\\Cloud\\Api\\TimelineCommentApi' => 'lib/Integration/Cloud/Api/TimelineCommentApi.php',
        'BitrixMigrator\\Integration\\Cloud\\Api\\ActivityApi' => 'lib/Integration/Cloud/Api/ActivityApi.php',
        
        // Writer
        'BitrixMigrator\\Writer\\Box\\CrmWriter' => 'lib/Writer/Box/CrmWriter.php',
        'BitrixMigrator\\Writer\\Box\\TimelineWriter' => 'lib/Writer/Box/TimelineWriter.php',
        
        // Service
        'BitrixMigrator\\Service\\QueueBuilder' => 'lib/Service/QueueBuilder.php',
        'BitrixMigrator\\Service\\TaskRunner' => 'lib/Service/TaskRunner.php',
        'BitrixMigrator\\Service\\Import\\DealImporter' => 'lib/Service/Import/DealImporter.php',
        'BitrixMigrator\\Service\\Import\\ContactImporter' => 'lib/Service/Import/ContactImporter.php',
        'BitrixMigrator\\Service\\Import\\CompanyImporter' => 'lib/Service/Import/CompanyImporter.php',
        'BitrixMigrator\\Service\\Import\\LeadImporter' => 'lib/Service/Import/LeadImporter.php',
        'BitrixMigrator\\Service\\Import\\TimelineImporter' => 'lib/Service/Import/TimelineImporter.php',
    ]
);
