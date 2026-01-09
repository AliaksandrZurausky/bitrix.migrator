<?php
/**
 * Admin file for Bitrix Migrator
 * 
 * This file should be copied to /bitrix/admin/ during module installation.
 * Location: /bitrix/admin/bitrix_migrator.php
 * 
 * During install.php execution:
 * @example copy(__DIR__ . '/../admin_install/bitrix_migrator.php', $_SERVER['DOCUMENT_ROOT'] . '/bitrix/admin/bitrix_migrator.php');
 */

// Proxy to the actual admin file in module directory
require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/bitrix_migrator/admin/bitrix_migrator.php');
