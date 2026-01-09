<?php
/**
 * Этот скрипт устанавливает файл админа в /bitrix/admin/
 * Нужно запустить в консоли:
 * php /path/to/bitrix/local/modules/bitrix_migrator/install_admin_file.php
 */

$sourceFile = __DIR__ . '/admin/bitrix_migrator.php';
$targetFile = realpath(__DIR__ . '/../../..') . '/bitrix/admin/bitrix_migrator.php';

if (!file_exists($sourceFile)) {
    die("Source file not found: $sourceFile\n");
}

if (copy($sourceFile, $targetFile)) {
    echo "Admin file installed successfully!\n";
    echo "File: $targetFile\n";
} else {
    die("Failed to copy file to $targetFile\n");
}
