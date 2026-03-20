<?php

use Bitrix\Main\Localization\Loc;

if (!defined('B_PROLOG_INCLUDED') || B_PROLOG_INCLUDED !== true) { die(); }

Loc::loadMessages('install.php');

global $APPLICATION;
$exception = $APPLICATION->GetException();
?>

<div class="migrator-install-page">
    <?php if ($exception): ?>
        <div class="migrator-alert migrator-alert--error">
            <div class="migrator-alert-icon">&#10006;</div>
            <div>
                <strong><?= Loc::getMessage('BITRIX_MIGRATOR_UNINSTALL_FAILED') ?></strong>
                <p><?= $exception->GetString() ?></p>
            </div>
        </div>
    <?php else: ?>
        <div class="migrator-alert migrator-alert--success">
            <div class="migrator-alert-icon">&#10004;</div>
            <div>
                <strong><?= Loc::getMessage('BITRIX_MIGRATOR_UNINSTALL_SUCCESS') ?></strong>
            </div>
        </div>
    <?php endif; ?>

    <div style="margin-top:20px;">
        <a href="/bitrix/admin/module_admin.php" class="migrator-btn migrator-btn--secondary">
            <?= Loc::getMessage('BITRIX_MIGRATOR_RETURN_MODULES') ?>
        </a>
    </div>
</div>

<style>
.migrator-install-page { max-width: 600px; margin: 20px 0; }
.migrator-alert { display: flex; gap: 12px; padding: 16px 20px; border-radius: 8px; font-size: 14px; line-height: 1.5; }
.migrator-alert p { margin: 4px 0 0; font-size: 13px; opacity: 0.85; }
.migrator-alert--success { background: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
.migrator-alert--error { background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }
.migrator-alert-icon { font-size: 20px; flex-shrink: 0; }
.migrator-btn { display: inline-block; padding: 10px 20px; border-radius: 6px; font-size: 13px; text-decoration: none; cursor: pointer; border: none; font-weight: 500; transition: all 0.2s; }
.migrator-btn--secondary { background: #f5f5f5; color: #333; border: 1px solid #d0d0d0; }
.migrator-btn--secondary:hover { background: #e8e8e8; }
</style>
