<?php

use Bitrix\Main\Localization\Loc;

if (!defined('B_PROLOG_INCLUDED') || B_PROLOG_INCLUDED !== true) { die(); }

Loc::loadMessages('install.php');
?>

<div class="migrator-install-page">
    <div class="migrator-alert migrator-alert--warning">
        <div class="migrator-alert-icon">&#9888;</div>
        <div>
            <strong>Удаление модуля Bitrix Migrator</strong>
            <p>Модуль будет деактивирован, административная страница и файлы AJAX будут удалены.</p>
        </div>
    </div>

    <form method="POST" style="margin-top:20px;">
        <?= bitrix_sessid_post() ?>
        <input type="hidden" name="uninstall_step" value="final">

        <div class="migrator-uninstall-option">
            <label class="migrator-checkbox-label">
                <input type="checkbox" name="delete_data" value="Y">
                <span>
                    <strong><?= Loc::getMessage('BITRIX_MIGRATOR_DELETE_DATA') ?></strong>
                    <small><?= Loc::getMessage('BITRIX_MIGRATOR_DELETE_DATA_HINT') ?></small>
                </span>
            </label>
        </div>

        <div style="display:flex;gap:10px;margin-top:24px;">
            <button type="submit" name="delete" value="Y" class="migrator-btn migrator-btn--danger">
                <?= Loc::getMessage('BITRIX_MIGRATOR_CONFIRM_DELETE') ?>
            </button>
            <button type="button" class="migrator-btn migrator-btn--secondary" onclick="history.back();">
                <?= Loc::getMessage('BITRIX_MIGRATOR_CANCEL') ?>
            </button>
        </div>
    </form>
</div>

<style>
.migrator-install-page { max-width: 600px; margin: 20px 0; }
.migrator-alert { display: flex; gap: 12px; padding: 16px 20px; border-radius: 8px; font-size: 14px; line-height: 1.5; }
.migrator-alert p { margin: 4px 0 0; font-size: 13px; opacity: 0.85; }
.migrator-alert--warning { background: #fff3cd; border: 1px solid #ffc107; color: #856404; }
.migrator-alert-icon { font-size: 20px; flex-shrink: 0; }
.migrator-uninstall-option { background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 8px; padding: 16px; }
.migrator-checkbox-label { display: flex; gap: 10px; align-items: flex-start; cursor: pointer; }
.migrator-checkbox-label input[type="checkbox"] { width: 18px; height: 18px; margin-top: 2px; flex-shrink: 0; cursor: pointer; }
.migrator-checkbox-label span { display: flex; flex-direction: column; gap: 2px; }
.migrator-checkbox-label small { color: #999; font-size: 12px; }
.migrator-btn { display: inline-block; padding: 10px 20px; border-radius: 6px; font-size: 13px; text-decoration: none; cursor: pointer; border: none; font-weight: 500; transition: all 0.2s; }
.migrator-btn--danger { background: #dc3545; color: #fff; }
.migrator-btn--danger:hover { background: #c82333; color: #fff; }
.migrator-btn--secondary { background: #f5f5f5; color: #333; border: 1px solid #d0d0d0; }
.migrator-btn--secondary:hover { background: #e8e8e8; }
</style>
