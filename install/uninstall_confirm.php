<?php

use Bitrix\Main\Localization\Loc;

if (!defined('B_PROLOG_INCLUDED') || B_PROLOG_INCLUDED !== true) { die(); }

Loc::loadMessages('install.php');
?>

<form method="POST">
    <?php echo bitrix_sessid_post(); ?>
    <input type="hidden" name="uninstall_step" value="final">
    
    <div style="margin: 20px 0;">
        <label>
            <input type="checkbox" name="delete_data" value="Y"> 
            <strong><?php echo Loc::getMessage('BITRIX_MIGRATOR_DELETE_DATA'); ?></strong>
        </label>
        <p style="color: #999; font-size: 12px; margin: 5px 0 0 20px;">
            <?php echo Loc::getMessage('BITRIX_MIGRATOR_DELETE_DATA_HINT'); ?>
        </p>
    </div>

    <div style="margin-top: 30px;">
        <button type="submit" class="adm-btn-red" name="delete" value="Y">
            <span><?php echo Loc::getMessage('BITRIX_MIGRATOR_CONFIRM_DELETE'); ?></span>
        </button>
        <button type="button" class="adm-btn" onclick="javascript:history.back();" style="margin-left: 10px;">
            <span><?php echo Loc::getMessage('BITRIX_MIGRATOR_CANCEL'); ?></span>
        </button>
    </div>
</form>

<style>
    .adm-btn-red {
        background-color: #ff4444;
        color: white;
        border: none;
        padding: 8px 16px;
        border-radius: 4px;
        cursor: pointer;
        font-size: 12px;
    }
    
    .adm-btn-red:hover {
        background-color: #cc0000;
    }
</style>
