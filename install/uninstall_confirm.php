<?php

use Bitrix\Main\Localization\Loc;

Loc::loadMessages('install.php');
?>

<div class="adm-info-message-warning">
    <div class="adm-info-message-text">
        Вы уверены, что хотите удалить модуль Bitrix Migrator?
    </div>
</div>

<form method="POST" style="margin-top: 20px;">
    <?php echo bitrix_sessid_post(); ?>
    <input type="hidden" name="uninstall_step" value="final">
    
    <div style="margin-bottom: 15px;">
        <label>
            <input type="checkbox" name="delete_data" value="Y"> 
            <strong>Удалить все данные миграции (очереди и логи)</strong>
        </label>
        <p style="color: #999; font-size: 12px; margin: 5px 0 0 20px;">
            Если не отмечено, таблицы будут сохранены
        </p>
    </div>

    <div style="margin-top: 30px;">
        <button type="submit" class="adm-btn-red" name="delete" value="Y">
            <span>✓ Удалить</span>
        </button>
        <button type="button" class="adm-btn" onclick="window.history.back();" style="margin-left: 10px;">
            <span>← Отмена</span>
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
    
    .adm-info-message-warning {
        background-color: #fff3cd;
        border: 1px solid #ffc107;
        color: #856404;
        padding: 10px;
        border-radius: 4px;
    }
</style>
