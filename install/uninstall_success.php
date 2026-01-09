<?php

use Bitrix\Main\Localization\Loc;

Loc::loadMessages('install.php');
?>

<div class="adm-info-message">
    <div class="adm-info-message-text">
        ✓ Модуль Bitrix Migrator успешно удален
    </div>
</div>

<div style="margin-top: 30px;">
    <button type="button" class="adm-btn" onclick="window.location.href='/bitrix/admin/settings.php';">
        <span>← Вернуться в админку</span>
    </button>
</div>
