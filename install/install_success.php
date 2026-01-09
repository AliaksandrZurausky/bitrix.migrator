<?php

use Bitrix\Main\Localization\Loc;

Loc::loadMessages('install.php');
?>

<div class="adm-info-message">
    <div class="adm-info-message-text">
        ✓ <?php echo Loc::getMessage('BITRIX_MIGRATOR_NAME'); ?> успешно установлен
    </div>
</div>

<p style="margin-top: 20px;">
    Модуль готов к работе. Перейдите в <strong>Admin > Settings > Bitrix Migrator</strong> для настройки.
</p>

<div style="margin-top: 30px;">
    <button type="button" class="adm-btn" onclick="window.location.href='/bitrix/admin/settings.php';">
        <span>← Вернуться в админку</span>
    </button>
</div>
