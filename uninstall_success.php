<?php

use Bitrix\Main\Localization\Loc;

Loc::loadMessages(__FILE__);

?>

<div class="adm-info-message">
    <div class="adm-info-message-text">
        <?php echo Loc::getMessage('UNINSTALL_SUCCESS'); ?>
    </div>
</div>

<p>
    <?php echo Loc::getMessage('UNINSTALL_INSTRUCTIONS'); ?>
</p>

<div style="margin-top: 20px;">
    <button type="button" class="adm-btn" onclick="window.location.href='/bitrix/admin/settings.php';">
        <span><?php echo Loc::getMessage('BACK_BUTTON'); ?></span>
    </button>
</div>
