<?php

use Bitrix\Main\Localization\Loc;

Loc::loadMessages(__FILE__);

?>

<div class="adm-info-message">
    <div class="adm-info-message-text">
        <?php echo Loc::getMessage('INSTALL_SUCCESS'); ?>
    </div>
</div>

<p>
    <?php echo Loc::getMessage('INSTALL_INSTRUCTIONS'); ?>
</p>

<ul>
    <li><?php echo Loc::getMessage('INSTALL_STEP_1'); ?></li>
    <li><?php echo Loc::getMessage('INSTALL_STEP_2'); ?></li>
    <li><?php echo Loc::getMessage('INSTALL_STEP_3'); ?></li>
</ul>

<div style="margin-top: 20px;">
    <button type="button" class="adm-btn" onclick="window.location.href='/bitrix/admin/settings.php';">
        <span><?php echo Loc::getMessage('BACK_BUTTON'); ?></span>
    </button>
</div>
