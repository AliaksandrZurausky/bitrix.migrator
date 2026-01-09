<?php

use Bitrix\Main\Localization\Loc;

if (!defined('B_PROLOG_INCLUDED') || B_PROLOG_INCLUDED !== true) { die(); }

Loc::loadMessages('install.php');

global $APPLICATION;
$exception = $APPLICATION->GetException();

if ($exception) {
    CAdminMessage::ShowMessage(
        Loc::getMessage('BITRIX_MIGRATOR_INSTALL_FAILED') . ': ' . $exception->GetString()
    );
} else {
    CAdminMessage::ShowNote(
        Loc::getMessage('BITRIX_MIGRATOR_INSTALL_SUCCESS')
    );
}
?>

<div style="margin-top: 20px;">
    <button type="button" class="adm-btn" onclick="javascript:history.back();">
        <?php echo Loc::getMessage('BITRIX_MIGRATOR_RETURN_MODULES'); ?>
    </button>
</div>
