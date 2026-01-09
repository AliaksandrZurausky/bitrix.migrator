<?php
if (!check_bitrix_sessid()) {
    return;
}

IncludeModuleLangFile(__FILE__);

$module_id = 'bitrix_migrator';
?>

<form action="<?= $APPLICATION->GetCurPage() ?>">
    <?= bitrix_sessid_post() ?>
    <input type="hidden" name="lang" value="<?= LANGUAGE_ID ?>">
    <input type="hidden" name="id" value="<?= htmlspecialcharsbx($module_id) ?>">
    <input type="hidden" name="uninstall" value="Y">
    <input type="hidden" name="step" value="2">

    <p>Вы уверены, что хотите удалить модуль "Bitrix Migrator"?</p>

    <p>
        <input type="checkbox" name="savedata" id="savedata" value="Y" checked>
        <label for="savedata">Сохранить данные (HL-блоки и настройки)</label>
    </p>

    <p>
        <input type="submit" name="inst" value="Удалить">
    </p>
</form>
