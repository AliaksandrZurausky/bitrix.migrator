<?php
require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_admin_before.php');

use Bitrix\Main\Loader;
use Bitrix\Main\Localization\Loc;
use Bitrix\Main\Config\Option;

Loc::loadMessages(__FILE__);

$MODULE_ID = 'bitrix_migrator';

if (!Loader::includeModule($MODULE_ID)) {
    require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_admin_after.php');
    ShowError(Loc::getMessage('BITRIX_MIGRATOR_MODULE_NOT_INSTALLED'));
    require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php');
    die();
}

$APPLICATION->SetTitle(Loc::getMessage('BITRIX_MIGRATOR_PAGE_TITLE'));

require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_admin_after.php');

// Handle form submission
if ($_SERVER['REQUEST_METHOD'] === 'POST' && check_bitrix_sessid()) {
    if (isset($_POST['save']) || isset($_POST['apply'])) {
        Option::set($MODULE_ID, 'cloud_url', $_POST['cloud_url'] ?? '');
        Option::set($MODULE_ID, 'cloud_webhook', $_POST['cloud_webhook'] ?? '');
        
        CAdminMessage::ShowNote(Loc::getMessage('BITRIX_MIGRATOR_SETTINGS_SAVED'));
    }
}

// Get current settings
$cloudUrl = Option::get($MODULE_ID, 'cloud_url', '');
$cloudWebhook = Option::get($MODULE_ID, 'cloud_webhook', '');

// Tab control
$tabControl = new CAdminTabControl('bitrix_migrator_tabs', [
    [
        'DIV' => 'settings',
        'TAB' => Loc::getMessage('BITRIX_MIGRATOR_TAB_SETTINGS'),
        'TITLE' => Loc::getMessage('BITRIX_MIGRATOR_TAB_SETTINGS_TITLE')
    ],
    [
        'DIV' => 'state',
        'TAB' => Loc::getMessage('BITRIX_MIGRATOR_TAB_STATE'),
        'TITLE' => Loc::getMessage('BITRIX_MIGRATOR_TAB_STATE_TITLE')
    ],
    [
        'DIV' => 'logs',
        'TAB' => Loc::getMessage('BITRIX_MIGRATOR_TAB_LOGS'),
        'TITLE' => Loc::getMessage('BITRIX_MIGRATOR_TAB_LOGS_TITLE')
    ]
]);
?>

<form method="POST" action="<?= $APPLICATION->GetCurPage() ?>" name="bitrix_migrator_form">
    <?= bitrix_sessid_post() ?>
    
    <?php $tabControl->Begin(); ?>
    
    <?php $tabControl->BeginNextTab(); ?>
    
    <tr>
        <td width="40%">
            <label for="cloud_url"><?= Loc::getMessage('BITRIX_MIGRATOR_CLOUD_URL') ?>:</label>
        </td>
        <td width="60%">
            <input type="text" 
                   id="cloud_url" 
                   name="cloud_url" 
                   value="<?= htmlspecialcharsbx($cloudUrl) ?>" 
                   size="50" 
                   placeholder="https://your-portal.bitrix24.ru">
        </td>
    </tr>
    
    <tr>
        <td>
            <label for="cloud_webhook"><?= Loc::getMessage('BITRIX_MIGRATOR_CLOUD_WEBHOOK') ?>:</label>
        </td>
        <td>
            <input type="text" 
                   id="cloud_webhook" 
                   name="cloud_webhook" 
                   value="<?= htmlspecialcharsbx($cloudWebhook) ?>" 
                   size="50" 
                   placeholder="1/abc123def456">
            <br>
            <small><?= Loc::getMessage('BITRIX_MIGRATOR_CLOUD_WEBHOOK_HINT') ?></small>
        </td>
    </tr>
    
    <?php $tabControl->BeginNextTab(); ?>
    
    <tr>
        <td colspan="2">
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_STATE_INFO') ?></p>
            <p><strong><?= Loc::getMessage('BITRIX_MIGRATOR_STATE_STATUS') ?>:</strong> 
                <?= Loc::getMessage('BITRIX_MIGRATOR_STATE_NOT_STARTED') ?>
            </p>
        </td>
    </tr>
    
    <?php $tabControl->BeginNextTab(); ?>
    
    <tr>
        <td colspan="2">
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_LOGS_INFO') ?></p>
        </td>
    </tr>
    
    <?php $tabControl->Buttons(); ?>
    
    <input type="submit" name="save" value="<?= Loc::getMessage('BITRIX_MIGRATOR_BTN_SAVE') ?>" class="adm-btn-save">
    <input type="reset" name="reset" value="<?= Loc::getMessage('BITRIX_MIGRATOR_BTN_CANCEL') ?>" onclick="window.location.reload();">
    
    <?php $tabControl->End(); ?>
</form>

<?php
require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php');
