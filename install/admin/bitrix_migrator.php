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
$APPLICATION->AddHeadScript('/local/modules/bitrix_migrator/install/admin/js/script.js');
$APPLICATION->SetAdditionalCSS('/local/modules/bitrix_migrator/install/admin/css/styles.css');

require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_admin_after.php');

// Get current settings
$webhookUrl = Option::get($MODULE_ID, 'webhook_url', '');
$connectionStatus = Option::get($MODULE_ID, 'connection_status', 'not_checked');
$dryrunStatus = Option::get($MODULE_ID, 'dryrun_status', 'idle');
$migrationPlan = Option::get($MODULE_ID, 'migration_plan', '');
$plan = $migrationPlan ? json_decode($migrationPlan, true) : [];
?>

<div class="migrator-container">
    <!-- Tabs Navigation -->
    <div class="migrator-tabs-nav">
        <button class="migrator-tab-btn active" data-tab="connection">
            <?= Loc::getMessage('BITRIX_MIGRATOR_TAB_CONNECTION') ?>
        </button>
        <button class="migrator-tab-btn" data-tab="dryrun">
            <?= Loc::getMessage('BITRIX_MIGRATOR_TAB_DRYRUN') ?>
        </button>
        <button class="migrator-tab-btn" data-tab="plan">
            <?= Loc::getMessage('BITRIX_MIGRATOR_TAB_PLAN') ?>
        </button>
        <button class="migrator-tab-btn" data-tab="migration">
            <?= Loc::getMessage('BITRIX_MIGRATOR_TAB_MIGRATION') ?>
        </button>
        <button class="migrator-tab-btn" data-tab="logs">
            <?= Loc::getMessage('BITRIX_MIGRATOR_TAB_LOGS') ?>
        </button>
    </div>

    <!-- Tab: Connection -->
    <div id="tab-connection" class="migrator-tab-content active">
        <div class="adm-detail-content">
            <h2><?= Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_TITLE') ?></h2>
            
            <div class="migrator-status-block" id="connection-status-block">
                <div class="migrator-status migrator-status-<?= htmlspecialcharsbx($connectionStatus) ?>">
                    <span class="migrator-status-icon"></span>
                    <span class="migrator-status-text" id="connection-status-text">
                        <?php
                        switch ($connectionStatus) {
                            case 'success':
                                echo Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_STATUS_SUCCESS');
                                break;
                            case 'error':
                                echo Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_STATUS_ERROR');
                                break;
                            default:
                                echo Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_STATUS_NOT_CHECKED');
                        }
                        ?>
                    </span>
                </div>
            </div>

            <form id="connection-form" class="migrator-form">
                <?= bitrix_sessid_post() ?>
                
                <div class="migrator-form-group">
                    <label for="webhook_url" class="migrator-label">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_WEBHOOK_URL') ?>:
                        <span class="migrator-required">*</span>
                    </label>
                    <input type="text" 
                           id="webhook_url" 
                           name="webhook_url" 
                           class="migrator-input"
                           value="<?= htmlspecialcharsbx($webhookUrl) ?>" 
                           placeholder="https://your-portal.bitrix24.ru/rest/1/abc123def456/"
                           required>
                    <div class="migrator-hint">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_WEBHOOK_URL_HINT') ?>
                    </div>
                </div>

                <div class="migrator-form-actions">
                    <button type="button" id="btn-save-connection" class="adm-btn-save">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_SAVE') ?>
                    </button>
                    <button type="button" id="btn-check-connection" class="adm-btn">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_CHECK_CONNECTION') ?>
                    </button>
                    <button type="button" id="btn-run-dryrun" class="adm-btn" <?= empty($webhookUrl) ? 'disabled' : '' ?>>
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_RUN_DRYRUN') ?>
                    </button>
                </div>
            </form>
        </div>
    </div>

    <!-- Tab: Dry Run -->
    <div id="tab-dryrun" class="migrator-tab-content">
        <div class="adm-detail-content">
            <h2><?= Loc::getMessage('BITRIX_MIGRATOR_DRYRUN_TITLE') ?></h2>
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_DRYRUN_INFO') ?></p>

            <div id="dryrun-progress-block" class="migrator-progress-block" style="display:none;">
                <div class="migrator-progress-bar">
                    <div id="dryrun-progress-fill" class="migrator-progress-fill" style="width:0%"></div>
                </div>
                <div id="dryrun-progress-text" class="migrator-progress-text">0%</div>
            </div>

            <div id="dryrun-results-block" style="display:none;">
                <h3><?= Loc::getMessage('BITRIX_MIGRATOR_DRYRUN_RESULTS_TITLE') ?></h3>
                <table class="migrator-results-table" id="dryrun-results-table">
                    <thead>
                        <tr>
                            <th><?= Loc::getMessage('BITRIX_MIGRATOR_TABLE_ENTITY') ?></th>
                            <th><?= Loc::getMessage('BITRIX_MIGRATOR_TABLE_COUNT') ?></th>
                            <th><?= Loc::getMessage('BITRIX_MIGRATOR_TABLE_STATUS') ?></th>
                        </tr>
                    </thead>
                    <tbody id="dryrun-results-tbody">
                    </tbody>
                </table>

                <div class="migrator-form-actions">
                    <button type="button" id="btn-goto-plan" class="adm-btn-save">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_GOTO_PLAN') ?>
                    </button>
                </div>
            </div>
        </div>
    </div>

    <!-- Tab: Migration Plan -->
    <div id="tab-plan" class="migrator-tab-content">
        <div class="adm-detail-content">
            <h2><?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_TITLE') ?></h2>
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_INFO') ?></p>

            <form id="plan-form" class="migrator-form">
                <div class="migrator-form-group">
                    <label class="migrator-label">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_ENTITIES') ?>:
                    </label>
                    <div id="plan-entities-list" class="migrator-checkbox-list">
                        <!-- Will be filled by JS -->
                    </div>
                </div>

                <div class="migrator-form-group">
                    <label for="plan-batch-size" class="migrator-label">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_BATCH_SIZE') ?>:
                    </label>
                    <select id="plan-batch-size" name="batchSize" class="migrator-select">
                        <option value="50" <?= ($plan['batchSize'] ?? 100) == 50 ? 'selected' : '' ?>>50</option>
                        <option value="100" <?= ($plan['batchSize'] ?? 100) == 100 ? 'selected' : '' ?>>100</option>
                        <option value="200" <?= ($plan['batchSize'] ?? 100) == 200 ? 'selected' : '' ?>>200</option>
                    </select>
                    <div class="migrator-hint">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_BATCH_SIZE_HINT') ?>
                    </div>
                </div>

                <div class="migrator-form-actions">
                    <button type="button" id="btn-save-plan" class="adm-btn-save">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_SAVE_PLAN') ?>
                    </button>
                    <button type="button" id="btn-goto-migration" class="adm-btn">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_GOTO_MIGRATION') ?>
                    </button>
                </div>
            </form>
        </div>
    </div>

    <!-- Tab: Migration -->
    <div id="tab-migration" class="migrator-tab-content">
        <div class="adm-detail-content">
            <h2><?= Loc::getMessage('BITRIX_MIGRATOR_MIGRATION_TITLE') ?></h2>
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_MIGRATION_INFO') ?></p>
        </div>
    </div>

    <!-- Tab: Logs -->
    <div id="tab-logs" class="migrator-tab-content">
        <div class="adm-detail-content">
            <h2><?= Loc::getMessage('BITRIX_MIGRATOR_LOGS_TITLE') ?></h2>
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_LOGS_INFO') ?></p>
        </div>
    </div>
</div>

<script>
window.BITRIX_MIGRATOR = {
    moduleId: '<?= $MODULE_ID ?>',
    sessid: '<?= bitrix_sessid() ?>',
    dryrunStatus: '<?= $dryrunStatus ?>'
};
</script>

<?php
require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php');
