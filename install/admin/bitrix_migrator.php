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
$cloudWebhookUrl = Option::get($MODULE_ID, 'cloud_webhook_url', '');
$boxWebhookUrl = Option::get($MODULE_ID, 'box_webhook_url', '');
$connectionStatusCloud = Option::get($MODULE_ID, 'connection_status_cloud', 'not_checked');
$connectionStatusBox = Option::get($MODULE_ID, 'connection_status_box', 'not_checked');
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
            
            <form id="connection-form" class="migrator-form">
                <?= bitrix_sessid_post() ?>
                
                <!-- Cloud Webhook -->
                <div class="migrator-form-group">
                    <label for="cloud_webhook_url" class="migrator-label">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_CLOUD_WEBHOOK_URL') ?>:
                    </label>
                    <input type="text" 
                           id="cloud_webhook_url" 
                           name="cloud_webhook_url" 
                           class="migrator-input"
                           value="<?= htmlspecialcharsbx($cloudWebhookUrl) ?>" 
                           placeholder="https://your-portal.bitrix24.ru/rest/1/abc123/">
                    <div class="migrator-hint">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_CLOUD_WEBHOOK_URL_HINT') ?>
                    </div>
                    <div class="migrator-status-block" id="connection-status-block-cloud">
                        <div class="migrator-status migrator-status-<?= htmlspecialcharsbx($connectionStatusCloud) ?>">
                            <span class="migrator-status-icon"></span>
                            <span class="migrator-status-text" id="connection-status-text-cloud">
                                <?php
                                switch ($connectionStatusCloud) {
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
                    <button type="button" id="btn-check-connection-cloud" class="adm-btn">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_CHECK_CONNECTION') ?>
                    </button>
                </div>

                <!-- Box Webhook -->
                <div class="migrator-form-group">
                    <label for="box_webhook_url" class="migrator-label">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BOX_WEBHOOK_URL') ?>:
                    </label>
                    <input type="text" 
                           id="box_webhook_url" 
                           name="box_webhook_url" 
                           class="migrator-input"
                           value="<?= htmlspecialcharsbx($boxWebhookUrl) ?>" 
                           placeholder="https://your-box.bitrix24.ru/rest/1/xyz789/">
                    <div class="migrator-hint">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BOX_WEBHOOK_URL_HINT') ?>
                    </div>
                    <div class="migrator-status-block" id="connection-status-block-box">
                        <div class="migrator-status migrator-status-<?= htmlspecialcharsbx($connectionStatusBox) ?>">
                            <span class="migrator-status-icon"></span>
                            <span class="migrator-status-text" id="connection-status-text-box">
                                <?php
                                switch ($connectionStatusBox) {
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
                    <button type="button" id="btn-check-connection-box" class="adm-btn">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_CHECK_CONNECTION') ?>
                    </button>
                </div>

                <div class="migrator-form-actions">
                    <button type="button" id="btn-save-connection" class="adm-btn-save">
                        <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_SAVE') ?>
                    </button>
                    <button type="button" id="btn-run-dryrun" class="adm-btn" <?= empty($cloudWebhookUrl) ? 'disabled' : '' ?>>
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

            <div id="dryrun-results" style="margin-top:20px;">

                <div id="dryrun-no-results" style="display:block;">
                    <p><?= Loc::getMessage('BITRIX_MIGRATOR_DRYRUN_NO_RESULTS') ?></p>
                </div>

                <div id="dryrun-summary" style="display:none;">
                    <h3><?= Loc::getMessage('BITRIX_MIGRATOR_DRYRUN_RESULTS_TITLE') ?></h3>

                    <!-- Structure -->
                    <div class="migrator-stat-group">
                        <div class="migrator-stat-group-title">Структура</div>
                        <div class="migrator-stats">
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="departments-count">—</div>
                                <div class="migrator-stat-label">Подразделений</div>
                            </div>
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="workgroups-count">—</div>
                                <div class="migrator-stat-label">Рабочих групп</div>
                            </div>
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="pipelines-count">—</div>
                                <div class="migrator-stat-label">Воронок сделок</div>
                            </div>
                        </div>
                    </div>

                    <!-- Users -->
                    <div class="migrator-stat-group">
                        <div class="migrator-stat-group-title">Пользователи</div>
                        <div class="migrator-stats">
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="users-cloud-count">—</div>
                                <div class="migrator-stat-label">Активных в облаке</div>
                            </div>
                            <div class="migrator-stat-card migrator-stat-card--success">
                                <div class="migrator-stat-value" id="users-matched-count">—</div>
                                <div class="migrator-stat-label">Найдено в коробке</div>
                            </div>
                            <div class="migrator-stat-card migrator-stat-card--warning">
                                <div class="migrator-stat-value" id="users-new-count">—</div>
                                <div class="migrator-stat-label">Новых к миграции</div>
                            </div>
                        </div>
                    </div>

                    <!-- CRM -->
                    <div class="migrator-stat-group">
                        <div class="migrator-stat-group-title">CRM</div>
                        <div class="migrator-stats">
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="crm-companies-count">—</div>
                                <div class="migrator-stat-label">Компаний</div>
                            </div>
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="crm-contacts-count">—</div>
                                <div class="migrator-stat-label">Контактов</div>
                            </div>
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="crm-deals-count">—</div>
                                <div class="migrator-stat-label">Сделок</div>
                            </div>
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="crm-leads-count">—</div>
                                <div class="migrator-stat-label">Лидов</div>
                            </div>
                        </div>
                    </div>

                    <!-- Tasks -->
                    <div class="migrator-stat-group">
                        <div class="migrator-stat-group-title">Задачи</div>
                        <div class="migrator-stats">
                            <div class="migrator-stat-card">
                                <div class="migrator-stat-value" id="tasks-count">—</div>
                                <div class="migrator-stat-label">Задач</div>
                            </div>
                        </div>
                    </div>

                    <!-- Actions -->
                    <div style="margin: 24px 0 8px;">
                        <button type="button" id="btn-show-structure" class="adm-btn">
                            <?= Loc::getMessage('BITRIX_MIGRATOR_BTN_SHOW_STRUCTURE') ?>
                        </button>
                    </div>

                    <!-- Department Tree (inline) -->
                    <div id="department-tree-container" style="display:none; margin-top:20px;">
                        <h4>Структура подразделений:</h4>
                        <div id="department-tree"></div>
                    </div>

                    <!-- New Users List -->
                    <div id="new-users-container" style="display:none; margin-top:20px;">
                        <h4>Новые пользователи (будут созданы при миграции):</h4>
                        <div id="new-users-list"></div>
                    </div>

                    <!-- Pipelines List -->
                    <div id="pipelines-container" style="display:none; margin-top:20px;">
                        <h4>Воронки сделок:</h4>
                        <div id="pipelines-list"></div>
                    </div>

                    <!-- CRM Custom Fields -->
                    <div id="crm-fields-container" style="display:none; margin-top:20px;">
                        <h4>Пользовательские поля CRM:</h4>
                        <div id="crm-fields-list"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Tab: Migration Plan -->
    <div id="tab-plan" class="migrator-tab-content">
        <div class="adm-detail-content">
            <h2><?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_TITLE') ?></h2>
            <p><?= Loc::getMessage('BITRIX_MIGRATOR_PLAN_INFO') ?></p>
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

<!-- Structure Slider -->
<div id="structure-slider" class="migrator-slider">
    <div class="migrator-slider-overlay"></div>
    <div class="migrator-slider-content">
        <div class="migrator-slider-header">
            <h3><?= Loc::getMessage('BITRIX_MIGRATOR_SLIDER_STRUCTURE_TITLE') ?></h3>
            <button type="button" class="migrator-slider-close">&times;</button>
        </div>
        <div class="migrator-slider-body">
            <div id="slider-department-tree"></div>
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
