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

function migratorStatusText($status) {
    switch ($status) {
        case 'success': return Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_STATUS_SUCCESS');
        case 'error': return Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_STATUS_ERROR');
        default: return Loc::getMessage('BITRIX_MIGRATOR_CONNECTION_STATUS_NOT_CHECKED');
    }
}
?>

<div class="migrator-container">
    <!-- Tabs Navigation -->
    <div class="migrator-tabs-nav">
        <button class="migrator-tab-btn active" data-tab="connection">Подключение</button>
        <button class="migrator-tab-btn" data-tab="dryrun">Анализ</button>
        <button class="migrator-tab-btn" data-tab="plan">План миграции</button>
        <button class="migrator-tab-btn" data-tab="migration">Миграция</button>
        <button class="migrator-tab-btn" data-tab="logs">Логи</button>
    </div>

    <!-- Tab: Connection -->
    <div id="tab-connection" class="migrator-tab-content active">
        <h2>Настройка подключения</h2>
        <p style="color:#666;font-size:14px;margin:0 0 20px;">Укажите вебхуки для доступа к облачному и коробочному порталам Bitrix24.</p>

        <form id="connection-form" class="migrator-form">
            <?= bitrix_sessid_post() ?>

            <div class="migrator-connection-grid">
                <!-- Cloud -->
                <div class="migrator-connection-card">
                    <div class="migrator-connection-card-title">
                        <span class="migrator-connection-card-icon migrator-connection-card-icon--cloud">&#9729;</span>
                        Облачный портал
                    </div>

                    <div class="migrator-form-group">
                        <label for="cloud_webhook_url" class="migrator-label">URL вебхука (Источник):</label>
                        <input type="text"
                               id="cloud_webhook_url"
                               name="cloud_webhook_url"
                               class="migrator-input"
                               value="<?= htmlspecialcharsbx($cloudWebhookUrl) ?>"
                               placeholder="https://portal.bitrix24.ru/rest/1/abc123/">
                        <div class="migrator-hint">Входящий вебхук облачного портала с нужными правами</div>
                    </div>

                    <div class="migrator-status-block" id="connection-status-block-cloud">
                        <div class="migrator-status migrator-status-<?= htmlspecialcharsbx($connectionStatusCloud) ?>">
                            <span class="migrator-status-icon"></span>
                            <span class="migrator-status-text" id="connection-status-text-cloud"><?= migratorStatusText($connectionStatusCloud) ?></span>
                        </div>
                    </div>

                    <button type="button" id="btn-check-connection-cloud" class="adm-btn">Проверить подключение</button>
                </div>

                <!-- Box -->
                <div class="migrator-connection-card">
                    <div class="migrator-connection-card-title">
                        <span class="migrator-connection-card-icon migrator-connection-card-icon--box">&#9881;</span>
                        Коробочный портал
                    </div>

                    <div class="migrator-form-group">
                        <label for="box_webhook_url" class="migrator-label">URL вебхука (Приёмник):</label>
                        <input type="text"
                               id="box_webhook_url"
                               name="box_webhook_url"
                               class="migrator-input"
                               value="<?= htmlspecialcharsbx($boxWebhookUrl) ?>"
                               placeholder="https://box.company.ru/rest/1/xyz789/">
                        <div class="migrator-hint">Входящий вебхук коробочного портала с нужными правами</div>
                    </div>

                    <div class="migrator-status-block" id="connection-status-block-box">
                        <div class="migrator-status migrator-status-<?= htmlspecialcharsbx($connectionStatusBox) ?>">
                            <span class="migrator-status-icon"></span>
                            <span class="migrator-status-text" id="connection-status-text-box"><?= migratorStatusText($connectionStatusBox) ?></span>
                        </div>
                    </div>

                    <button type="button" id="btn-check-connection-box" class="adm-btn">Проверить подключение</button>
                </div>
            </div>

            <div class="migrator-form-actions">
                <button type="button" id="btn-save-connection" class="adm-btn-save">Сохранить настройки</button>
                <button type="button" id="btn-run-dryrun" class="adm-btn" <?= empty($cloudWebhookUrl) ? 'disabled' : '' ?>>Запустить анализ (Dry Run)</button>
            </div>
        </form>
    </div>

    <!-- Tab: Dry Run -->
    <div id="tab-dryrun" class="migrator-tab-content">
        <h2>Анализ порталов</h2>
        <p style="color:#666;font-size:14px;margin:0 0 20px;">Результаты сравнительного анализа облачного и коробочного порталов.</p>

        <div id="dryrun-results">
            <div id="dryrun-no-results" style="display:block;">
                <div style="text-align:center;padding:40px 20px;color:#999;">
                    <div style="font-size:40px;margin-bottom:12px;">&#128270;</div>
                    <p style="font-size:15px;">Анализ ещё не выполнялся</p>
                    <p style="font-size:13px;">Перейдите на вкладку «Подключение» и нажмите «Запустить анализ»</p>
                </div>
            </div>
            <div id="dryrun-summary" style="display:none;"></div>
        </div>
    </div>

    <!-- Tab: Migration Plan -->
    <div id="tab-plan" class="migrator-tab-content">
        <h2>План миграции</h2>
        <p style="color:#666;font-size:14px;margin:0 0 20px;">Выберите, какие данные нужно перенести из облака в коробку. Раскройте секции для детальной настройки.</p>

        <div id="plan-no-dryrun" style="display:none;">
            <div style="text-align:center;padding:40px 20px;color:#e67e22;">
                <div style="font-size:40px;margin-bottom:12px;">&#9888;</div>
                <p style="font-size:15px;">Сначала выполните анализ порталов (Dry Run)</p>
            </div>
        </div>

        <div id="plan-builder" style="display:none;"></div>

        <div id="plan-settings" style="display:none; margin-top:24px;">
            <div class="migrator-accordion">
                <div class="migrator-accordion-header" id="plan-settings-header" style="cursor:pointer;">
                    <span class="migrator-accordion-arrow">&#9654;</span>
                    <span class="migrator-accordion-title">Настройки миграции</span>
                </div>
                <div class="migrator-accordion-body" id="plan-settings-body">
                    <div class="migrator-form-group" style="margin-bottom:16px;">
                        <label class="migrator-label">Маппинг пользователей:</label>
                        <select id="plan-user-strategy" class="migrator-input" style="width:auto;">
                            <option value="email">По совпадению email</option>
                        </select>
                    </div>
                    <div class="migrator-form-group" style="margin-bottom:16px;">
                        <label class="migrator-label">Рабочие группы — при совпадении по названию:</label>
                        <select id="plan-conflict-resolution" class="migrator-input" style="width:auto;">
                            <option value="skip">Использовать существующую (не создавать)</option>
                            <option value="create">Создать новую</option>
                        </select>
                    </div>
                    <div class="migrator-form-group" style="margin-bottom:0;">
                        <label class="migrator-label">Удаление пользовательских полей перед миграцией:</label>
                        <div style="margin-top:8px;">
                            <label style="display:flex;align-items:center;gap:6px;margin-bottom:6px;font-weight:600;">
                                <input type="checkbox" id="plan-delete-userfields" checked>
                                Удалять существующие поля на box перед созданием
                            </label>
                            <div id="plan-delete-userfields-entities" style="margin-left:24px;">
                                <label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
                                    <input type="checkbox" class="plan-delete-uf-entity" data-entity="deal" checked>
                                    Сделки
                                </label>
                                <label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
                                    <input type="checkbox" class="plan-delete-uf-entity" data-entity="contact" checked>
                                    Контакты
                                </label>
                                <label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
                                    <input type="checkbox" class="plan-delete-uf-entity" data-entity="company" checked>
                                    Компании
                                </label>
                                <label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">
                                    <input type="checkbox" class="plan-delete-uf-entity" data-entity="lead" checked>
                                    Лиды
                                </label>
                            </div>
                            <div id="plan-delete-userfields-smart" style="margin-left:24px;margin-top:8px;"></div>
                            <p style="margin:8px 0 0;font-size:12px;color:#888;">Отмеченные типы сущностей — поля будут удалены и пересозданы из облака. Снимите галочку, чтобы не удалять поля для конкретного типа. Смарт-процессы появятся после загрузки данных.</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div id="plan-summary" style="display:none; margin-top:24px;"></div>

        <div id="plan-actions" style="display:none; margin-top:20px;" class="migrator-form-actions">
            <button type="button" id="btn-save-plan" class="adm-btn-save">Сохранить план</button>
        </div>
    </div>

    <!-- Tab: Migration -->
    <div id="tab-migration" class="migrator-tab-content">
        <h2>Миграция данных</h2>
        <p style="color:#666;font-size:14px;margin:0 0 20px;">Запуск и контроль миграции данных из облака в коробку на основе сохранённого плана.</p>

        <div id="migration-controls" style="margin-top:20px; display:flex; gap:10px; align-items:center;">
            <button type="button" id="btn-start-migration" class="adm-btn-save">
                Запустить миграцию
            </button>
            <button type="button" id="btn-stop-migration" class="adm-btn migrator-btn-stop" style="display:none;">
                Остановить миграцию
            </button>
            <span id="migration-pid-info" style="display:none; font-size:12px; color:#888;"></span>
        </div>

        <div id="migration-phases" style="display:none; margin-top:20px;"></div>

        <div id="migration-progress" style="display:none; margin-top:20px;">
            <div class="migrator-accordion open">
                <div class="migrator-accordion-header">
                    <span class="migrator-accordion-title">Прогресс миграции</span>
                    <span class="migrator-accordion-badges">
                        <span class="migrator-badge" id="migration-status-badge" style="border-color:#6c757d;color:#6c757d;">idle</span>
                    </span>
                </div>
                <div class="migrator-accordion-body" style="display:block;">
                    <div id="migration-phase-label" style="margin-bottom:8px;font-weight:600;font-size:14px;color:#0080C8;"></div>
                    <div id="migration-message" style="margin-bottom:12px;font-size:14px;"></div>
                    <div id="migration-stats-row" class="migrator-stats" style="margin-bottom:12px;"></div>
                    <div class="migrator-progress-bar-wrap" style="margin-bottom:16px;">
                        <div class="migrator-progress-bar" id="migration-progress-bar" style="width:0%"></div>
                    </div>
                    <div style="font-weight:600;margin-bottom:6px;font-size:13px;">Лог:</div>
                    <div id="migration-log" class="migrator-log-box"></div>
                </div>
            </div>
        </div>
    </div>

    <!-- Tab: Logs -->
    <div id="tab-logs" class="migrator-tab-content">
        <h2>Журнал миграции</h2>
        <p style="color:#666;font-size:14px;margin:0 0 20px;">Полная история всех операций миграции.</p>

        <div style="margin-bottom:12px; display:flex; gap:10px; align-items:center;">
            <button type="button" id="btn-refresh-logs" class="adm-btn">Обновить</button>
            <label style="font-size:13px; display:flex; align-items:center; gap:6px;">
                <input type="checkbox" id="logs-auto-refresh"> Автообновление
            </label>
            <span id="logs-file-path" style="font-size:12px; color:#888; margin-left:auto;"></span>
        </div>

        <div id="logs-content">
            <div id="logs-empty" style="text-align:center;padding:40px 20px;color:#999;">
                <div style="font-size:40px;margin-bottom:12px;">&#128203;</div>
                <p style="font-size:15px;">Логи миграции появятся после запуска</p>
            </div>
            <div id="logs-box" class="migrator-log-box" style="display:none; max-height:600px;"></div>
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
