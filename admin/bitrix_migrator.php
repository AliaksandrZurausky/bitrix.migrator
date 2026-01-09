<?php

define('ADMIN_MODULE_NAME', 'bitrix_migrator');

require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_admin.php');

use Bitrix\Main\Loader;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Page\Asset;

Loader::includeModule('bitrix_migrator');

$MODULE_ID = 'bitrix_migrator';
$request = \Bitrix\Main\Application::getInstance()->getContext()->getRequest();
$tab = $request->get('tab') ?: 'settings';

if ($request->isPost() && check_bitrix_sessid()) {
    $action = $request->get('action');
    
    switch ($action) {
        case 'save_webhook':
            $webhook = $request->get('webhook_url');
            if (!empty($webhook)) {
                Option::set($MODULE_ID, 'CLOUD_WEBHOOK_URL', $webhook);
                $message = 'Веб-хук сохранён';
            }
            break;
            
        case 'test_webhook':
            try {
                $cloudClient = new \BitrixMigrator\Integration\Cloud\RestClient();
                $result = $cloudClient->call('user.current', []);
                if (isset($result['ID'])) {
                    $testMessage = 'Соединение успешно! Пользователь: ' . htmlspecialchars($result['NAME']);
                } else {
                    $testMessage = 'Ошибка: неверный формат ответа';
                }
            } catch (\Exception $e) {
                $testMessage = 'Ошибка соединения: ' . htmlspecialchars($e->getMessage());
            }
            break;
            
        case 'start_migration':
            Option::set($MODULE_ID, 'MIGRATION_ENABLED', 'Y');
            $startMessage = 'Миграция запущена';
            break;
            
        case 'pause_migration':
            Option::set($MODULE_ID, 'MIGRATION_ENABLED', 'N');
            $pauseMessage = 'Миграция остановлена';
            break;
            
        case 'build_queue':
            try {
                $entityType = $request->get('entity_type') ?: 'DEAL';
                $queueRepo = new \BitrixMigrator\Repository\Hl\QueueRepository();
                $cloudClient = new \BitrixMigrator\Integration\Cloud\RestClient();
                $queueBuilder = new \BitrixMigrator\Service\QueueBuilder($queueRepo, $cloudClient);
                
                $method = 'buildFor' . ucfirst(strtolower($entityType)) . 's';
                if (method_exists($queueBuilder, $method)) {
                    $count = $queueBuilder->$method();
                    $queueMessage = "В очередь добавлено $count сущностей типа $entityType";
                } else {
                    $queueMessage = "Неизвестный тип сущности: $entityType";
                }
            } catch (\Exception $e) {
                $queueMessage = 'Ошибка: ' . htmlspecialchars($e->getMessage());
            }
            break;
            
        case 'save_batch':
            $batchSize = (int)$request->get('batch_size');
            if ($batchSize > 0 && $batchSize <= 500) {
                Option::set($MODULE_ID, 'BATCH_SIZE', $batchSize);
                $message = 'Настройки сохранены';
            }
            break;
    }
}

$webhookUrl = Option::get($MODULE_ID, 'CLOUD_WEBHOOK_URL', '');
$migrationEnabled = Option::get($MODULE_ID, 'MIGRATION_ENABLED', 'N') === 'Y';
$batchSize = (int)Option::get($MODULE_ID, 'BATCH_SIZE', 50);

$tabControl = new CAdminTabControl('migrator_tabs', [
    ['DIV' => 'tab-settings', 'TAB' => 'Подключение', 'ICON' => 'settings'],
    ['DIV' => 'tab-queue', 'TAB' => 'Очередь', 'ICON' => 'list'],
    ['DIV' => 'tab-logs', 'TAB' => 'Логи', 'ICON' => 'history'],
]);

$APPLICATION->SetTitle('Bitrix Migrator - Миграция данных');

// Загружаем JS
Asset::getInstance()->addJs('/bitrix/admin/js/bitrix_migrator.js');
?>

<h1>Bitrix Migrator</h1>
<p style="font-size: 12px; color: #999; margin: -10px 0 20px 0;">
    Миграция данных из облакного портала в локальный сервер
</p>

<?php if (!empty($message)): ?>
<div class="adm-info-message" style="margin-bottom: 10px;">
    <div class="adm-info-message-text"><?php echo htmlspecialchars($message); ?></div>
</div>
<?php endif; ?>

<?php if (!empty($testMessage)): ?>
<div class="<?php echo strpos($testMessage, 'Ошибка') === false ? 'adm-info-message' : 'adm-error-message'; ?>" style="margin-bottom: 10px;">
    <div class="adm-info-message-text"><?php echo htmlspecialchars($testMessage); ?></div>
</div>
<?php endif; ?>

<?php if (!empty($startMessage)): ?>
<div class="adm-info-message" style="margin-bottom: 10px;">
    <div class="adm-info-message-text"><?php echo htmlspecialchars($startMessage); ?></div>
</div>
<?php endif; ?>

<?php if (!empty($pauseMessage)): ?>
<div class="adm-info-message" style="margin-bottom: 10px;">
    <div class="adm-info-message-text"><?php echo htmlspecialchars($pauseMessage); ?></div>
</div>
<?php endif; ?>

<?php if (!empty($queueMessage)): ?>
<div class="<?php echo strpos($queueMessage, 'Ошибка') === false ? 'adm-info-message' : 'adm-error-message'; ?>" style="margin-bottom: 10px;">
    <div class="adm-info-message-text"><?php echo htmlspecialchars($queueMessage); ?></div>
</div>
<?php endif; ?>

<?php $tabControl->Begin(); ?>

<?php $tabControl->BeginNextTab(); ?>
<table class="adm-detail-content-table edit-table" width="100%">
    <tbody>
        <tr class="heading">
            <td colspan="2">Webhook облачного портала</td>
        </tr>
        <tr>
            <td width="40%" class="adm-detail-content-cell-l">
                <label for="webhook_url">URL webhook:</label>
            </td>
            <td width="60%" class="adm-detail-content-cell-r">
                <form method="POST" style="display: inline;">
                    <?php echo bitrix_sessid_post(); ?>
                    <input type="hidden" name="action" value="save_webhook">
                    <input type="text" name="webhook_url" id="webhook_url" value="<?php echo htmlspecialchars($webhookUrl); ?>" 
                           style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 12px; font-family: monospace;" 
                           placeholder="https://your-cloud.bitrix24.ru/rest/1/your_key">
                    <div style="margin-top: 10px;">
                        <button type="submit" class="adm-btn-save" name="save" value="Y">
                            <span>Сохранить</span>
                        </button>
                    </div>
                </form>
            </td>
        </tr>
        <tr>
            <td colspan="2" class="adm-detail-content-cell-l" style="padding-top: 20px;">
                <form method="POST" style="display: inline;">
                    <?php echo bitrix_sessid_post(); ?>
                    <input type="hidden" name="action" value="test_webhook">
                    <button type="submit" class="adm-btn" name="test" value="Y">
                        <span>Проверить соединение</span>
                    </button>
                </form>
            </td>
        </tr>
        
        <tr class="heading" style="padding-top: 30px;">
            <td colspan="2">Настройки миграции</td>
        </tr>
        <tr>
            <td width="40%" class="adm-detail-content-cell-l">
                <label for="batch_size">Размер пачки (задач за итерацию):</label>
            </td>
            <td width="60%" class="adm-detail-content-cell-r">
                <form method="POST" style="display: inline;">
                    <?php echo bitrix_sessid_post(); ?>
                    <input type="hidden" name="action" value="save_batch">
                    <input type="number" name="batch_size" id="batch_size" value="<?php echo $batchSize; ?>" 
                           style="width: 100px; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" 
                           min="1" max="500">
                    <button type="submit" class="adm-btn" name="save" value="Y" style="margin-left: 10px;">
                        <span>Сохранить</span>
                    </button>
                </form>
            </td>
        </tr>
        <tr>
            <td width="40%" class="adm-detail-content-cell-l">
                <label>Состояние миграции:</label>
            </td>
            <td width="60%" class="adm-detail-content-cell-r">
                <div style="padding: 10px 0;">
                    <span class="adm-lbl" style="margin-right: 20px;">
                        <?php if ($migrationEnabled): ?>
                            <span style="color: #3fa43f; font-weight: bold;">●</span> Включена
                        <?php else: ?>
                            <span style="color: #999; font-weight: bold;">●</span> Отключена
                        <?php endif; ?>
                    </span>
                </div>
            </td>
        </tr>
        <tr>
            <td colspan="2" class="adm-detail-content-cell-l" style="padding-top: 10px;">
                <form method="POST" style="display: inline-block; margin-right: 10px;">
                    <?php echo bitrix_sessid_post(); ?>
                    <input type="hidden" name="action" value="start_migration">
                    <button type="submit" class="adm-btn-green" name="start" value="Y" 
                            <?php if ($migrationEnabled) echo 'disabled'; ?>>
                        <span>Запустить</span>
                    </button>
                </form>
                <form method="POST" style="display: inline-block;">
                    <?php echo bitrix_sessid_post(); ?>
                    <input type="hidden" name="action" value="pause_migration">
                    <button type="submit" class="adm-btn-red" name="pause" value="Y"
                            <?php if (!$migrationEnabled) echo 'disabled'; ?>>
                        <span>Остановить</span>
                    </button>
                </form>
            </td>
        </tr>
    </tbody>
</table>

<?php $tabControl->BeginNextTab(); ?>
<table class="adm-detail-content-table edit-table" width="100%">
    <tbody>
        <tr class="heading">
            <td colspan="2">Формирование очереди</td>
        </tr>
        <tr>
            <td colspan="2" class="adm-detail-content-cell-l">
                <p style="margin-bottom: 15px; color: #666; font-size: 12px;">
                    Выберите тип сущности, которую нужно мигрировать.
                </p>
                <form method="POST" style="display: inline;">
                    <?php echo bitrix_sessid_post(); ?>
                    <input type="hidden" name="action" value="build_queue">
                    <select name="entity_type" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px; margin-right: 10px;">
                        <option value="DEAL">Сделки</option>
                        <option value="CONTACT">Контакты</option>
                        <option value="COMPANY">Компании</option>
                        <option value="LEAD">Лиды</option>
                    </select>
                    <button type="submit" class="adm-btn" name="build" value="Y">
                        <span>Добавить в очередь</span>
                    </button>
                </form>
            </td>
        </tr>
        
        <tr class="heading" style="padding-top: 30px;">
            <td colspan="2">Информация об очереди</td>
        </tr>
        <tr>
            <td colspan="2" class="adm-detail-content-cell-l" style="padding: 20px;">
                <div style="background: #f5f5f5; padding: 15px; border-radius: 4px; border: 1px solid #ddd;" id="queue-stats">
                    <div style="margin-bottom: 10px;"><strong>Статистика:</strong></div>
                    <table width="100%" style="font-size: 12px;">
                        <tr>
                            <td width="50%">Всего задач:</td>
                            <td><span class="stat-total">-</span></td>
                        </tr>
                        <tr>
                            <td>Выполнено:</td>
                            <td><span class="stat-completed">-</span></td>
                        </tr>
                        <tr>
                            <td>В очереди:</td>
                            <td><span class="stat-pending">-</span></td>
                        </tr>
                        <tr>
                            <td>Ошибок:</td>
                            <td><span class="stat-errors">-</span></td>
                        </tr>
                    </table>
                    <div style="margin-top: 15px; font-size: 11px; color: #999;">
                        <em>Статистика обновляется каждые 5 секунд</em>
                    </div>
                </div>
            </td>
        </tr>
    </tbody>
</table>

<?php $tabControl->BeginNextTab(); ?>
<table class="adm-detail-content-table edit-table" width="100%">
    <tbody>
        <tr class="heading">
            <td colspan="2">Журнал миграции</td>
        </tr>
        <tr>
            <td colspan="2" class="adm-detail-content-cell-l" style="padding: 20px;">
                <div style="background: #f5f5f5; padding: 15px; border-radius: 4px; border: 1px solid #ddd; min-height: 300px;" id="logs-container">
                    <table width="100%" style="font-size: 12px; border-collapse: collapse;" id="logs-table">
                        <thead>
                            <tr style="border-bottom: 1px solid #ddd;">
                                <th width="15%" style="text-align: left; padding: 8px;">Время</th>
                                <th width="10%" style="text-align: left; padding: 8px;">Уровень</th>
                                <th width="25%" style="text-align: left; padding: 8px;">Область</th>
                                <th width="50%" style="text-align: left; padding: 8px;">Сообщение</th>
                            </tr>
                        </thead>
                        <tbody id="logs-tbody">
                            <tr>
                                <td colspan="4" style="text-align: center; padding: 30px; color: #999;">
                                    <em>Загружаю...</em>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </td>
        </tr>
        
        <tr class="heading" style="padding-top: 20px;">
            <td colspan="2">Фильтры</td>
        </tr>
        <tr>
            <td colspan="2" class="adm-detail-content-cell-l" style="padding: 15px;">
                <div>
                    <label style="margin-right: 20px;">
                        <input type="checkbox" class="log-filter" value="ERROR" checked> Ошибки (ERROR)
                    </label>
                    <label style="margin-right: 20px;">
                        <input type="checkbox" class="log-filter" value="WARNING" checked> Предупреждения (WARNING)
                    </label>
                    <label>
                        <input type="checkbox" class="log-filter" value="INFO"> Информация (INFO)
                    </label>
                </div>
            </td>
        </tr>
    </tbody>
</table>

<?php $tabControl->End(); ?>

<style>
    .adm-btn-green {
        background-color: #3fa43f;
        color: white;
        border: none;
        padding: 8px 16px;
        border-radius: 4px;
        cursor: pointer;
        font-size: 12px;
        transition: background-color 0.2s;
    }
    
    .adm-btn-green:hover:not(:disabled) {
        background-color: #2d7a2d;
    }
    
    .adm-btn-red {
        background-color: #ff4444;
        color: white;
        border: none;
        padding: 8px 16px;
        border-radius: 4px;
        cursor: pointer;
        font-size: 12px;
        transition: background-color 0.2s;
    }
    
    .adm-btn-red:hover:not(:disabled) {
        background-color: #cc0000;
    }
    
    .adm-btn-green:disabled,
    .adm-btn-red:disabled {
        opacity: 0.5;
        cursor: not-allowed;
    }
    
    .adm-error-message {
        background-color: #fce4e4;
        border: 1px solid #f0b0b0;
        color: #a93b3b;
        padding: 10px;
        border-radius: 4px;
    }
    
    #logs-table tbody tr:hover {
        background-color: #efefef;
    }
    
    .log-level-error {
        color: #ff4444;
        font-weight: bold;
    }
    
    .log-level-warning {
        color: #ff8f00;
        font-weight: bold;
    }
    
    .log-level-info {
        color: #666;
    }
</style>

<?php require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_admin.php'); ?>
