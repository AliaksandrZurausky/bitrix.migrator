/**
 * Bitrix Migrator - Main JavaScript
 */

(function() {
    'use strict';

    let dryrunPolling = null;

    // Wait for DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    function init() {
        initTabs();
        initConnectionForm();
        initDryRunTab();
        initPlanTab();
    }

    /**
     * Initialize tabs functionality
     */
    function initTabs() {
        const tabButtons = document.querySelectorAll('.migrator-tab-btn');
        const tabContents = document.querySelectorAll('.migrator-tab-content');

        tabButtons.forEach(button => {
            button.addEventListener('click', function() {
                const tabId = this.getAttribute('data-tab');

                // Remove active class from all tabs
                tabButtons.forEach(btn => btn.classList.remove('active'));
                tabContents.forEach(content => content.classList.remove('active'));

                // Add active class to clicked tab
                this.classList.add('active');
                document.getElementById('tab-' + tabId).classList.add('active');

                // Special handling for dry run tab
                if (tabId === 'dryrun') {
                    checkDryRunStatus();
                }
            });
        });
    }

    /**
     * Initialize connection form
     */
    function initConnectionForm() {
        const btnSave = document.getElementById('btn-save-connection');
        const btnCheck = document.getElementById('btn-check-connection');
        const btnDryRun = document.getElementById('btn-run-dryrun');

        if (btnSave) {
            btnSave.addEventListener('click', saveConnection);
        }

        if (btnCheck) {
            btnCheck.addEventListener('click', checkConnection);
        }

        if (btnDryRun) {
            btnDryRun.addEventListener('click', runDryRun);
        }

        // Enable/disable Dry Run button based on form input
        const webhookUrl = document.getElementById('webhook_url');

        if (webhookUrl && btnDryRun) {
            webhookUrl.addEventListener('input', function() {
                btnDryRun.disabled = !webhookUrl.value.trim();
            });
        }
    }

    /**
     * Initialize Dry Run tab
     */
    function initDryRunTab() {
        const btnGotoPlan = document.getElementById('btn-goto-plan');
        if (btnGotoPlan) {
            btnGotoPlan.addEventListener('click', function() {
                document.querySelector('[data-tab="plan"]').click();
            });
        }

        // Check status on init if needed
        if (window.BITRIX_MIGRATOR.dryrunStatus === 'running') {
            checkDryRunStatus();
        }
    }

    /**
     * Initialize Plan tab
     */
    function initPlanTab() {
        const btnSavePlan = document.getElementById('btn-save-plan');
        const btnGotoMigration = document.getElementById('btn-goto-migration');

        if (btnSavePlan) {
            btnSavePlan.addEventListener('click', saveMigrationPlan);
        }

        if (btnGotoMigration) {
            btnGotoMigration.addEventListener('click', function() {
                document.querySelector('[data-tab="migration"]').click();
            });
        }
    }

    /**
     * Save connection settings
     */
    function saveConnection() {
        const webhookUrl = document.getElementById('webhook_url').value.trim();

        if (!webhookUrl) {
            showMessage('error', 'Заполните URL webhook');
            return;
        }

        setButtonLoading('btn-save-connection', true);

        BX.ajax({
            url: '/local/ajax/bitrix_migrator/save_connection.php',
            data: {
                webhookUrl: webhookUrl,
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                setButtonLoading('btn-save-connection', false);
                if (response.success) {
                    showMessage('success', 'Настройки сохранены');
                } else {
                    showMessage('error', response.error || 'Ошибка сохранения');
                }
            },
            onfailure: function(error) {
                setButtonLoading('btn-save-connection', false);
                showMessage('error', 'Ошибка сохранения');
            }
        });
    }

    /**
     * Check connection to cloud
     */
    function checkConnection() {
        const webhookUrl = document.getElementById('webhook_url').value.trim();

        if (!webhookUrl) {
            showMessage('error', 'Заполните URL webhook');
            return;
        }

        setButtonLoading('btn-check-connection', true);
        setConnectionStatus('loading', 'Проверка подключения...');

        BX.ajax({
            url: '/local/ajax/bitrix_migrator/check_connection.php',
            data: {
                webhookUrl: webhookUrl,
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                setButtonLoading('btn-check-connection', false);
                if (response.success) {
                    setConnectionStatus('success', 'Подключение успешно');
                    showMessage('success', 'Подключение установлено');
                    document.getElementById('btn-run-dryrun').disabled = false;
                } else {
                    setConnectionStatus('error', 'Ошибка подключения');
                    showMessage('error', response.error || 'Ошибка подключения');
                }
            },
            onfailure: function(error) {
                setButtonLoading('btn-check-connection', false);
                setConnectionStatus('error', 'Ошибка подключения');
                showMessage('error', 'Ошибка подключения');
            }
        });
    }

    /**
     * Run dry run
     */
    function runDryRun() {
        if (!confirm('Запустить анализ данных из облака?')) {
            return;
        }

        setButtonLoading('btn-run-dryrun', true);
        showMessage('info', 'Анализ запущен...');

        BX.ajax({
            url: '/local/ajax/bitrix_migrator/start_dryrun.php',
            data: {
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                setButtonLoading('btn-run-dryrun', false);
                if (response.success) {
                    showMessage('success', 'Анализ запущен, переключаемся на вкладку Dry Run');
                    setTimeout(function() {
                        document.querySelector('[data-tab="dryrun"]').click();
                        startDryRunPolling();
                    }, 1000);
                } else {
                    showMessage('error', response.error || 'Ошибка запуска');
                }
            },
            onfailure: function(error) {
                setButtonLoading('btn-run-dryrun', false);
                showMessage('error', 'Ошибка запуска');
            }
        });
    }

    /**
     * Check dry run status
     */
    function checkDryRunStatus() {
        BX.ajax({
            url: '/local/ajax/bitrix_migrator/get_dryrun_status.php',
            data: {
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                if (!response.success) return;

                const status = response.status;
                const progress = response.progress;

                if (status === 'running') {
                    showDryRunProgress(progress);
                    startDryRunPolling();
                } else if (status === 'completed') {
                    hideDryRunProgress();
                    showDryRunResults(response.results);
                    stopDryRunPolling();
                } else if (status === 'error') {
                    hideDryRunProgress();
                    showMessage('error', response.error || 'Ошибка анализа');
                    stopDryRunPolling();
                }
            }
        });
    }

    /**
     * Start polling for dry run status
     */
    function startDryRunPolling() {
        if (dryrunPolling) return;
        dryrunPolling = setInterval(checkDryRunStatus, 3000);
    }

    /**
     * Stop polling
     */
    function stopDryRunPolling() {
        if (dryrunPolling) {
            clearInterval(dryrunPolling);
            dryrunPolling = null;
        }
    }

    /**
     * Show dry run progress
     */
    function showDryRunProgress(progress) {
        const block = document.getElementById('dryrun-progress-block');
        const fill = document.getElementById('dryrun-progress-fill');
        const text = document.getElementById('dryrun-progress-text');

        if (block) block.style.display = 'block';
        if (fill) fill.style.width = progress + '%';
        if (text) text.textContent = progress + '%';
    }

    /**
     * Hide dry run progress
     */
    function hideDryRunProgress() {
        const block = document.getElementById('dryrun-progress-block');
        if (block) block.style.display = 'none';
    }

    /**
     * Show dry run results
     */
    function showDryRunResults(results) {
        const block = document.getElementById('dryrun-results-block');
        const tbody = document.getElementById('dryrun-results-tbody');

        if (!block || !tbody) return;

        tbody.innerHTML = '';

        results.forEach(function(item) {
            const tr = document.createElement('tr');
            tr.innerHTML = 
                '<td>' + item.name + '</td>' +
                '<td>' + item.count + '</td>' +
                '<td><span class="migrator-status-badge migrator-status-badge-' + item.status + '">' + 
                (item.status === 'ready' ? 'Готово' : item.status === 'empty' ? 'Пусто' : 'Ошибка') +
                '</span></td>';
            tbody.appendChild(tr);
        });

        block.style.display = 'block';

        // Fill plan entities
        fillPlanEntities(results);
    }

    /**
     * Fill plan entities checkboxes
     */
    function fillPlanEntities(results) {
        const container = document.getElementById('plan-entities-list');
        if (!container) return;

        container.innerHTML = '';

        results.forEach(function(item) {
            if (item.count > 0 && item.status === 'ready') {
                const label = document.createElement('label');
                label.className = 'migrator-checkbox-label';
                label.innerHTML = 
                    '<input type="checkbox" name="entities[]" value="' + item.key + '" checked> ' +
                    item.name + ' (' + item.count + ')';
                container.appendChild(label);
            }
        });
    }

    /**
     * Save migration plan
     */
    function saveMigrationPlan() {
        const checkboxes = document.querySelectorAll('input[name="entities[]"]:checked');
        const entities = Array.from(checkboxes).map(cb => cb.value);
        const batchSize = document.getElementById('plan-batch-size').value;

        if (entities.length === 0) {
            showMessage('error', 'Выберите хотя бы одну сущность');
            return;
        }

        setButtonLoading('btn-save-plan', true);

        BX.ajax({
            url: '/local/ajax/bitrix_migrator/save_migration_plan.php',
            data: {
                entities: entities,
                batchSize: batchSize,
                priority: entities,
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                setButtonLoading('btn-save-plan', false);
                if (response.success) {
                    showMessage('success', 'План миграции сохранён');
                } else {
                    showMessage('error', response.error || 'Ошибка сохранения');
                }
            },
            onfailure: function(error) {
                setButtonLoading('btn-save-plan', false);
                showMessage('error', 'Ошибка сохранения');
            }
        });
    }

    /**
     * Set connection status
     */
    function setConnectionStatus(status, text) {
        const statusBlock = document.querySelector('.migrator-status');
        const statusText = document.getElementById('connection-status-text');

        if (statusBlock && statusText) {
            statusBlock.className = 'migrator-status migrator-status-' + status;
            statusText.textContent = text;
        }
    }

    /**
     * Show message
     */
    function showMessage(type, text) {
        const existingMessages = document.querySelectorAll('.migrator-message');
        existingMessages.forEach(function(msg) {
            msg.remove();
        });

        const message = document.createElement('div');
        message.className = 'migrator-message migrator-message-' + type;
        message.textContent = text;

        const container = document.querySelector('.migrator-tab-content.active .adm-detail-content');
        if (container) {
            container.insertBefore(message, container.firstChild);

            setTimeout(function() {
                message.remove();
            }, 5000);
        }
    }

    /**
     * Set button loading state
     */
    function setButtonLoading(buttonId, isLoading) {
        const button = document.getElementById(buttonId);
        if (!button) return;

        if (isLoading) {
            button.disabled = true;
            button.dataset.originalText = button.textContent;
            button.innerHTML = '<span class="migrator-loader"></span>';
        } else {
            button.disabled = false;
            button.textContent = button.dataset.originalText || button.textContent;
        }
    }

})();
