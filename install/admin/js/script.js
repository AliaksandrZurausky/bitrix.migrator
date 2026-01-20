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
            });
        });
    }

    /**
     * Initialize connection form
     */
    function initConnectionForm() {
        const btnSave = document.getElementById('btn-save-connection');
        const btnCheckCloud = document.getElementById('btn-check-connection-cloud');
        const btnCheckBox = document.getElementById('btn-check-connection-box');
        const btnDryRun = document.getElementById('btn-run-dryrun');

        if (btnSave) {
            btnSave.addEventListener('click', saveConnection);
        }

        if (btnCheckCloud) {
            btnCheckCloud.addEventListener('click', function() {
                checkConnection('cloud');
            });
        }

        if (btnCheckBox) {
            btnCheckBox.addEventListener('click', function() {
                checkConnection('box');
            });
        }

        if (btnDryRun) {
            btnDryRun.addEventListener('click', runDryRun);
        }

        // Enable/disable Dry Run button based on cloud webhook
        const cloudWebhookUrl = document.getElementById('cloud_webhook_url');

        if (cloudWebhookUrl && btnDryRun) {
            cloudWebhookUrl.addEventListener('input', function() {
                btnDryRun.disabled = !cloudWebhookUrl.value.trim();
            });
        }
    }

    /**
     * Save connection settings
     */
    function saveConnection() {
        const cloudWebhookUrl = document.getElementById('cloud_webhook_url').value.trim();
        const boxWebhookUrl = document.getElementById('box_webhook_url').value.trim();

        if (!cloudWebhookUrl && !boxWebhookUrl) {
            showMessage('error', 'Заполните хотя бы один webhook URL');
            return;
        }

        setButtonLoading('btn-save-connection', true);

        BX.ajax({
            url: '/local/ajax/bitrix_migrator/save_connection.php',
            data: {
                cloudWebhookUrl: cloudWebhookUrl,
                boxWebhookUrl: boxWebhookUrl,
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
     * Check connection to cloud or box
     */
    function checkConnection(type) {
        const inputId = type === 'cloud' ? 'cloud_webhook_url' : 'box_webhook_url';
        const btnId = type === 'cloud' ? 'btn-check-connection-cloud' : 'btn-check-connection-box';
        const webhookUrl = document.getElementById(inputId).value.trim();

        if (!webhookUrl) {
            showMessage('error', 'Заполните URL webhook');
            return;
        }

        setButtonLoading(btnId, true);
        setConnectionStatus(type, 'loading', 'Проверка подключения...');

        BX.ajax({
            url: '/local/ajax/bitrix_migrator/check_connection.php',
            data: {
                type: type,
                webhookUrl: webhookUrl,
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                setButtonLoading(btnId, false);
                if (response.success) {
                    setConnectionStatus(type, 'success', 'Подключение успешно');
                    showMessage('success', 'Подключение ' + (type === 'cloud' ? 'к облаку' : 'к box') + ' установлено');
                    if (type === 'cloud') {
                        document.getElementById('btn-run-dryrun').disabled = false;
                    }
                } else {
                    setConnectionStatus(type, 'error', 'Ошибка подключения');
                    showMessage('error', response.error || 'Ошибка подключения');
                }
            },
            onfailure: function(error) {
                setButtonLoading(btnId, false);
                setConnectionStatus(type, 'error', 'Ошибка подключения');
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
                    showMessage('success', 'Анализ выполнен');
                    // Get results
                    setTimeout(function() {
                        getDryRunResults();
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
     * Get dry run results
     */
    function getDryRunResults() {
        BX.ajax({
            url: '/local/ajax/bitrix_migrator/get_dryrun_status.php',
            data: {
                sessid: window.BITRIX_MIGRATOR.sessid
            },
            method: 'POST',
            dataType: 'json',
            onsuccess: function(response) {
                if (response.success && response.result) {
                    const resultsDiv = document.getElementById('dryrun-results');
                    const resultsPre = document.getElementById('dryrun-results-pre');
                    
                    if (resultsDiv && resultsPre) {
                        resultsDiv.style.display = 'block';
                        
                        // Format departments list
                        let output = 'Всего подразделений: ' + response.result.count + '\n\n';
                        
                        if (response.result.departments && response.result.departments.length > 0) {
                            output += 'Список подразделений:\n';
                            response.result.departments.forEach(function(dept) {
                                output += '  - ID: ' + dept.ID + ', Название: ' + dept.NAME + ', Родитель: ' + (dept.PARENT || 'нет') + '\n';
                            });
                        }
                        
                        resultsPre.textContent = output;
                    }
                }
            }
        });
    }

    /**
     * Set connection status
     */
    function setConnectionStatus(type, status, text) {
        const statusBlock = document.querySelector('#connection-status-block-' + type + ' .migrator-status');
        const statusText = document.getElementById('connection-status-text-' + type);

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
