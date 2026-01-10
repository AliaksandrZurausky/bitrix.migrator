/**
 * Bitrix Migrator - Main JavaScript
 */

(function() {
    'use strict';

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
     * Save connection settings
     */
    function saveConnection() {
        const webhookUrl = document.getElementById('webhook_url').value.trim();

        if (!webhookUrl) {
            showMessage('error', 'Заполните URL webhook');
            return;
        }

        setButtonLoading('btn-save-connection', true);

        BX.ajax.runAction('bitrix_migrator:migrator.saveConnection', {
            data: {
                webhookUrl: webhookUrl,
                sessid: window.BITRIX_MIGRATOR.sessid
            }
        }).then(function(response) {
            setButtonLoading('btn-save-connection', false);
            if (response.data.success) {
                showMessage('success', 'Настройки сохранены');
            } else {
                showMessage('error', response.data.error || 'Ошибка сохранения');
            }
        }).catch(function(error) {
            setButtonLoading('btn-save-connection', false);
            showMessage('error', error.message || 'Ошибка сохранения');
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

        BX.ajax.runAction('bitrix_migrator:migrator.checkConnection', {
            data: {
                webhookUrl: webhookUrl,
                sessid: window.BITRIX_MIGRATOR.sessid
            }
        }).then(function(response) {
            setButtonLoading('btn-check-connection', false);
            if (response.data.success) {
                setConnectionStatus('success', 'Подключение успешно');
                showMessage('success', 'Подключение установлено');
                document.getElementById('btn-run-dryrun').disabled = false;
            } else {
                setConnectionStatus('error', 'Ошибка подключения');
                showMessage('error', response.data.error || 'Ошибка подключения');
            }
        }).catch(function(error) {
            setButtonLoading('btn-check-connection', false);
            setConnectionStatus('error', 'Ошибка подключения');
            showMessage('error', error.message || 'Ошибка подключения');
        });
    }

    /**
     * Run dry run
     */
    function runDryRun() {
        if (!confirm('Запустить сухой прогон?')) {
            return;
        }

        setButtonLoading('btn-run-dryrun', true);
        showMessage('info', 'Сухой прогон запущен...');

        BX.ajax.runAction('bitrix_migrator:migrator.startDryRun', {
            data: {
                sessid: window.BITRIX_MIGRATOR.sessid
            }
        }).then(function(response) {
            setButtonLoading('btn-run-dryrun', false);
            if (response.data.success) {
                showMessage('success', 'Сухой прогон завершён');
                // Switch to Dry Run tab
                setTimeout(function() {
                    document.querySelector('[data-tab="dryrun"]').click();
                }, 1000);
            } else {
                showMessage('error', response.data.error || 'Ошибка');
            }
        }).catch(function(error) {
            setButtonLoading('btn-run-dryrun', false);
            showMessage('error', error.message || 'Ошибка');
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
        // Remove existing messages
        const existingMessages = document.querySelectorAll('.migrator-message');
        existingMessages.forEach(function(msg) {
            msg.remove();
        });

        // Create new message
        const message = document.createElement('div');
        message.className = 'migrator-message migrator-message-' + type;
        message.textContent = text;

        // Insert message
        const container = document.querySelector('.migrator-tab-content.active .adm-detail-content');
        if (container) {
            container.insertBefore(message, container.firstChild);

            // Auto-hide after 5 seconds
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