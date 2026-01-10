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

        // Enable/disable Dry Run button based on form inputs
        const cloudUrl = document.getElementById('cloud_url');
        const cloudWebhook = document.getElementById('cloud_webhook');

        if (cloudUrl && cloudWebhook && btnDryRun) {
            const checkInputs = () => {
                btnDryRun.disabled = !cloudUrl.value.trim() || !cloudWebhook.value.trim();
            };

            cloudUrl.addEventListener('input', checkInputs);
            cloudWebhook.addEventListener('input', checkInputs);
        }
    }

    /**
     * Save connection settings
     */
    function saveConnection() {
        const cloudUrl = document.getElementById('cloud_url').value.trim();
        const cloudWebhook = document.getElementById('cloud_webhook').value.trim();

        if (!cloudUrl || !cloudWebhook) {
            showMessage('error', BX.message('BITRIX_MIGRATOR_ERROR_EMPTY_FIELDS'));
            return;
        }

        setButtonLoading('btn-save-connection', true);

        BX.ajax.runAction('bitrix:migrator.api.connection.save', {
            data: {
                cloudUrl: cloudUrl,
                cloudWebhook: cloudWebhook
            }
        }).then(function(response) {
            if (response.data.success) {
                showMessage('success', BX.message('BITRIX_MIGRATOR_SETTINGS_SAVED'));
            } else {
                showMessage('error', response.data.error || BX.message('BITRIX_MIGRATOR_ERROR_SAVE'));
            }
        }).catch(function(error) {
            showMessage('error', error.message || BX.message('BITRIX_MIGRATOR_ERROR_SAVE'));
        }).finally(function() {
            setButtonLoading('btn-save-connection', false);
        });
    }

    /**
     * Check connection to cloud
     */
    function checkConnection() {
        const cloudUrl = document.getElementById('cloud_url').value.trim();
        const cloudWebhook = document.getElementById('cloud_webhook').value.trim();

        if (!cloudUrl || !cloudWebhook) {
            showMessage('error', BX.message('BITRIX_MIGRATOR_ERROR_EMPTY_FIELDS'));
            return;
        }

        setButtonLoading('btn-check-connection', true);
        setConnectionStatus('loading', BX.message('BITRIX_MIGRATOR_CONNECTION_STATUS_CHECKING'));

        BX.ajax.runAction('bitrix:migrator.api.connection.check', {
            data: {
                cloudUrl: cloudUrl,
                cloudWebhook: cloudWebhook
            }
        }).then(function(response) {
            if (response.data.success) {
                setConnectionStatus('success', BX.message('BITRIX_MIGRATOR_CONNECTION_STATUS_SUCCESS'));
                showMessage('success', BX.message('BITRIX_MIGRATOR_CONNECTION_SUCCESS'));
                document.getElementById('btn-run-dryrun').disabled = false;
            } else {
                setConnectionStatus('error', response.data.error || BX.message('BITRIX_MIGRATOR_CONNECTION_STATUS_ERROR'));
                showMessage('error', response.data.error || BX.message('BITRIX_MIGRATOR_CONNECTION_ERROR'));
            }
        }).catch(function(error) {
            setConnectionStatus('error', BX.message('BITRIX_MIGRATOR_CONNECTION_STATUS_ERROR'));
            showMessage('error', error.message || BX.message('BITRIX_MIGRATOR_CONNECTION_ERROR'));
        }).finally(function() {
            setButtonLoading('btn-check-connection', false);
        });
    }

    /**
     * Run dry run
     */
    function runDryRun() {
        if (!confirm(BX.message('BITRIX_MIGRATOR_DRYRUN_CONFIRM'))) {
            return;
        }

        setButtonLoading('btn-run-dryrun', true);
        showMessage('info', BX.message('BITRIX_MIGRATOR_DRYRUN_STARTED'));

        BX.ajax.runAction('bitrix:migrator.api.dryrun.start', {
            data: {}
        }).then(function(response) {
            if (response.data.success) {
                showMessage('success', BX.message('BITRIX_MIGRATOR_DRYRUN_SUCCESS'));
                // Switch to Dry Run tab
                setTimeout(() => {
                    document.querySelector('[data-tab="dryrun"]').click();
                }, 1000);
            } else {
                showMessage('error', response.data.error || BX.message('BITRIX_MIGRATOR_DRYRUN_ERROR'));
            }
        }).catch(function(error) {
            showMessage('error', error.message || BX.message('BITRIX_MIGRATOR_DRYRUN_ERROR'));
        }).finally(function() {
            setButtonLoading('btn-run-dryrun', false);
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
        existingMessages.forEach(msg => msg.remove());

        // Create new message
        const message = document.createElement('div');
        message.className = 'migrator-message migrator-message-' + type;
        message.textContent = text;

        // Insert message
        const container = document.querySelector('.migrator-tab-content.active .adm-detail-content');
        if (container) {
            container.insertBefore(message, container.firstChild);

            // Auto-hide after 5 seconds
            setTimeout(() => {
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
