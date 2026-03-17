(function() {
    'use strict';

    let departmentsData = [];

    document.addEventListener('DOMContentLoaded', function() {
        initTabs();
        initConnectionHandlers();
        initDryRunHandlers();
        loadDryRunResults();
    });

    // -------------------------------------------------------------------------
    // Tabs
    // -------------------------------------------------------------------------
    function initTabs() {
        const tabButtons  = document.querySelectorAll('.migrator-tab-btn');
        const tabContents = document.querySelectorAll('.migrator-tab-content');

        tabButtons.forEach(function(button) {
            button.addEventListener('click', function() {
                const targetTab = this.getAttribute('data-tab');
                tabButtons.forEach(function(btn) { btn.classList.remove('active'); });
                tabContents.forEach(function(content) { content.classList.remove('active'); });
                this.classList.add('active');
                document.getElementById('tab-' + targetTab).classList.add('active');
            });
        });
    }

    // -------------------------------------------------------------------------
    // Connection
    // -------------------------------------------------------------------------
    function initConnectionHandlers() {
        document.getElementById('btn-save-connection')?.addEventListener('click', saveConnection);
        document.getElementById('btn-check-connection-cloud')?.addEventListener('click', function() { checkConnection('cloud'); });
        document.getElementById('btn-check-connection-box')?.addEventListener('click', function() { checkConnection('box'); });
        document.getElementById('btn-run-dryrun')?.addEventListener('click', runDryRun);
    }

    function saveConnection() {
        const cloudUrl = document.getElementById('cloud_webhook_url').value.trim();
        const boxUrl   = document.getElementById('box_webhook_url').value.trim();

        if (!cloudUrl && !boxUrl) {
            alert('Укажите хотя бы один вебхук (cloud или box)');
            return;
        }

        const formData = new FormData();
        formData.append('sessid',           window.BITRIX_MIGRATOR.sessid);
        formData.append('cloud_webhook_url', cloudUrl);
        formData.append('box_webhook_url',   boxUrl);

        fetch('/local/ajax/bitrix_migrator/save_connection.php', { method: 'POST', body: formData })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    alert('Настройки сохранены');
                    location.reload();
                } else {
                    alert('Ошибка: ' + (data.error || 'Unknown error'));
                }
            })
            .catch(function() { alert('Ошибка сохранения'); });
    }

    function checkConnection(type) {
        const statusText  = document.getElementById('connection-status-text-' + type);
        const statusBlock = document.getElementById('connection-status-block-' + type).querySelector('.migrator-status');
        const webhookUrl  = document.getElementById(type + '_webhook_url').value.trim();

        if (!webhookUrl) {
            statusBlock.className  = 'migrator-status migrator-status-error';
            statusText.textContent = 'Укажите URL вебхука';
            return;
        }

        statusBlock.className  = 'migrator-status migrator-status-checking';
        statusText.textContent = 'Проверка подключения...';

        const formData = new FormData();
        formData.append('sessid',      window.BITRIX_MIGRATOR.sessid);
        formData.append('type',        type);
        formData.append('webhook_url', webhookUrl);

        fetch('/local/ajax/bitrix_migrator/check_connection.php', { method: 'POST', body: formData })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    statusBlock.className  = 'migrator-status migrator-status-success';
                    statusText.textContent = 'Подключение успешно установлено';
                } else {
                    statusBlock.className  = 'migrator-status migrator-status-error';
                    statusText.textContent = 'Ошибка: ' + (data.error || 'Unknown error');
                }
            })
            .catch(function() {
                statusBlock.className  = 'migrator-status migrator-status-error';
                statusText.textContent = 'Ошибка проверки подключения';
            });
    }

    // -------------------------------------------------------------------------
    // Dry Run
    // -------------------------------------------------------------------------
    function initDryRunHandlers() {
        document.getElementById('btn-show-structure')?.addEventListener('click', showStructureSlider);

        const closeBtn = document.querySelector('.migrator-slider-close');
        const overlay  = document.querySelector('.migrator-slider-overlay');
        if (closeBtn) closeBtn.addEventListener('click', closeStructureSlider);
        if (overlay)  overlay.addEventListener('click',  closeStructureSlider);
    }

    function runDryRun() {
        const btn = document.getElementById('btn-run-dryrun');
        btn.disabled    = true;
        btn.textContent = 'Анализ выполняется...';

        const formData = new FormData();
        formData.append('sessid', window.BITRIX_MIGRATOR.sessid);

        fetch('/local/ajax/bitrix_migrator/start_dryrun.php', { method: 'POST', body: formData })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    loadDryRunResults();
                    document.querySelector('[data-tab="dryrun"]').click();
                } else {
                    alert('Ошибка: ' + (data.error || 'Unknown error'));
                }
                btn.disabled    = false;
                btn.textContent = 'Запустить Dry Run';
            })
            .catch(function() {
                alert('Ошибка запуска анализа');
                btn.disabled    = false;
                btn.textContent = 'Запустить Dry Run';
            });
    }

    function loadDryRunResults() {
        fetch('/local/ajax/bitrix_migrator/get_dryrun_status.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success && data.status === 'completed' && data.data) {
                    displayDryRunResults(data.data);
                } else {
                    document.getElementById('dryrun-summary').style.display   = 'none';
                    document.getElementById('dryrun-no-results').style.display = 'block';
                }
            })
            .catch(function(error) { console.error('Error loading dry run results:', error); });
    }

    function displayDryRunResults(data) {
        document.getElementById('dryrun-summary').style.display   = 'block';
        document.getElementById('dryrun-no-results').style.display = 'none';

        const depts = data.departments || {};
        const users = data.users       || {};
        const crm   = data.crm         || {};

        // Structure
        document.getElementById('departments-count').textContent = depts.count !== undefined ? depts.count : '—';
        document.getElementById('workgroups-count').textContent  = (data.workgroups || {}).count !== undefined ? data.workgroups.count : '—';
        document.getElementById('pipelines-count').textContent   = (data.pipelines  || {}).count !== undefined ? data.pipelines.count  : '—';

        // Users
        document.getElementById('users-cloud-count').textContent   = users.cloud_active_count !== undefined ? users.cloud_active_count : '—';
        document.getElementById('users-matched-count').textContent  = users.matched_count      !== undefined ? users.matched_count      : '—';
        document.getElementById('users-new-count').textContent      = users.new_count          !== undefined ? users.new_count          : '—';

        // CRM
        document.getElementById('crm-companies-count').textContent = crm.companies !== undefined ? crm.companies : '—';
        document.getElementById('crm-contacts-count').textContent  = crm.contacts  !== undefined ? crm.contacts  : '—';
        document.getElementById('crm-deals-count').textContent     = crm.deals     !== undefined ? crm.deals     : '—';
        document.getElementById('crm-leads-count').textContent     = crm.leads     !== undefined ? crm.leads     : '—';

        // Tasks & Smart Processes
        document.getElementById('tasks-count').textContent          = (data.tasks          || {}).count !== undefined ? data.tasks.count          : '—';
        document.getElementById('smart-processes-count').textContent = (data.smart_processes || {}).count !== undefined ? data.smart_processes.count : '—';

        // Department tree
        if (depts.list && depts.list.length > 0) {
            departmentsData = depts.list;
            const treeEl = document.getElementById('department-tree');
            treeEl.innerHTML = '';
            buildDepartmentTree(depts.list, treeEl);
            document.getElementById('department-tree-container').style.display = 'block';
        }

        // New users
        if (users.new_list && users.new_list.length > 0) {
            buildNewUsersList(users.new_list);
            document.getElementById('new-users-container').style.display = 'block';
        }

        // Pipelines
        if (data.pipelines && data.pipelines.list && data.pipelines.list.length > 0) {
            buildPipelinesList(data.pipelines.list);
            document.getElementById('pipelines-container').style.display = 'block';
        }

        // Custom fields
        if (data.crm_custom_fields && !data.crm_custom_fields.error) {
            var hasFields = Object.keys(data.crm_custom_fields).some(function(e) {
                return Object.keys(data.crm_custom_fields[e] || {}).length > 0;
            });
            if (hasFields) {
                buildCrmFieldsList(data.crm_custom_fields);
                document.getElementById('crm-fields-container').style.display = 'block';
            }
        }

        // Smart processes
        if (data.smart_processes && data.smart_processes.list && data.smart_processes.list.length > 0) {
            buildSmartProcessesList(data.smart_processes.list);
            document.getElementById('smart-processes-container').style.display = 'block';
        }
    }

    // -------------------------------------------------------------------------
    // Department Tree
    // -------------------------------------------------------------------------
    function buildDepartmentTree(departments, container) {
        var depMap   = {};
        var rootDeps = [];

        departments.forEach(function(dep) {
            depMap[dep.ID] = Object.assign({}, dep, { children: [] });
        });

        departments.forEach(function(dep) {
            if (dep.PARENT && depMap[dep.PARENT]) {
                depMap[dep.PARENT].children.push(depMap[dep.ID]);
            } else {
                rootDeps.push(depMap[dep.ID]);
            }
        });

        var ul = document.createElement('ul');
        ul.className = 'migrator-tree';
        rootDeps.forEach(function(dep) { ul.appendChild(createTreeNode(dep)); });
        container.appendChild(ul);
    }

    function createTreeNode(department) {
        var li = document.createElement('li');
        li.className = 'migrator-tree-item';

        var nodeContent = document.createElement('div');
        nodeContent.className = 'migrator-tree-node';

        if (department.children && department.children.length > 0) {
            var toggle = document.createElement('span');
            toggle.className  = 'migrator-tree-toggle';
            toggle.textContent = '▶';
            toggle.addEventListener('click', function() {
                li.classList.toggle('expanded');
                toggle.textContent = li.classList.contains('expanded') ? '▼' : '▶';
            });
            nodeContent.appendChild(toggle);
        } else {
            var spacer = document.createElement('span');
            spacer.className = 'migrator-tree-spacer';
            nodeContent.appendChild(spacer);
        }

        var label = document.createElement('span');
        label.className  = 'migrator-tree-label';
        label.textContent = department.NAME + ' (ID: ' + department.ID + ')';
        nodeContent.appendChild(label);

        li.appendChild(nodeContent);

        if (department.children && department.children.length > 0) {
            var childUl = document.createElement('ul');
            childUl.className = 'migrator-tree-children';
            department.children.forEach(function(child) { childUl.appendChild(createTreeNode(child)); });
            li.appendChild(childUl);
        }

        return li;
    }

    // -------------------------------------------------------------------------
    // New Users List
    // -------------------------------------------------------------------------
    function buildNewUsersList(users) {
        var container = document.getElementById('new-users-list');
        container.innerHTML = '';

        var table = document.createElement('table');
        table.className = 'migrator-table';
        table.innerHTML = '<thead><tr><th>ФИО</th><th>Email</th><th>Должность</th><th>Отдел (ID)</th></tr></thead>';

        var tbody = document.createElement('tbody');
        users.forEach(function(u) {
            var row  = document.createElement('tr');
            var name = [u.NAME, u.SECOND_NAME, u.LAST_NAME].filter(Boolean).join(' ');
            row.innerHTML =
                '<td>' + escHtml(name)                         + '</td>' +
                '<td>' + escHtml(u.EMAIL || '')                + '</td>' +
                '<td>' + escHtml(u.WORK_POSITION || '')        + '</td>' +
                '<td>' + escHtml(String(u.DEPARTMENT || ''))   + '</td>';
            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        container.appendChild(table);
    }

    // -------------------------------------------------------------------------
    // Pipelines List
    // -------------------------------------------------------------------------
    function buildPipelinesList(pipelines) {
        var container = document.getElementById('pipelines-list');
        container.innerHTML = '';

        pipelines.forEach(function(pipeline) {
            var block = document.createElement('div');
            block.className = 'migrator-pipeline-block';

            var title = document.createElement('div');
            title.className  = 'migrator-pipeline-title';
            title.textContent = pipeline.name + ' (' + (pipeline.stages_count || 0) + ' стадий)';
            block.appendChild(title);

            if (pipeline.stages && pipeline.stages.length > 0) {
                var stagesEl = document.createElement('div');
                stagesEl.className = 'migrator-pipeline-stages';
                pipeline.stages.forEach(function(stage) {
                    var chip = document.createElement('span');
                    chip.className  = 'migrator-stage-chip';
                    chip.textContent = stage.NAME || stage.name || stage.ID || '';
                    if (stage.COLOR || stage.color) {
                        chip.style.borderColor = '#' + (stage.COLOR || stage.color);
                    }
                    stagesEl.appendChild(chip);
                });
                block.appendChild(stagesEl);
            }

            container.appendChild(block);
        });
    }

    // -------------------------------------------------------------------------
    // CRM Custom Fields
    // -------------------------------------------------------------------------
    function buildCrmFieldsList(customFields) {
        var container = document.getElementById('crm-fields-list');
        container.innerHTML = '';

        var labels = { deal: 'Сделки', contact: 'Контакты', company: 'Компании', lead: 'Лиды' };

        Object.keys(customFields).forEach(function(entity) {
            var fields = customFields[entity];
            if (!fields || Object.keys(fields).length === 0) return;

            var section = document.createElement('div');
            section.style.marginBottom = '12px';

            var heading = document.createElement('strong');
            heading.textContent = (labels[entity] || entity) + ': ';
            section.appendChild(heading);

            Object.keys(fields).forEach(function(fieldCode) {
                var chip = document.createElement('span');
                chip.className  = 'migrator-stage-chip';
                chip.title      = fields[fieldCode].title || '';
                chip.textContent = fieldCode;
                section.appendChild(chip);
            });

            container.appendChild(section);
        });
    }

    // -------------------------------------------------------------------------
    // Structure Slider
    // -------------------------------------------------------------------------
    function showStructureSlider() {
        var slider    = document.getElementById('structure-slider');
        var sliderTree = document.getElementById('slider-department-tree');
        sliderTree.innerHTML = '';
        buildDepartmentTree(departmentsData, sliderTree);
        slider.classList.add('active');
    }

    function closeStructureSlider() {
        document.getElementById('structure-slider').classList.remove('active');
    }

    // -------------------------------------------------------------------------
    // Smart Processes List
    // -------------------------------------------------------------------------
    function buildSmartProcessesList(processes) {
        var container = document.getElementById('smart-processes-list');
        container.innerHTML = '';

        var table = document.createElement('table');
        table.className = 'migrator-table';
        table.innerHTML = '<thead><tr><th>Название</th><th>entityTypeId</th><th>Записей</th></tr></thead>';

        var tbody = document.createElement('tbody');
        processes.forEach(function(p) {
            var row = document.createElement('tr');
            row.innerHTML =
                '<td>' + escHtml(p.title || '')         + '</td>' +
                '<td>' + escHtml(String(p.entityTypeId)) + '</td>' +
                '<td>' + escHtml(String(p.count || 0))  + '</td>';
            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        container.appendChild(table);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    function escHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
})();
