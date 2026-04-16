(function() {
    'use strict';

    var dryRunData = null;
    var savedPlan  = null;
    var migrationPollTimer = null;

    var PHASE_LABELS = {
        departments: 'Подразделения',
        users: 'Пользователи',
        crm_fields: 'CRM поля',
        pipelines: 'Воронки сделок',
        companies: 'Компании',
        contacts: 'Контакты',
        deals: 'Сделки',
        leads: 'Лиды',
        invoices: 'Счета',
        timeline: 'Таймлайн и активности',
        workgroups: 'Рабочие группы',
        smart_processes: 'Смарт-процессы',
        tasks: 'Задачи'
    };

    var PHASE_ORDER = ['departments', 'users', 'crm_fields', 'pipelines', 'companies', 'contacts', 'leads', 'deals', 'invoices', 'timeline', 'workgroups', 'smart_processes', 'tasks'];

    var logsAutoRefreshTimer = null;

    document.addEventListener('DOMContentLoaded', function() {
        initTabs();
        initConnectionHandlers();
        loadDryRunResults();
        initPlanHandlers();
        initMigrationHandlers();
        initLogsHandlers();
    });

    // =========================================================================
    // Tabs
    // =========================================================================
    function initTabs() {
        var tabButtons  = document.querySelectorAll('.migrator-tab-btn');
        var tabContents = document.querySelectorAll('.migrator-tab-content');

        tabButtons.forEach(function(button) {
            button.addEventListener('click', function() {
                var targetTab = this.getAttribute('data-tab');
                tabButtons.forEach(function(btn) { btn.classList.remove('active'); });
                tabContents.forEach(function(content) { content.classList.remove('active'); });
                this.classList.add('active');
                document.getElementById('tab-' + targetTab).classList.add('active');

                if (targetTab === 'plan') loadPlan();
                if (targetTab === 'logs') loadLogs();
            });
        });
    }

    // =========================================================================
    // Connection
    // =========================================================================
    function initConnectionHandlers() {
        document.getElementById('btn-save-connection')?.addEventListener('click', saveConnection);
        document.getElementById('btn-check-connection-cloud')?.addEventListener('click', function() { checkConnection('cloud'); });
        document.getElementById('btn-check-connection-box')?.addEventListener('click', function() { checkConnection('box'); });
        document.getElementById('btn-run-dryrun')?.addEventListener('click', runDryRun);
    }

    function saveConnection() {
        var cloudUrl = document.getElementById('cloud_webhook_url').value.trim();
        var boxUrl   = document.getElementById('box_webhook_url').value.trim();
        if (!cloudUrl && !boxUrl) { alert('Укажите хотя бы один вебхук'); return; }

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);
        fd.append('cloud_webhook_url', cloudUrl);
        fd.append('box_webhook_url', boxUrl);

        fetch('/local/ajax/bitrix_migrator/save_connection.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) { data.success ? (alert('Настройки сохранены'), location.reload()) : alert('Ошибка: ' + (data.error || '')); })
            .catch(function() { alert('Ошибка сохранения'); });
    }

    function checkConnection(type) {
        var statusText  = document.getElementById('connection-status-text-' + type);
        var statusBlock = document.getElementById('connection-status-block-' + type).querySelector('.migrator-status');
        var webhookUrl  = document.getElementById(type + '_webhook_url').value.trim();
        if (!webhookUrl) { statusBlock.className = 'migrator-status migrator-status-error'; statusText.textContent = 'Укажите URL вебхука'; return; }

        statusBlock.className = 'migrator-status migrator-status-checking';
        statusText.textContent = 'Проверка подключения...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);
        fd.append('type', type);
        fd.append('webhook_url', webhookUrl);

        fetch('/local/ajax/bitrix_migrator/check_connection.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                statusBlock.className = 'migrator-status migrator-status-' + (data.success ? 'success' : 'error');
                statusText.textContent = data.success ? 'Подключение успешно' : 'Ошибка: ' + (data.error || '');
            })
            .catch(function() { statusBlock.className = 'migrator-status migrator-status-error'; statusText.textContent = 'Ошибка проверки'; });
    }

    // =========================================================================
    // Dry Run
    // =========================================================================
    function runDryRun() {
        var btn = document.getElementById('btn-run-dryrun');
        btn.disabled = true; btn.textContent = 'Анализ выполняется...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);

        fetch('/local/ajax/bitrix_migrator/start_dryrun.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) { loadDryRunResults(); document.querySelector('[data-tab="dryrun"]').click(); }
                else alert('Ошибка: ' + (data.error || ''));
                btn.disabled = false; btn.textContent = 'Запустить анализ (Dry Run)';
            })
            .catch(function() { alert('Ошибка запуска анализа'); btn.disabled = false; btn.textContent = 'Запустить анализ (Dry Run)'; });
    }

    function loadDryRunResults() {
        fetch('/local/ajax/bitrix_migrator/get_dryrun_status.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success && data.status === 'completed' && data.data) {
                    dryRunData = data.data;
                    displayDryRunResults(data.data);
                } else {
                    document.getElementById('dryrun-summary').style.display = 'none';
                    document.getElementById('dryrun-no-results').style.display = 'block';
                }
            })
            .catch(function(e) { console.error('Error loading dry run:', e); });
    }

    // =========================================================================
    // Display Dry Run Results
    // =========================================================================
    function displayDryRunResults(data) {
        var summary = document.getElementById('dryrun-summary');
        summary.style.display = 'block';
        document.getElementById('dryrun-no-results').style.display = 'none';
        summary.innerHTML = '';
        summary.appendChild(el('h3', {}, 'Результаты анализа портала'));

        var depts = data.departments || {};
        var users = data.users || {};
        var crm = data.crm || {};
        var deptNameMap = users.dept_name_map || {};

        // 1. Departments
        summary.appendChild(makeAccordion('Подразделения', val(depts.count), function(body) {
            if (depts.list && depts.list.length > 0) {
                var actions = el('div', { style: 'margin-bottom:12px;' });
                var openBtn = el('button', { type: 'button', className: 'adm-btn' }, 'Открыть структуру в окне');
                openBtn.addEventListener('click', function() {
                    openDepartmentModal(depts.list, users.all_active_list || []);
                });
                actions.appendChild(openBtn);
                body.appendChild(actions);
                buildDepartmentTree(depts.list, body);
            } else {
                body.appendChild(el('p', { style: 'color:#999' }, 'Нет данных'));
            }
        }));

        // 2. Users
        var ub = [badge('В облаке', val(users.cloud_active_count), '#0080C8')];
        if (users.box_active_count !== undefined) ub.push(badge('В коробке', val(users.box_active_count), '#17a2b8'));
        if (users.matched_count !== undefined) ub.push(badge('Совпали', val(users.matched_count), '#28a745'));
        if (users.new_count !== undefined) ub.push(badge('Новых', val(users.new_count), '#e67e22'));

        summary.appendChild(makeAccordion('Пользователи', ub, function(body) {
            var sr = el('div', { className: 'migrator-stats' });
            sr.appendChild(statCard(val(users.cloud_count), 'Всего в облаке'));
            sr.appendChild(statCard(val(users.cloud_active_count), 'Активных в облаке'));
            if (users.box_count !== undefined) { sr.appendChild(statCard(val(users.box_count), 'Всего в коробке')); sr.appendChild(statCard(val(users.box_active_count), 'Активных в коробке')); }
            if (users.matched_count !== undefined) sr.appendChild(statCard(val(users.matched_count), 'Совпали по email', 'success'));
            if (users.new_count !== undefined) sr.appendChild(statCard(val(users.new_count), 'Новых к миграции', 'warning'));
            body.appendChild(sr);
            if (users.new_list && users.new_list.length > 0) {
                body.appendChild(el('h4', { style: 'margin-top:16px' }, 'Новые пользователи (будут созданы):'));
                buildNewUsersList(users.new_list, deptNameMap, body);
            }
        }));

        // 3. CRM
        var cb = [];
        var cl = val(crm.companies_regular !== undefined ? crm.companies_regular : crm.companies);
        if (crm.companies_my > 0) cl += ' (+' + crm.companies_my + ' мои)';
        cb.push(badge('Компании', cl, '#6c757d'));
        cb.push(badge('Контакты', val(crm.contacts), '#6c757d'));
        cb.push(badge('Сделки', val(crm.deals), '#6c757d'));
        cb.push(badge('Лиды', val(crm.leads), '#6c757d'));

        summary.appendChild(makeAccordion('CRM', cb, function(body) {
            var sr = el('div', { className: 'migrator-stats' });
            var cv = crm.companies_regular !== undefined ? crm.companies_regular : crm.companies;
            sr.appendChild(statCard(val(cv), crm.companies_my > 0 ? 'Компаний (+ ' + crm.companies_my + ' мои)' : 'Компаний'));
            sr.appendChild(statCard(val(crm.contacts), 'Контактов'));
            sr.appendChild(statCard(val(crm.deals), 'Сделок'));
            sr.appendChild(statCard(val(crm.leads), 'Лидов'));
            body.appendChild(sr);

            if (data.crm_custom_fields && !data.crm_custom_fields.error) {
                var labels = { deal: 'Сделки', contact: 'Контакты', company: 'Компании', lead: 'Лиды' };
                Object.keys(data.crm_custom_fields).forEach(function(entity) {
                    var fields = data.crm_custom_fields[entity];
                    if (!fields || Object.keys(fields).length === 0) return;
                    body.appendChild(makeSubAccordion('Пользовательские поля: ' + (labels[entity] || entity), Object.keys(fields).length + ' полей', function(sb) {
                        var t = el('table', { className: 'migrator-table' });
                        t.innerHTML = '<thead><tr><th>Код</th><th>Название</th><th>Тип</th></tr></thead>';
                        var tb = document.createElement('tbody');
                        Object.keys(fields).forEach(function(code) { var f = fields[code]; var r = document.createElement('tr'); r.innerHTML = '<td>' + esc(code) + '</td><td>' + esc(f.title || '') + '</td><td>' + esc(f.type || '') + '</td>'; tb.appendChild(r); });
                        t.appendChild(tb); sb.appendChild(t);
                    }));
                });
            }
        }));

        // 4. Pipelines
        var pipelines = (data.pipelines || {}).list || [];
        summary.appendChild(makeAccordion('Воронки сделок', val((data.pipelines || {}).count) + ' воронок', function(body) {
            pipelines.forEach(function(p) {
                body.appendChild(makeSubAccordion(p.name + ' (' + (p.stages_count || 0) + ' стадий)', null, function(sb) {
                    if (p.stages && p.stages.length > 0) {
                        var se = el('div', { className: 'migrator-pipeline-stages' });
                        p.stages.forEach(function(s) {
                            var ch = el('span', { className: 'migrator-stage-chip' }, s.NAME || s.name || s.ID || '');
                            if (s.COLOR || s.color) { ch.style.borderLeftColor = '#' + (s.COLOR || s.color).replace('#', ''); ch.style.borderLeftWidth = '3px'; }
                            se.appendChild(ch);
                        });
                        sb.appendChild(se);
                    } else sb.appendChild(el('p', { style: 'color:#999' }, 'Нет стадий'));
                }));
            });
        }));

        // 5. Tasks
        summary.appendChild(makeAccordion('Задачи', val((data.tasks || {}).count), null));

        // 6. Workgroups
        var wgList = (data.workgroups || {}).list || [];
        summary.appendChild(makeAccordion('Рабочие группы', val((data.workgroups || {}).count), wgList.length > 0 ? function(body) {
            var t = el('table', { className: 'migrator-table' });
            t.innerHTML = '<thead><tr><th>ID</th><th>Название</th><th>Описание</th></tr></thead>';
            var tb = document.createElement('tbody');
            wgList.forEach(function(wg) { var r = document.createElement('tr'); r.innerHTML = '<td>' + esc(wg.ID) + '</td><td>' + esc(wg.NAME) + '</td><td>' + esc((wg.DESCRIPTION || '').substring(0, 100)) + '</td>'; tb.appendChild(r); });
            t.appendChild(tb); body.appendChild(t);
        } : null));

        // 7. Smart processes
        var sps = (data.smart_processes || {}).list || [];
        summary.appendChild(makeAccordion('Смарт-процессы', val((data.smart_processes || {}).count), sps.length > 0 ? function(body) {
            sps.forEach(function(sp) {
                var cfc = sp.custom_fields ? Object.keys(sp.custom_fields).length : 0;
                body.appendChild(makeSubAccordion(sp.title + ' (entityTypeId: ' + sp.entityTypeId + ', записей: ' + (sp.count || 0) + ')', cfc > 0 ? cfc + ' польз. полей' : null, function(sb) {
                    if (cfc > 0) {
                        var t = el('table', { className: 'migrator-table' });
                        t.innerHTML = '<thead><tr><th>Код</th><th>Название</th><th>Тип</th></tr></thead>';
                        var tb = document.createElement('tbody');
                        Object.keys(sp.custom_fields).forEach(function(code) { var f = sp.custom_fields[code]; var r = document.createElement('tr'); r.innerHTML = '<td>' + esc(code) + '</td><td>' + esc(f.title || '') + '</td><td>' + esc(f.type || '') + '</td>'; tb.appendChild(r); });
                        t.appendChild(tb); sb.appendChild(t);
                    } else sb.appendChild(el('p', { style: 'color:#999' }, 'Нет пользовательских полей'));
                }));
            });
        } : null));
    }

    // =========================================================================
    // Migration Plan
    // =========================================================================
    function initPlanHandlers() {
        document.getElementById('btn-save-plan')?.addEventListener('click', savePlan);

        var settingsHeader = document.getElementById('plan-settings-header');
        if (settingsHeader) {
            settingsHeader.addEventListener('click', function() {
                var acc = settingsHeader.parentElement;
                var arrow = settingsHeader.querySelector('.migrator-accordion-arrow');
                acc.classList.toggle('open');
                arrow.textContent = acc.classList.contains('open') ? '\u25BC' : '\u25B6';
            });
        }

        // Delete userfields master toggle — affects both CRM entities and smart processes
        var deleteUfMaster = document.getElementById('plan-delete-userfields');
        if (deleteUfMaster) {
            deleteUfMaster.addEventListener('change', function() {
                var containers = [
                    document.getElementById('plan-delete-userfields-entities'),
                    document.getElementById('plan-delete-userfields-smart')
                ];
                containers.forEach(function(c) {
                    if (c) {
                        c.style.opacity = deleteUfMaster.checked ? '1' : '0.5';
                        c.style.pointerEvents = deleteUfMaster.checked ? 'auto' : 'none';
                    }
                });
            });
        }

        // Cleanup accordion toggle
        var cleanupHeader = document.getElementById('plan-cleanup-header');
        if (cleanupHeader) {
            cleanupHeader.addEventListener('click', function() {
                var acc = cleanupHeader.parentElement;
                var arrow = cleanupHeader.querySelector('.migrator-accordion-arrow');
                acc.classList.toggle('open');
                arrow.textContent = acc.classList.contains('open') ? '\u25BC' : '\u25B6';
            });
        }
    }

    function loadPlan() {
        fetch('/local/ajax/bitrix_migrator/get_plan.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                savedPlan = data.plan || null;
                buildPlanUI();
            })
            .catch(function() { buildPlanUI(); });
    }

    function buildPlanUI() {
        var builder = document.getElementById('plan-builder');
        var noDryRun = document.getElementById('plan-no-dryrun');
        var settings = document.getElementById('plan-settings');
        var summary = document.getElementById('plan-summary');
        var actions = document.getElementById('plan-actions');

        var cleanupSettings = document.getElementById('plan-cleanup-settings');

        if (!dryRunData) {
            noDryRun.style.display = 'block';
            builder.style.display = 'none';
            settings.style.display = 'none';
            if (cleanupSettings) cleanupSettings.style.display = 'none';
            summary.style.display = 'none';
            actions.style.display = 'none';
            return;
        }

        noDryRun.style.display = 'none';
        builder.style.display = 'block';
        settings.style.display = 'block';
        if (cleanupSettings) cleanupSettings.style.display = 'block';
        actions.style.display = 'flex';
        builder.innerHTML = '';

        var plan = savedPlan || {};
        var depts = dryRunData.departments || {};
        var users = dryRunData.users || {};
        var crm = dryRunData.crm || {};
        var pipelines = (dryRunData.pipelines || {}).list || [];
        var wgList = (dryRunData.workgroups || {}).list || [];
        var smartProcs = (dryRunData.smart_processes || {}).list || [];
        var deptNameMap = users.dept_name_map || {};

        // Restore settings
        if (plan.settings) {
            if (plan.settings.user_match_strategy) document.getElementById('plan-user-strategy').value = plan.settings.user_match_strategy;
            if (plan.settings.conflict_resolution) document.getElementById('plan-conflict-resolution').value = plan.settings.conflict_resolution;
            if (plan.settings.send_invite) document.getElementById('plan-send-invite').value = plan.settings.send_invite;
            var delMigratedEl = document.getElementById('plan-delete-migrated-data');
            if (delMigratedEl) delMigratedEl.checked = !!plan.settings.delete_migrated_data;
        }

        // Restore delete userfields setting
        if (plan.delete_userfields) {
            document.getElementById('plan-delete-userfields').checked = plan.delete_userfields.enabled !== false;
            var skipEntities = plan.delete_userfields.skip_entities || [];
            document.querySelectorAll('.plan-delete-uf-entity').forEach(function(cb) {
                var entity = cb.getAttribute('data-entity');
                cb.checked = skipEntities.indexOf(entity) === -1;
            });
        }

        // Restore cleanup settings
        if (plan.cleanup) {
            document.querySelectorAll('.plan-cleanup-phase').forEach(function(cb) {
                var phase = cb.getAttribute('data-phase');
                cb.checked = !!plan.cleanup[phase];
            });
        }

        var DUPLICATE_CRITERIA = {
            companies: [
                { value: 'TITLE', label: 'Название компании' },
                { value: 'RQ_INN', label: 'УНП / ИНН' },
                { value: 'PHONE', label: 'Телефон' },
                { value: 'EMAIL', label: 'Email' },
            ],
            contacts: [
                { value: 'EMAIL', label: 'Email' },
                { value: 'PHONE', label: 'Телефон' },
                { value: 'FULL_NAME', label: 'ФИО (Имя + Фамилия)' },
            ],
            deals: [
                { value: 'TITLE', label: 'Название сделки' },
            ],
            leads: [
                { value: 'TITLE', label: 'Название лида' },
                { value: 'EMAIL', label: 'Email' },
                { value: 'PHONE', label: 'Телефон' },
            ],
        };

        var DUPLICATE_ACTIONS = [
            { value: 'skip', label: 'Пропустить (не создавать)' },
            { value: 'update', label: 'Обновить существующий' },
            { value: 'create', label: 'Создать дубль' },
        ];

        var sections = [];

        // 1. Departments
        sections.push(buildPlanSection('departments', 'Подразделения', depts.count || 0, plan.departments, null));

        // 2. Users
        var newUsers = users.new_list || [];
        sections.push(buildPlanSection('users', 'Пользователи (новые к миграции)', newUsers.length, plan.users, function(body) {
            if (newUsers.length === 0) { body.appendChild(el('p', { style: 'color:#999' }, 'Нет новых пользователей')); return; }
            var t = el('table', { className: 'migrator-table' });
            t.innerHTML = '<thead><tr><th style="width:30px;"><input type="checkbox" data-plan-select-all="users" checked></th><th>ФИО</th><th>Email</th><th>Отдел</th></tr></thead>';
            var tb = document.createElement('tbody');
            newUsers.forEach(function(u) {
                var uid = String(u.ID);
                var checked = isItemEnabled(plan, 'users', uid);
                var r = document.createElement('tr');
                var name = [u.LAST_NAME, u.NAME, u.SECOND_NAME].filter(Boolean).join(' ');
                var deptIds = u.UF_DEPARTMENT || u.DEPARTMENT || [];
                if (!Array.isArray(deptIds)) deptIds = [deptIds];
                var deptNames = deptIds.map(function(id) { return deptNameMap[String(id)] || ('ID:' + id); }).join(', ');
                r.innerHTML = '<td><input type="checkbox" data-plan-item="users" data-item-id="' + esc(uid) + '" ' + (checked ? 'checked' : '') + '></td>' +
                    '<td>' + esc(name) + '</td><td>' + esc(u.EMAIL || '') + '</td><td>' + esc(deptNames) + '</td>';
                tb.appendChild(r);
            });
            t.appendChild(tb); body.appendChild(t);
            syncSelectAll(t);
        }));

        // 3. Companies
        sections.push(buildPlanSection('companies', 'Компании', crm.companies_regular || crm.companies || 0, plan.companies, function(body) {
            buildDuplicateSettings(body, 'companies', plan, DUPLICATE_CRITERIA, DUPLICATE_ACTIONS);
        }));

        // 4. Contacts
        sections.push(buildPlanSection('contacts', 'Контакты', crm.contacts || 0, plan.contacts, function(body) {
            buildDuplicateSettings(body, 'contacts', plan, DUPLICATE_CRITERIA, DUPLICATE_ACTIONS);
        }));

        // 5. Pipelines
        sections.push(buildPlanSection('pipelines', 'Воронки сделок', pipelines.length, plan.pipelines, function(body) {
            if (pipelines.length === 0) { body.appendChild(el('p', { style: 'color:#999' }, 'Нет воронок')); return; }
            pipelines.forEach(function(p) {
                var pid = String(p.id);
                var checked = isItemEnabled(plan, 'pipelines', pid);
                var row = el('div', { className: 'migrator-plan-item' });
                var cb = el('input');
                cb.type = 'checkbox'; cb.checked = checked;
                cb.setAttribute('data-plan-item', 'pipelines');
                cb.setAttribute('data-item-id', pid);
                row.appendChild(cb);
                row.appendChild(el('span', {}, p.name + ' (' + (p.stages_count || 0) + ' стадий)'));
                body.appendChild(row);
            });
        }));

        // 6. Leads
        sections.push(buildPlanSection('leads', 'Лиды', crm.leads || 0, plan.leads, null));

        // 7. Deals
        sections.push(buildPlanSection('deals', 'Сделки', crm.deals || 0, plan.deals, null));

        // 7.5 Invoices (SmartInvoice entityTypeId=31)
        sections.push(buildPlanSection('invoices', 'Счета', crm.invoices || 0, plan.invoices, null));

        // 8. Timeline
        sections.push(buildPlanSection('timeline', 'Таймлайн и активности', null, plan.timeline, null));

        // 9. Tasks
        sections.push(buildPlanSection('tasks', 'Задачи', (dryRunData.tasks || {}).count || 0, plan.tasks, null));

        // 9. Workgroups
        var wgData = dryRunData.workgroups || {};
        var wgMatchedNames = (wgData.matched_list || []).map(function(wg) { return wg.NAME.toLowerCase(); });
        sections.push(buildPlanSection('workgroups', 'Рабочие группы', wgList.length, plan.workgroups, function(body) {
            if (wgList.length === 0) { body.appendChild(el('p', { style: 'color:#999' }, 'Нет рабочих групп')); return; }

            if (wgData.matched_count > 0) {
                var info = el('p', { style: 'margin-bottom:10px;font-size:13px;color:#666' });
                info.innerHTML = 'Совпали по названию с коробкой: <strong>' + wgData.matched_count + '</strong>. Новых: <strong>' + (wgData.new_count || 0) + '</strong>';
                body.appendChild(info);
            }

            wgList.forEach(function(wg) {
                var wid = String(wg.ID);
                var checked = isItemEnabled(plan, 'workgroups', wid);
                var isMatched = wgMatchedNames.indexOf(wg.NAME.toLowerCase()) !== -1;
                var row = el('div', { className: 'migrator-plan-item' });
                var cb = el('input');
                cb.type = 'checkbox'; cb.checked = checked;
                cb.setAttribute('data-plan-item', 'workgroups');
                cb.setAttribute('data-item-id', wid);
                row.appendChild(cb);
                row.appendChild(el('span', {}, wg.NAME));
                if (isMatched) {
                    row.appendChild(badge('', 'есть в коробке', '#28a745'));
                }
                body.appendChild(row);
            });
        }));

        // 10. Smart processes
        sections.push(buildPlanSection('smart_processes', 'Смарт-процессы', smartProcs.length, plan.smart_processes, function(body) {
            if (smartProcs.length === 0) { body.appendChild(el('p', { style: 'color:#999' }, 'Нет смарт-процессов')); return; }
            smartProcs.forEach(function(sp) {
                var spId = String(sp.entityTypeId);
                var checked = isItemEnabled(plan, 'smart_processes', spId);
                var row = el('div', { className: 'migrator-plan-item' });
                var cb = el('input');
                cb.type = 'checkbox'; cb.checked = checked;
                cb.setAttribute('data-plan-item', 'smart_processes');
                cb.setAttribute('data-item-id', spId);
                row.appendChild(cb);
                row.appendChild(el('span', {}, sp.title + ' (записей: ' + (sp.count || 0) + ')'));

                if (sp.custom_fields && Object.keys(sp.custom_fields).length > 0) {
                    var fieldsToggle = el('span', { className: 'migrator-plan-fields-toggle' }, 'поля \u25B6');
                    row.appendChild(fieldsToggle);
                    var fieldsDiv = el('div', { className: 'migrator-plan-fields', style: 'display:none' });
                    Object.keys(sp.custom_fields).forEach(function(code) {
                        var f = sp.custom_fields[code];
                        var fchecked = isFieldEnabled(plan, 'smart_process_fields', spId, code);
                        var fr = el('div', { className: 'migrator-plan-field-item' });
                        var fcb = el('input');
                        fcb.type = 'checkbox'; fcb.checked = fchecked;
                        fcb.setAttribute('data-plan-field', 'smart_process_fields');
                        fcb.setAttribute('data-entity-id', spId);
                        fcb.setAttribute('data-field-code', code);
                        fr.appendChild(fcb);
                        fr.appendChild(el('span', {}, code + ' \u2014 ' + (f.title || '') + ' (' + (f.type || '') + ')'));
                        fieldsDiv.appendChild(fr);
                    });
                    body.appendChild(row);
                    body.appendChild(fieldsDiv);
                    fieldsToggle.addEventListener('click', function() {
                        var vis = fieldsDiv.style.display === 'none';
                        fieldsDiv.style.display = vis ? 'block' : 'none';
                        fieldsToggle.textContent = vis ? 'поля \u25BC' : 'поля \u25B6';
                    });
                } else {
                    body.appendChild(row);
                }
            });
        }));

        // 11. CRM custom fields
        var crmFields = dryRunData.crm_custom_fields || {};
        var crmLabels = { deal: 'Сделки', contact: 'Контакты', company: 'Компании', lead: 'Лиды' };
        var hasAnyFields = Object.keys(crmFields).some(function(e) { return crmFields[e] && Object.keys(crmFields[e]).length > 0; });

        if (hasAnyFields) {
            sections.push(buildPlanSection('crm_custom_fields', 'Пользовательские поля CRM', null, plan.crm_custom_fields, function(body) {
                Object.keys(crmFields).forEach(function(entity) {
                    var fields = crmFields[entity];
                    if (!fields || Object.keys(fields).length === 0) return;

                    var fieldKeys = Object.keys(fields);
                    body.appendChild(makeSubAccordion(
                        (crmLabels[entity] || entity) + ' (' + fieldKeys.length + ' полей)',
                        null,
                        function(subBody) {
                            fieldKeys.forEach(function(code) {
                                var f = fields[code];
                                var fchecked = isFieldEnabled(plan, 'crm_custom_fields', entity, code);
                                var fr = el('div', { className: 'migrator-plan-field-item' });
                                var fcb = el('input');
                                fcb.type = 'checkbox'; fcb.checked = fchecked;
                                fcb.setAttribute('data-plan-field', 'crm_custom_fields');
                                fcb.setAttribute('data-entity-id', entity);
                                fcb.setAttribute('data-field-code', code);
                                fr.appendChild(fcb);
                                fr.appendChild(el('span', {}, code + ' \u2014 ' + (f.title || '') + ' (' + (f.type || '') + ')'));
                                subBody.appendChild(fr);
                            });
                        }
                    ));
                });
            }));
        }

        sections.forEach(function(s) { builder.appendChild(s); });

        // Wire select-all
        builder.querySelectorAll('[data-plan-select-all]').forEach(function(sa) {
            sa.addEventListener('change', function() {
                var group = sa.getAttribute('data-plan-select-all');
                var table = sa.closest('table');
                table.querySelectorAll('[data-plan-item="' + group + '"]').forEach(function(cb) { cb.checked = sa.checked; });
            });
        });

        // Build smart process checkboxes for delete-userfields setting
        var spContainer = document.getElementById('plan-delete-userfields-smart');
        if (spContainer && smartProcs.length > 0) {
            spContainer.innerHTML = '<div style="font-weight:600;margin-bottom:4px;font-size:13px;">Смарт-процессы:</div>';
            var skipEntities = (plan.delete_userfields || {}).skip_entities || [];
            smartProcs.forEach(function(sp) {
                var spId = String(sp.entityTypeId);
                var lbl = el('label', { style: 'display:flex;align-items:center;gap:6px;margin-bottom:4px;' });
                var cb = el('input');
                cb.type = 'checkbox';
                cb.className = 'plan-delete-uf-entity';
                cb.setAttribute('data-entity', 'smart_' + spId);
                cb.checked = skipEntities.indexOf('smart_' + spId) === -1;
                lbl.appendChild(cb);
                lbl.appendChild(el('span', {}, sp.title));
                spContainer.appendChild(lbl);
            });
        }

        updatePlanSummary();
        builder.addEventListener('change', function() { updatePlanSummary(); });
    }

    function buildDuplicateSettings(container, entityKey, plan, criteriaMap, actionsMap) {
        var saved = (plan.duplicate_settings || {})[entityKey] || {};
        var criteria = criteriaMap[entityKey] || [];
        if (criteria.length === 0) return;

        var wrapper = el('div', { className: 'migrator-duplicate-settings' });
        wrapper.appendChild(el('div', { style: 'font-weight:600;margin-bottom:8px;font-size:13px' }, 'Поиск дублей:'));

        var critDiv = el('div', { style: 'margin-bottom:10px' });
        var savedMatchBy = saved.match_by || [criteria[0].value];
        criteria.forEach(function(c) {
            var row = el('div', { className: 'migrator-plan-field-item' });
            var cb = el('input');
            cb.type = 'checkbox';
            cb.checked = savedMatchBy.indexOf(c.value) !== -1;
            cb.setAttribute('data-dup-criteria', entityKey);
            cb.setAttribute('data-dup-value', c.value);
            row.appendChild(cb);
            row.appendChild(el('span', {}, c.label));
            critDiv.appendChild(row);
        });
        wrapper.appendChild(critDiv);

        var actionRow = el('div', { style: 'display:flex;align-items:center;gap:8px' });
        actionRow.appendChild(el('span', { style: 'font-size:13px' }, 'При найденном дубле:'));
        var sel = el('select', { className: 'migrator-input', style: 'width:auto;font-size:13px' });
        sel.setAttribute('data-dup-action', entityKey);
        actionsMap.forEach(function(a) {
            var opt = el('option');
            opt.value = a.value;
            opt.textContent = a.label;
            if ((saved.action || 'skip') === a.value) opt.selected = true;
            sel.appendChild(opt);
        });
        actionRow.appendChild(sel);
        wrapper.appendChild(actionRow);

        container.appendChild(wrapper);
    }

    function buildPlanSection(key, title, count, savedState, bodyBuilder) {
        var section = el('div', { className: 'migrator-accordion migrator-plan-section' });
        section.setAttribute('data-plan-key', key);

        var header = el('div', { className: 'migrator-accordion-header' });

        var masterCb = el('input');
        masterCb.type = 'checkbox';
        masterCb.className = 'migrator-plan-master';
        masterCb.checked = savedState !== undefined ? (savedState.enabled !== false) : true;
        masterCb.setAttribute('data-plan-master', key);
        masterCb.addEventListener('click', function(e) { e.stopPropagation(); });
        masterCb.addEventListener('change', function() {
            var body = section.querySelector('.migrator-accordion-body');
            if (body) body.style.opacity = masterCb.checked ? '1' : '0.4';
        });
        header.appendChild(masterCb);

        if (bodyBuilder) {
            header.appendChild(el('span', { className: 'migrator-accordion-arrow' }, '\u25B6'));
        }

        header.appendChild(el('span', { className: 'migrator-accordion-title' }, title));

        if (count !== null) {
            var badgeWrap = el('span', { className: 'migrator-accordion-badges' });
            badgeWrap.appendChild(badge('', String(count), '#0080C8'));
            header.appendChild(badgeWrap);
        }

        section.appendChild(header);

        if (bodyBuilder) {
            var body = el('div', { className: 'migrator-accordion-body' });
            if (!masterCb.checked) body.style.opacity = '0.4';
            bodyBuilder(body);
            section.appendChild(body);

            header.addEventListener('click', function() {
                section.classList.toggle('open');
                var a = header.querySelector('.migrator-accordion-arrow');
                if (a) a.textContent = section.classList.contains('open') ? '\u25BC' : '\u25B6';
            });
            header.style.cursor = 'pointer';
        }

        return section;
    }

    function isItemEnabled(plan, sectionKey, itemId) {
        if (!plan || !plan[sectionKey]) return true;
        var excluded = plan[sectionKey].excluded_ids || [];
        return excluded.indexOf(itemId) === -1;
    }

    function isFieldEnabled(plan, sectionKey, entityId, fieldCode) {
        if (!plan || !plan[sectionKey] || !plan[sectionKey][entityId]) return true;
        var excluded = plan[sectionKey][entityId].excluded_fields || [];
        return excluded.indexOf(fieldCode) === -1;
    }

    function syncSelectAll(table) {
        var sa = table.querySelector('[data-plan-select-all]');
        if (!sa) return;
        var items = table.querySelectorAll('[data-plan-item]');
        var allChecked = true;
        items.forEach(function(cb) { if (!cb.checked) allChecked = false; });
        sa.checked = allChecked;
    }

    function collectPlan() {
        var plan = {};
        var builder = document.getElementById('plan-builder');

        builder.querySelectorAll('[data-plan-master]').forEach(function(mc) {
            var key = mc.getAttribute('data-plan-master');
            plan[key] = { enabled: mc.checked };

            var section = mc.closest('.migrator-plan-section');
            var items = section.querySelectorAll('[data-plan-item="' + key + '"]');
            if (items.length > 0) {
                var excluded = [];
                items.forEach(function(cb) { if (!cb.checked) excluded.push(cb.getAttribute('data-item-id')); });
                plan[key].excluded_ids = excluded;
            }
        });

        // CRM custom fields
        var crmFieldsCbs = builder.querySelectorAll('[data-plan-field="crm_custom_fields"]');
        if (crmFieldsCbs.length > 0) {
            plan.crm_custom_fields = plan.crm_custom_fields || { enabled: true };
            var crmExcl = {};
            crmFieldsCbs.forEach(function(cb) {
                var entity = cb.getAttribute('data-entity-id');
                var code = cb.getAttribute('data-field-code');
                if (!cb.checked) {
                    if (!crmExcl[entity]) crmExcl[entity] = { excluded_fields: [] };
                    crmExcl[entity].excluded_fields.push(code);
                }
            });
            Object.keys(crmExcl).forEach(function(e) { plan.crm_custom_fields[e] = crmExcl[e]; });
        }

        // Smart process fields
        var spFieldsCbs = builder.querySelectorAll('[data-plan-field="smart_process_fields"]');
        if (spFieldsCbs.length > 0) {
            if (!plan.smart_process_fields) plan.smart_process_fields = {};
            spFieldsCbs.forEach(function(cb) {
                var entityId = cb.getAttribute('data-entity-id');
                var code = cb.getAttribute('data-field-code');
                if (!cb.checked) {
                    if (!plan.smart_process_fields[entityId]) plan.smart_process_fields[entityId] = { excluded_fields: [] };
                    plan.smart_process_fields[entityId].excluded_fields.push(code);
                }
            });
        }

        // Duplicate settings
        plan.duplicate_settings = {};
        ['companies', 'contacts', 'leads', 'deals'].forEach(function(entityKey) {
            var critCbs = builder.querySelectorAll('[data-dup-criteria="' + entityKey + '"]');
            var actionSel = builder.querySelector('[data-dup-action="' + entityKey + '"]');
            if (critCbs.length === 0) return;

            var matchBy = [];
            critCbs.forEach(function(cb) { if (cb.checked) matchBy.push(cb.getAttribute('data-dup-value')); });
            plan.duplicate_settings[entityKey] = {
                match_by: matchBy,
                action: actionSel ? actionSel.value : 'skip'
            };
        });

        plan.settings = {
            user_match_strategy: document.getElementById('plan-user-strategy').value,
            conflict_resolution: document.getElementById('plan-conflict-resolution').value,
            send_invite: document.getElementById('plan-send-invite').value,
            delete_migrated_data: !!(document.getElementById('plan-delete-migrated-data') || {}).checked
        };

        // Delete userfields setting
        var deleteUfEnabled = document.getElementById('plan-delete-userfields').checked;
        var skipEntities = [];
        document.querySelectorAll('.plan-delete-uf-entity').forEach(function(cb) {
            if (!cb.checked) {
                skipEntities.push(cb.getAttribute('data-entity'));
            }
        });
        plan.delete_userfields = {
            enabled: deleteUfEnabled,
            skip_entities: skipEntities
        };

        // Cleanup settings (delete existing data before migration)
        var cleanup = {};
        document.querySelectorAll('.plan-cleanup-phase').forEach(function(cb) {
            var phase = cb.getAttribute('data-phase');
            if (cb.checked) cleanup[phase] = true;
        });
        plan.cleanup = cleanup;

        return plan;
    }

    function updatePlanSummary() {
        var summaryEl = document.getElementById('plan-summary');
        summaryEl.style.display = 'block';
        summaryEl.innerHTML = '';

        var plan = collectPlan();
        var items = [];

        var entityLabels = {
            departments: 'Подразделения', users: 'Пользователи', companies: 'Компании',
            contacts: 'Контакты', pipelines: 'Воронки', deals: 'Сделки', leads: 'Лиды',
            timeline: 'Таймлайн', tasks: 'Задачи', workgroups: 'Рабочие группы',
            smart_processes: 'Смарт-процессы', crm_custom_fields: 'Польз. поля CRM'
        };

        var order = ['departments', 'users', 'companies', 'contacts', 'pipelines', 'leads', 'deals', 'timeline', 'tasks', 'workgroups', 'smart_processes', 'crm_custom_fields'];
        order.forEach(function(key) {
            if (!plan[key]) return;
            var enabled = plan[key].enabled !== false;
            var excluded = (plan[key].excluded_ids || []).length;
            var label = entityLabels[key] || key;
            var statusIcon = enabled ? '\u2705' : '\u274C';
            var note = '';
            if (enabled && excluded > 0) note = ' (исключено: ' + excluded + ')';
            items.push(statusIcon + ' ' + label + note);
        });

        summaryEl.appendChild(el('h4', {}, 'Сводка плана миграции:'));
        var list = el('div', { className: 'migrator-plan-summary-list' });
        items.forEach(function(item) { list.appendChild(el('div', { className: 'migrator-plan-summary-item' }, item)); });
        summaryEl.appendChild(list);
    }

    function savePlan() {
        var plan = collectPlan();
        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);
        fd.append('plan', JSON.stringify(plan));

        var btn = document.getElementById('btn-save-plan');
        btn.disabled = true; btn.textContent = 'Сохранение...';

        fetch('/local/ajax/bitrix_migrator/save_plan.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) { savedPlan = plan; alert('План миграции сохранён'); }
                else alert('Ошибка: ' + (data.error || ''));
                btn.disabled = false; btn.textContent = 'Сохранить план';
            })
            .catch(function() { alert('Ошибка сохранения плана'); btn.disabled = false; btn.textContent = 'Сохранить план'; });
    }

    // =========================================================================
    // Migration
    // =========================================================================
    function initMigrationHandlers() {
        document.getElementById('btn-start-migration')?.addEventListener('click', startMigration);
        document.getElementById('btn-start-incremental')?.addEventListener('click', startIncrementalMigration);
        document.getElementById('btn-start-test-migration')?.addEventListener('click', startTestMigration);
        document.getElementById('btn-stop-migration')?.addEventListener('click', stopMigration);
        document.getElementById('btn-pause-migration')?.addEventListener('click', pauseMigration);
        document.getElementById('btn-resume-migration')?.addEventListener('click', resumeMigration);
        checkMigrationStatus();
    }

    function startMigration() {
        if (!confirm('Запустить полную миграцию данных из облака в коробку?')) return;

        var btn = document.getElementById('btn-start-migration');
        btn.disabled = true; btn.textContent = 'Запуск...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);
        fd.append('type', 'full');

        fetch('/local/ajax/bitrix_migrator/start_migration.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    showMigrationProgress();
                    showMigrationRunningUI();
                    startMigrationPolling();
                } else {
                    alert('Ошибка: ' + (data.error || ''));
                    btn.disabled = false; btn.textContent = 'Запустить миграцию';
                }
            })
            .catch(function() {
                alert('Ошибка запуска миграции');
                btn.disabled = false; btn.textContent = 'Запустить миграцию';
            });
    }

    function startTestMigration() {
        var typeEl = document.querySelector('input[name="test-scope-type"]:checked');
        var scopeType = typeEl ? typeEl.value : 'company';
        var idInput = document.getElementById('test-scope-id');
        var scopeId = parseInt(idInput.value, 10);
        if (!scopeId || scopeId <= 0) {
            alert('Укажите ID сущности в облаке');
            idInput.focus();
            return;
        }
        var skipPrereqs = document.getElementById('test-skip-prereqs').checked;
        var typeLabel = scopeType === 'company' ? 'компания' : (scopeType === 'contact' ? 'контакт' : 'задача');
        if (!confirm('Запустить тестовый прогон для: ' + typeLabel + ' #' + scopeId + '?')) return;

        var btn = document.getElementById('btn-start-test-migration');
        btn.disabled = true; btn.textContent = 'Запуск...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);
        fd.append('type', 'full');
        fd.append('scope_type', scopeType);
        fd.append('scope_ids', String(scopeId));
        fd.append('scope_skip_prereqs', skipPrereqs ? '1' : '0');

        fetch('/local/ajax/bitrix_migrator/start_migration.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    showMigrationProgress();
                    showMigrationRunningUI();
                    startMigrationPolling();
                } else {
                    alert('Ошибка: ' + (data.error || ''));
                    btn.disabled = false; btn.textContent = 'Запустить тестовый прогон';
                }
            })
            .catch(function() {
                alert('Ошибка запуска тестового прогона');
                btn.disabled = false; btn.textContent = 'Запустить тестовый прогон';
            });
    }

    function startIncrementalMigration() {
        var lastTs = window.BITRIX_MIGRATOR.lastMigrationTimestamp || '';
        var msg = 'Запустить инкрементальную миграцию?';
        if (lastTs) msg += '\nБудут перенесены только записи, созданные после ' + lastTs;
        if (!confirm(msg)) return;

        var btn = document.getElementById('btn-start-incremental');
        var fullBtn = document.getElementById('btn-start-migration');
        btn.disabled = true; btn.textContent = 'Запуск...';
        fullBtn.disabled = true;

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);
        fd.append('type', 'incremental');

        fetch('/local/ajax/bitrix_migrator/start_migration.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    showMigrationProgress();
                    showMigrationRunningUI();
                    startMigrationPolling();
                } else {
                    alert('Ошибка: ' + (data.error || ''));
                    btn.disabled = false; btn.textContent = 'Инкрементальная миграция';
                    fullBtn.disabled = false;
                }
            })
            .catch(function() {
                alert('Ошибка запуска миграции');
                btn.disabled = false; btn.textContent = 'Инкрементальная миграция';
                fullBtn.disabled = false;
            });
    }

    function stopMigration() {
        if (!confirm('Остановить миграцию? Процесс завершится после текущей операции.')) return;

        var btn = document.getElementById('btn-stop-migration');
        btn.disabled = true; btn.textContent = 'Остановка...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);

        fetch('/local/ajax/bitrix_migrator/stop_migration.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (!data.success) {
                    alert('Ошибка: ' + (data.error || ''));
                    btn.disabled = false; btn.textContent = 'Остановить миграцию';
                }
                // Polling will handle the UI update
            })
            .catch(function() {
                alert('Ошибка остановки');
                btn.disabled = false; btn.textContent = 'Остановить миграцию';
            });
    }

    function pauseMigration() {
        var btn = document.getElementById('btn-pause-migration');
        btn.disabled = true; btn.textContent = 'Пауза...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);

        fetch('/local/ajax/bitrix_migrator/pause_migration.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (!data.success) {
                    alert('Ошибка: ' + (data.error || ''));
                    btn.disabled = false; btn.textContent = 'Пауза';
                }
            })
            .catch(function() {
                alert('Ошибка паузы');
                btn.disabled = false; btn.textContent = 'Пауза';
            });
    }

    function resumeMigration() {
        var btn = document.getElementById('btn-resume-migration');
        btn.disabled = true; btn.textContent = 'Продолжение...';

        var fd = new FormData();
        fd.append('sessid', window.BITRIX_MIGRATOR.sessid);

        fetch('/local/ajax/bitrix_migrator/resume_migration.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (!data.success) {
                    alert('Ошибка: ' + (data.error || ''));
                    btn.disabled = false; btn.textContent = 'Продолжить';
                }
            })
            .catch(function() {
                alert('Ошибка возобновления');
                btn.disabled = false; btn.textContent = 'Продолжить';
            });
    }

    function showMigrationRunningUI() {
        var startBtn = document.getElementById('btn-start-migration');
        var incrementalBtn = document.getElementById('btn-start-incremental');
        var stopBtn = document.getElementById('btn-stop-migration');
        var pauseBtn = document.getElementById('btn-pause-migration');
        var resumeBtn = document.getElementById('btn-resume-migration');
        startBtn.disabled = true;
        startBtn.textContent = 'Миграция выполняется...';
        if (incrementalBtn) { incrementalBtn.disabled = true; }
        stopBtn.style.display = 'inline-flex';
        stopBtn.disabled = false;
        stopBtn.textContent = 'Остановить миграцию';
        pauseBtn.style.display = 'inline-flex';
        pauseBtn.disabled = false;
        pauseBtn.textContent = 'Пауза';
        resumeBtn.style.display = 'none';
    }

    function showMigrationPausedUI() {
        var startBtn = document.getElementById('btn-start-migration');
        var stopBtn = document.getElementById('btn-stop-migration');
        var pauseBtn = document.getElementById('btn-pause-migration');
        var resumeBtn = document.getElementById('btn-resume-migration');
        startBtn.disabled = true;
        startBtn.textContent = 'Миграция на паузе';
        pauseBtn.style.display = 'none';
        resumeBtn.style.display = 'inline-flex';
        resumeBtn.disabled = false;
        resumeBtn.textContent = 'Продолжить';
        stopBtn.style.display = 'inline-flex';
        stopBtn.disabled = false;
        stopBtn.textContent = 'Остановить миграцию';
    }

    function showMigrationIdleUI() {
        var startBtn = document.getElementById('btn-start-migration');
        var incrementalBtn = document.getElementById('btn-start-incremental');
        var stopBtn = document.getElementById('btn-stop-migration');
        var pauseBtn = document.getElementById('btn-pause-migration');
        var resumeBtn = document.getElementById('btn-resume-migration');
        var pidInfo = document.getElementById('migration-pid-info');
        startBtn.disabled = false;
        startBtn.textContent = 'Запустить миграцию';
        if (incrementalBtn) {
            incrementalBtn.disabled = !window.BITRIX_MIGRATOR.lastMigrationTimestamp;
            incrementalBtn.textContent = 'Инкрементальная миграция';
        }
        stopBtn.style.display = 'none';
        pauseBtn.style.display = 'none';
        resumeBtn.style.display = 'none';
        if (pidInfo) pidInfo.style.display = 'none';
    }

    function showMigrationProgress() {
        document.getElementById('migration-progress').style.display = 'block';
        document.getElementById('migration-phases').style.display = 'block';
    }

    function startMigrationPolling() {
        if (migrationPollTimer) clearInterval(migrationPollTimer);
        migrationPollTimer = setInterval(pollMigrationStatus, 3000);
        pollMigrationStatus();
    }

    function checkMigrationStatus() {
        fetch('/local/ajax/bitrix_migrator/get_migration_status.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success && (data.status === 'running' || data.status === 'stopping')) {
                    showMigrationProgress();
                    showMigrationRunningUI();
                    startMigrationPolling();
                    if (data.pid > 0) {
                        var pidInfo = document.getElementById('migration-pid-info');
                        if (pidInfo) {
                            pidInfo.style.display = 'inline';
                            pidInfo.textContent = 'PID: ' + data.pid;
                        }
                    }
                } else if (data.success && data.status === 'paused') {
                    showMigrationProgress();
                    showMigrationPausedUI();
                    startMigrationPolling();
                } else if (data.success && (data.status === 'completed' || data.status === 'error' || data.status === 'stopped')) {
                    if (data.log && data.log.length > 0) {
                        showMigrationProgress();
                        updateMigrationUI(data);
                    }
                }
            })
            .catch(function() {});
    }

    function pollMigrationStatus() {
        fetch('/local/ajax/bitrix_migrator/get_migration_status.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (!data.success) return;
                updateMigrationUI(data);

                // Show PID
                if (data.pid > 0) {
                    var pidInfo = document.getElementById('migration-pid-info');
                    if (pidInfo) {
                        pidInfo.style.display = 'inline';
                        pidInfo.textContent = 'PID: ' + data.pid;
                    }
                }

                if (data.status === 'paused') {
                    showMigrationPausedUI();
                } else if (data.status === 'running') {
                    showMigrationRunningUI();
                }

                if (data.status === 'completed' || data.status === 'error' || data.status === 'stopped') {
                    clearInterval(migrationPollTimer);
                    migrationPollTimer = null;
                    showMigrationIdleUI();
                }
            })
            .catch(function() {});
    }

    function updateMigrationUI(data) {
        var statusBadge = document.getElementById('migration-status-badge');
        var messageEl   = document.getElementById('migration-message');
        var phaseLabel  = document.getElementById('migration-phase-label');
        var statsRow    = document.getElementById('migration-stats-row');
        var progressBar = document.getElementById('migration-progress-bar');
        var logEl       = document.getElementById('migration-log');

        // Status badge
        var statusColors = { running: '#ffc107', completed: '#28a745', error: '#dc3545', idle: '#6c757d', stopping: '#e67e22', stopped: '#e67e22' };
        var statusLabels = { running: 'Выполняется', completed: 'Завершено', error: 'Ошибка', idle: 'Ожидание', stopping: 'Останавливается...', stopped: 'Остановлено' };
        var color = statusColors[data.status] || '#6c757d';
        statusBadge.style.borderColor = color;
        statusBadge.style.color = color;
        statusBadge.textContent = statusLabels[data.status] || data.status;

        // Message
        messageEl.textContent = data.message || '';

        // Phase label
        if (phaseLabel) phaseLabel.textContent = data.message || '';

        // Phase list
        var phases = data.phases || {};
        var progress = data.progress || {};
        var phasesEl = document.getElementById('migration-phases');
        if (phasesEl) {
            phasesEl.innerHTML = '';
            var phaseList = el('div', { className: 'migrator-phase-list' });

            PHASE_ORDER.forEach(function(key) {
                var st = phases[key] || 'pending';
                var cls = 'migrator-phase-item';
                var icon = '\u2022';

                if (st === 'running') { cls += ' migrator-phase-item--active'; icon = '\u25B6'; }
                else if (st === 'done') { cls += ' migrator-phase-item--done'; icon = '\u2714'; }
                else if (st === 'error') { cls += ' migrator-phase-item--error'; icon = '\u2716'; }
                else if (st === 'skipped') { icon = '\u2014'; }

                var item = el('div', { className: cls });
                item.appendChild(el('span', { className: 'migrator-phase-icon' }, icon));
                item.appendChild(el('span', {}, PHASE_LABELS[key] || key));

                // Show progress for running phase
                if (st === 'running' && progress[key]) {
                    var p = progress[key];
                    var pctText = p.total > 0 ? Math.round((p.offset / p.total) * 100) + '%' : '';
                    var progressText = p.offset + '/' + p.total;
                    if (pctText) progressText += ' (' + pctText + ')';
                    item.appendChild(el('span', { style: 'margin-left:auto;font-size:12px;color:#0080C8;font-weight:600' }, progressText));
                } else if (st === 'skipped') {
                    item.appendChild(el('span', { style: 'margin-left:auto;font-size:12px;color:#999' }, 'пропущено'));
                }

                phaseList.appendChild(item);
            });

            phasesEl.appendChild(phaseList);
        }

        // Stats
        var stats = data.stats || {};
        statsRow.innerHTML = '';
        if (stats.total > 0) {
            statsRow.appendChild(statCard(String(stats.total), 'Всего'));
            statsRow.appendChild(statCard(String(stats.migrated || 0), 'Перенесено', 'success'));
            statsRow.appendChild(statCard(String(stats.errors || 0), 'Ошибок', stats.errors > 0 ? 'warning' : undefined));
            statsRow.appendChild(statCard(String(stats.current || 0), 'Текущая'));
        }

        // Progress bar
        var donePhases = 0;
        var totalPhases = PHASE_ORDER.length;
        PHASE_ORDER.forEach(function(k) {
            var s = phases[k] || 'pending';
            if (s === 'done' || s === 'skipped') donePhases++;
            else if (s === 'running') donePhases += 0.5;
        });
        var pct = totalPhases > 0 ? Math.round((donePhases / totalPhases) * 100) : 0;
        progressBar.style.width = pct + '%';

        // Log
        if (data.log && data.log.length > 0) {
            logEl.textContent = data.log.join('\n');
            logEl.scrollTop = logEl.scrollHeight;
        }
    }

    // =========================================================================
    // Logs Tab
    // =========================================================================
    function initLogsHandlers() {
        document.getElementById('btn-refresh-logs')?.addEventListener('click', loadLogs);
        document.getElementById('logs-auto-refresh')?.addEventListener('change', function() {
            if (this.checked) {
                loadLogs();
                logsAutoRefreshTimer = setInterval(loadLogs, 3000);
            } else {
                if (logsAutoRefreshTimer) { clearInterval(logsAutoRefreshTimer); logsAutoRefreshTimer = null; }
            }
        });
    }

    function loadLogs() {
        fetch('/local/ajax/bitrix_migrator/get_migration_status.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (!data.success) return;

                var emptyEl = document.getElementById('logs-empty');
                var boxEl = document.getElementById('logs-box');
                var filePathEl = document.getElementById('logs-file-path');

                if (data.log && data.log.length > 0) {
                    emptyEl.style.display = 'none';
                    boxEl.style.display = 'block';
                    boxEl.textContent = data.log.join('\n');
                    boxEl.scrollTop = boxEl.scrollHeight;
                } else {
                    emptyEl.style.display = 'block';
                    boxEl.style.display = 'none';
                }

                if (data.log_file && filePathEl) {
                    filePathEl.textContent = 'Файл: ' + data.log_file;
                }
            })
            .catch(function() {});
    }

    // =========================================================================
    // Accordion builders
    // =========================================================================
    function makeAccordion(title, badgeContent, bodyBuilder) {
        var section = el('div', { className: 'migrator-accordion' });
        var header = el('div', { className: 'migrator-accordion-header' });

        if (bodyBuilder) {
            header.appendChild(el('span', { className: 'migrator-accordion-arrow' }, '\u25B6'));
        }
        header.appendChild(el('span', { className: 'migrator-accordion-title' }, title));

        if (badgeContent) {
            var bw = el('span', { className: 'migrator-accordion-badges' });
            if (Array.isArray(badgeContent)) badgeContent.forEach(function(b) { bw.appendChild(b); });
            else bw.appendChild(badge('', badgeContent, '#0080C8'));
            header.appendChild(bw);
        }

        section.appendChild(header);

        if (bodyBuilder) {
            var body = el('div', { className: 'migrator-accordion-body' });
            bodyBuilder(body);
            section.appendChild(body);
            header.addEventListener('click', function() {
                section.classList.toggle('open');
                var a = header.querySelector('.migrator-accordion-arrow');
                if (a) a.textContent = section.classList.contains('open') ? '\u25BC' : '\u25B6';
            });
            header.style.cursor = 'pointer';
        }
        return section;
    }

    function makeSubAccordion(title, badgeText, bodyBuilder) {
        var section = el('div', { className: 'migrator-sub-accordion' });
        var header = el('div', { className: 'migrator-sub-accordion-header' });
        var arrow = el('span', { className: 'migrator-accordion-arrow' }, '\u25B6');
        header.appendChild(arrow);
        header.appendChild(el('span', {}, title));
        if (badgeText) header.appendChild(badge('', badgeText, '#6c757d'));
        section.appendChild(header);

        var body = el('div', { className: 'migrator-sub-accordion-body' });
        if (bodyBuilder) bodyBuilder(body);
        section.appendChild(body);

        header.addEventListener('click', function() {
            section.classList.toggle('open');
            arrow.textContent = section.classList.contains('open') ? '\u25BC' : '\u25B6';
        });
        header.style.cursor = 'pointer';
        return section;
    }

    // =========================================================================
    // Department Tree
    // =========================================================================
    function buildDepartmentTree(departments, container) {
        var depMap = {}, rootDeps = [];
        departments.forEach(function(dep) { depMap[dep.ID] = Object.assign({}, dep, { children: [] }); });
        departments.forEach(function(dep) {
            if (dep.PARENT && depMap[dep.PARENT]) depMap[dep.PARENT].children.push(depMap[dep.ID]);
            else rootDeps.push(depMap[dep.ID]);
        });
        var ul = el('ul', { className: 'migrator-tree' });
        rootDeps.forEach(function(dep) { ul.appendChild(createTreeNode(dep, true)); });
        container.appendChild(ul);
    }

    function createTreeNode(department, expandRoot) {
        var li = el('li', { className: 'migrator-tree-item' + (expandRoot ? ' expanded' : '') });
        var nc = el('div', { className: 'migrator-tree-node' });
        if (department.children && department.children.length > 0) {
            var toggle = el('span', { className: 'migrator-tree-toggle' }, expandRoot ? '\u25BC' : '\u25B6');
            toggle.addEventListener('click', function() { li.classList.toggle('expanded'); toggle.textContent = li.classList.contains('expanded') ? '\u25BC' : '\u25B6'; });
            nc.appendChild(toggle);
        } else nc.appendChild(el('span', { className: 'migrator-tree-spacer' }));

        var label = el('span', { className: 'migrator-tree-label' }, department.NAME);
        label.appendChild(el('span', { className: 'migrator-tree-id' }, ' #' + department.ID));
        nc.appendChild(label);
        li.appendChild(nc);

        if (department.children && department.children.length > 0) {
            var childUl = el('ul', { className: 'migrator-tree-children' });
            department.children.forEach(function(child) { childUl.appendChild(createTreeNode(child, false)); });
            li.appendChild(childUl);
        }
        return li;
    }

    // =========================================================================
    // Department Modal — OrgChart-based (Bitrix-style hierarchy)
    // =========================================================================
    function _userFullName(u) {
        return [u.LAST_NAME, u.NAME, u.SECOND_NAME].filter(Boolean).join(' ') || ('ID:' + u.ID);
    }

    function _esc(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function _pluralize(n, forms) {
        var mod10 = n % 10, mod100 = n % 100;
        if (mod10 === 1 && mod100 !== 11) return forms[0];
        if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return forms[1];
        return forms[2];
    }

    function _deptModalEscHandler(e) {
        if (e.key === 'Escape') closeDepartmentModal();
    }

    var _deptModalPanCleanup = null;
    function closeDepartmentModal() {
        var existing = document.getElementById('migrator-dept-modal');
        if (document.fullscreenElement || document.webkitFullscreenElement) {
            try {
                var exitFs = document.exitFullscreen || document.webkitExitFullscreen || document.mozCancelFullScreen || document.msExitFullscreen;
                if (exitFs) exitFs.call(document);
            } catch (e) { /* ignore */ }
        }
        if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
        document.removeEventListener('keydown', _deptModalEscHandler);
        if (_deptModalPanCleanup) { _deptModalPanCleanup(); _deptModalPanCleanup = null; }
    }

    function _closeEmployeesModal() {
        var m = document.getElementById('migrator-dept-employees-modal');
        if (m && m.parentNode) m.parentNode.removeChild(m);
    }

    function openDeptEmployeesModal(dept, userById) {
        _closeEmployeesModal();

        var overlay = document.createElement('div');
        overlay.className = 'migrator-modal-overlay migrator-modal-overlay--nested';
        overlay.id = 'migrator-dept-employees-modal';
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) _closeEmployeesModal();
        });

        var dialog = document.createElement('div');
        dialog.className = 'migrator-modal-dialog migrator-modal-dialog--employees';
        overlay.appendChild(dialog);

        var header = el('div', { className: 'migrator-modal-header' });
        var titleWrap = el('div');
        titleWrap.appendChild(el('div', { className: 'migrator-modal-title' }, 'Сотрудники: ' + (dept.name || '')));
        var subTitle = el('div', { className: 'migrator-modal-subtitle' }, dept.userIds.length + ' ' + _pluralize(dept.userIds.length, ['сотрудник', 'сотрудника', 'сотрудников']));
        titleWrap.appendChild(subTitle);
        header.appendChild(titleWrap);
        var closeBtn = el('button', { type: 'button', className: 'migrator-modal-close', 'aria-label': 'Закрыть' }, '\u00D7');
        closeBtn.addEventListener('click', _closeEmployeesModal);
        header.appendChild(closeBtn);
        dialog.appendChild(header);

        var body = el('div', { className: 'migrator-modal-employees-body' });

        if (dept.userIds.length === 0) {
            body.appendChild(el('div', { className: 'migrator-modal-empty' }, 'В этом подразделении нет сотрудников'));
        } else {
            var users = dept.userIds.map(function(id) { return userById[id]; }).filter(Boolean);
            users.sort(function(a, b) {
                if (dept.ufHead && a.ID == dept.ufHead) return -1;
                if (dept.ufHead && b.ID == dept.ufHead) return 1;
                return (a.LAST_NAME || '').localeCompare(b.LAST_NAME || '');
            });

            var grid = el('div', { className: 'migrator-modal-users' });
            users.forEach(function(u) {
                var card = el('div', { className: 'migrator-modal-user' });
                var avatar = el('div', { className: 'migrator-modal-user-avatar' });
                if (u.PERSONAL_PHOTO && String(u.PERSONAL_PHOTO).indexOf('http') === 0) {
                    avatar.style.backgroundImage = 'url(' + u.PERSONAL_PHOTO + ')';
                } else {
                    avatar.textContent = ((u.NAME || ' ').charAt(0) + (u.LAST_NAME || ' ').charAt(0)).toUpperCase();
                }
                card.appendChild(avatar);

                var info = el('div', { className: 'migrator-modal-user-info' });
                var nameLine = el('div', { className: 'migrator-modal-user-name' }, _userFullName(u));
                if (dept.ufHead && u.ID == dept.ufHead) {
                    nameLine.appendChild(el('span', { className: 'migrator-modal-user-badge' }, 'Руководитель'));
                }
                info.appendChild(nameLine);
                if (u.WORK_POSITION) info.appendChild(el('div', { className: 'migrator-modal-user-pos' }, u.WORK_POSITION));
                var meta = [];
                if (u.EMAIL) meta.push(u.EMAIL);
                if (u.WORK_PHONE) meta.push(u.WORK_PHONE);
                if (meta.length > 0) info.appendChild(el('div', { className: 'migrator-modal-user-meta' }, meta.join(' \u00B7 ')));

                card.appendChild(info);
                grid.appendChild(card);
            });
            body.appendChild(grid);
        }

        dialog.appendChild(body);
        // Append inside fullscreen element if present, otherwise body
        var parent = document.fullscreenElement || document.webkitFullscreenElement || document.body;
        parent.appendChild(overlay);
    }

    function _loadOrgChartLib(cb) {
        if (typeof jQuery !== 'undefined' && jQuery.fn && jQuery.fn.orgchart) { cb(); return; }
        var basePath = '/local/modules/bitrix_migrator/install/admin/js/vendor/orgchart.min.js';
        function loadPlugin() {
            if (jQuery.fn && jQuery.fn.orgchart) { cb(); return; }
            var s = document.createElement('script');
            s.src = basePath;
            s.onload = function() { cb(); };
            s.onerror = function() { cb(new Error('Не удалось загрузить orgchart.min.js')); };
            document.head.appendChild(s);
        }
        if (typeof jQuery === 'undefined') {
            var j = document.createElement('script');
            j.src = '/bitrix/js/main/jquery/jquery-3.6.0.min.js';
            j.onload = loadPlugin;
            j.onerror = function() { cb(new Error('Не удалось загрузить jQuery')); };
            document.head.appendChild(j);
        } else {
            loadPlugin();
        }
    }

    function openDepartmentModal(departments, allUsers) {
        closeDepartmentModal();
        _loadOrgChartLib(function(err) {
            if (err) { alert(err.message); return; }
            _openDepartmentModalInternal(departments, allUsers);
        });
    }

    function _openDepartmentModalInternal(departments, allUsers) {
        var depMap = {};
        departments.forEach(function(d) {
            depMap[d.ID] = { id: d.ID, name: d.NAME || ('ID:' + d.ID), parent: d.PARENT || null, ufHead: d.UF_HEAD || null, userIds: [], children: [] };
        });
        var roots = [];
        departments.forEach(function(d) {
            var node = depMap[d.ID];
            if (node.parent && depMap[node.parent]) depMap[node.parent].children.push(node);
            else roots.push(node);
        });

        var userById = {};
        (allUsers || []).forEach(function(u) {
            userById[u.ID] = u;
            (u.UF_DEPARTMENT || []).forEach(function(did) {
                if (depMap[did]) depMap[did].userIds.push(u.ID);
            });
        });

        function toDatasource(node) {
            var head = node.ufHead && userById[node.ufHead] ? userById[node.ufHead] : null;
            var subDeptCount = node.children.length;
            var ds = {
                id: 'dep_' + node.id,
                name: node.name,
                _deptId: node.id,
                _head: head,
                _directCount: node.userIds.length,
                _childDeptCount: subDeptCount
            };
            if (subDeptCount > 0) ds.children = node.children.map(toDatasource);
            return ds;
        }

        var datasource;
        if (roots.length === 1) {
            datasource = toDatasource(roots[0]);
        } else {
            datasource = {
                id: 'dep_virtual_root',
                name: 'Компания',
                _deptId: 0, _head: null, _directCount: 0,
                _childDeptCount: roots.length,
                children: roots.map(toDatasource)
            };
        }

        var overlay = document.createElement('div');
        overlay.className = 'migrator-modal-overlay migrator-modal-overlay--fullscreen';
        overlay.id = 'migrator-dept-modal';

        var dialog = document.createElement('div');
        dialog.className = 'migrator-modal-dialog migrator-modal-dialog--chart';
        overlay.appendChild(dialog);

        var header = el('div', { className: 'migrator-modal-header' });
        header.appendChild(el('div', { className: 'migrator-modal-title' }, 'Структура компании'));
        var closeBtn = el('button', { type: 'button', className: 'migrator-modal-close', 'aria-label': 'Закрыть' }, '\u00D7');
        closeBtn.addEventListener('click', closeDepartmentModal);
        header.appendChild(closeBtn);
        dialog.appendChild(header);

        var chartHost = document.createElement('div');
        chartHost.className = 'migrator-orgchart-host';
        dialog.appendChild(chartHost);

        var pannable = document.createElement('div');
        pannable.className = 'migrator-orgchart-pannable';
        chartHost.appendChild(pannable);

        document.body.appendChild(overlay);

        // Request real fullscreen (F11-style) on the overlay
        var reqFs = overlay.requestFullscreen || overlay.webkitRequestFullscreen || overlay.mozRequestFullScreen || overlay.msRequestFullscreen;
        if (reqFs) {
            try { reqFs.call(overlay); } catch (e) { /* ignore */ }
        }

        function nodeTemplate(data) {
            var head = data._head;
            var headHtml = '';
            if (head) {
                var photo = (head.PERSONAL_PHOTO && String(head.PERSONAL_PHOTO).indexOf('http') === 0)
                    ? '<span class="mig-oc-avatar" style="background-image:url(\'' + _esc(head.PERSONAL_PHOTO) + '\')"></span>'
                    : '<span class="mig-oc-avatar mig-oc-avatar--initials">' + _esc(((head.NAME || ' ').charAt(0) + (head.LAST_NAME || ' ').charAt(0)).toUpperCase()) + '</span>';
                headHtml =
                    '<div class="mig-oc-head">' +
                        photo +
                        '<div class="mig-oc-head-info">' +
                            '<div class="mig-oc-head-name">' + _esc(_userFullName(head)) + '</div>' +
                            '<div class="mig-oc-head-pos">' + _esc(head.WORK_POSITION || 'Должность не указана') + '</div>' +
                        '</div>' +
                        '<span class="mig-oc-head-badge">\uD83D\uDC65 ' + data._directCount + '</span>' +
                    '</div>';
            } else if (data._directCount > 0) {
                headHtml =
                    '<div class="mig-oc-head mig-oc-head--empty">' +
                        '<span class="mig-oc-head-badge">\uD83D\uDC65 ' + data._directCount + ' сотр.</span>' +
                    '</div>';
            }

            var subLabel = data._childDeptCount > 0
                ? '<div class="mig-oc-sub">Подчинённые<br><span class="mig-oc-subcount">' + data._childDeptCount + ' ' + _pluralize(data._childDeptCount, ['отдел', 'отдела', 'отделов']) + '</span></div>'
                : '<div class="mig-oc-sub mig-oc-sub--empty">нет отделов в подчинении</div>';

            return '' +
                '<div class="mig-oc-card-inner">' +
                    '<div class="mig-oc-title">' +
                        '<span class="mig-oc-icon">\uD83C\uDFE2</span>' +
                        '<span class="mig-oc-name">' + _esc(data.name) + '</span>' +
                    '</div>' +
                    headHtml +
                    subLabel +
                '</div>';
        }

        if (typeof jQuery === 'undefined' || !jQuery.fn.orgchart) {
            chartHost.textContent = 'Ошибка: библиотека OrgChart не загружена';
            return;
        }

        jQuery(pannable).orgchart({
            data: datasource,
            nodeContent: 'name',
            visibleLevel: 3,
            pan: false,
            zoom: true,
            zoominLimit: 2,
            zoomoutLimit: 0.4,
            nodeTemplate: nodeTemplate
        });

        // Click on card -> open employees modal
        jQuery(pannable).on('click', '.node', function(e) {
            if (jQuery(e.target).closest('.edge, .topEdge, .bottomEdge, .leftEdge, .rightEdge, .toggleBtn').length) return;
            var id = this.id || '';
            var m = id.match(/^dep_(\d+)$/);
            if (!m) return;
            var deptId = parseInt(m[1], 10);
            var dept = depMap[deptId];
            if (dept) openDeptEmployeesModal(dept, userById);
        });

        // Custom pan — applied on wrapper, preserves OrgChart's zoom on .orgchart
        var panState = { x: 0, y: 0, startX: 0, startY: 0, dragging: false };
        function applyPan() {
            pannable.style.transform = 'translate(' + panState.x + 'px, ' + panState.y + 'px)';
        }
        function onPanDown(e) {
            if (e.button !== 0) return;
            var t = e.target;
            if (t.closest && t.closest('.node')) return; // let node clicks pass
            panState.dragging = true;
            panState.startX = e.clientX - panState.x;
            panState.startY = e.clientY - panState.y;
            chartHost.classList.add('migrator-panning');
            e.preventDefault();
        }
        function onPanMove(e) {
            if (!panState.dragging) return;
            panState.x = e.clientX - panState.startX;
            panState.y = e.clientY - panState.startY;
            applyPan();
        }
        function onPanUp() {
            if (!panState.dragging) return;
            panState.dragging = false;
            chartHost.classList.remove('migrator-panning');
        }
        chartHost.addEventListener('mousedown', onPanDown);
        document.addEventListener('mousemove', onPanMove);
        document.addEventListener('mouseup', onPanUp);
        _deptModalPanCleanup = function() {
            document.removeEventListener('mousemove', onPanMove);
            document.removeEventListener('mouseup', onPanUp);
        };

        document.addEventListener('keydown', _deptModalEscHandler);
    }

    // =========================================================================
    // New Users List
    // =========================================================================
    function buildNewUsersList(users, deptNameMap, container) {
        var t = el('table', { className: 'migrator-table' });
        t.innerHTML = '<thead><tr><th>ФИО</th><th>Email</th><th>Должность</th><th>Отдел</th></tr></thead>';
        var tb = document.createElement('tbody');
        users.forEach(function(u) {
            var r = document.createElement('tr');
            var name = [u.LAST_NAME, u.NAME, u.SECOND_NAME].filter(Boolean).join(' ');
            var deptIds = u.UF_DEPARTMENT || u.DEPARTMENT || [];
            if (!Array.isArray(deptIds)) deptIds = [deptIds];
            var dn = deptIds.map(function(id) { return deptNameMap[String(id)] || ('ID:' + id); }).join(', ');
            r.innerHTML = '<td>' + esc(name) + '</td><td>' + esc(u.EMAIL || '') + '</td><td>' + esc(u.WORK_POSITION || '') + '</td><td>' + esc(dn) + '</td>';
            tb.appendChild(r);
        });
        t.appendChild(tb); container.appendChild(t);
    }

    // =========================================================================
    // Helpers
    // =========================================================================
    function el(tag, attrs, text) {
        var node = document.createElement(tag);
        if (attrs) Object.keys(attrs).forEach(function(k) {
            if (k === 'className') node.className = attrs[k];
            else if (k === 'style') node.style.cssText = attrs[k];
            else node.setAttribute(k, attrs[k]);
        });
        if (text) node.textContent = text;
        return node;
    }

    function badge(label, value, color) {
        var b = el('span', { className: 'migrator-badge' });
        b.style.borderColor = color || '#0080C8';
        b.style.color = color || '#0080C8';
        b.textContent = label ? label + ': ' + value : value;
        return b;
    }

    function statCard(value, label, variant) {
        var card = el('div', { className: 'migrator-stat-card' + (variant ? ' migrator-stat-card--' + variant : '') });
        card.appendChild(el('div', { className: 'migrator-stat-value' }, String(value)));
        card.appendChild(el('div', { className: 'migrator-stat-label' }, label));
        return card;
    }

    function val(v) { return v !== undefined && v !== null ? String(v) : '\u2014'; }

    function esc(str) {
        return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // =====================================================================
    // Repair (Дозаполнение) tab
    // =====================================================================

    var repairPollTimer = null;
    var activeRepairBtn = null;

    window.startRepair = function(btn) {
        // Collect checkboxes from the accordion section that contains the clicked button
        var section = btn.closest('.migrator-accordion-body');
        var checkboxes = section.querySelectorAll('input[name="repair_type"]:checked');
        var types = [];
        checkboxes.forEach(function(cb) { types.push(cb.value); });
        if (types.length === 0) {
            alert('Выберите хотя бы один тип');
            return;
        }

        // Disable all start buttons, show stop
        activeRepairBtn = btn;
        document.querySelectorAll('.btn-repair-start').forEach(function(b) { b.disabled = true; });
        document.getElementById('btn-stop-repair').style.display = '';
        document.getElementById('repair-progress-section').style.display = '';
        document.getElementById('repair-log-section').style.display = '';
        document.getElementById('repair-status-text').textContent = 'Запуск...';
        document.getElementById('repair-progress-bar').style.width = '0%';
        document.getElementById('repair-progress-text').textContent = '';
        document.getElementById('repair-log').textContent = '';

        var fd = new FormData();
        fd.append('sessid', BX.bitrix_sessid());
        fd.append('types', JSON.stringify(types));

        fetch('/local/ajax/bitrix_migrator/start_repair.php', { method: 'POST', body: fd })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.success) {
                    repairPollTimer = setInterval(pollRepairStatus, 2000);
                } else {
                    document.getElementById('repair-status-text').textContent = 'Ошибка: ' + (data.error || '');
                    document.querySelectorAll('.btn-repair-start').forEach(function(b) { b.disabled = false; });
                    document.getElementById('btn-stop-repair').style.display = 'none';
                }
            })
            .catch(function(err) {
                document.getElementById('repair-status-text').textContent = 'Ошибка сети: ' + err;
                document.querySelectorAll('.btn-repair-start').forEach(function(b) { b.disabled = false; });
            });
    };

    window.stopRepair = function() {
        var fd = new FormData();
        fd.append('sessid', BX.bitrix_sessid());
        fetch('/local/ajax/bitrix_migrator/stop_repair.php', { method: 'POST', body: fd });
        document.getElementById('repair-status-text').textContent = 'Остановка...';
    };

    function pollRepairStatus() {
        fetch('/local/ajax/bitrix_migrator/get_repair_status.php')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (!data.success) return;

                var statusEl = document.getElementById('repair-status-text');
                var progressBar = document.getElementById('repair-progress-bar');
                var progressText = document.getElementById('repair-progress-text');
                var logEl = document.getElementById('repair-log');

                statusEl.textContent = data.message || data.status;

                if (data.progress && data.progress.total > 0) {
                    var pct = Math.round((data.progress.current / data.progress.total) * 100);
                    progressBar.style.width = pct + '%';
                    progressText.textContent = data.progress.current + ' / ' + data.progress.total + ' (' + pct + '%)';
                }

                if (data.log && data.log.length > 0) {
                    logEl.textContent = data.log.join('\n');
                    logEl.scrollTop = logEl.scrollHeight;
                }

                if (data.status === 'completed' || data.status === 'error' || data.status === 'stopped') {
                    clearInterval(repairPollTimer);
                    repairPollTimer = null;
                    document.querySelectorAll('.btn-repair-start').forEach(function(b) { b.disabled = false; });
                    document.getElementById('btn-stop-repair').style.display = 'none';
                    if (data.status === 'completed') {
                        statusEl.textContent = 'Завершено';
                        progressBar.style.width = '100%';
                    }
                }
            })
            .catch(function() {});
    }

    // Accordion toggle for repair tab
    function initRepairTab() {
        document.querySelectorAll('.repair-accordion-header').forEach(function(header) {
            header.addEventListener('click', function() {
                var acc = header.parentElement;
                var arrow = header.querySelector('.migrator-accordion-arrow');
                acc.classList.toggle('open');
                arrow.textContent = acc.classList.contains('open') ? '\u25BC' : '\u25B6';
            });
        });
    }
    document.addEventListener('DOMContentLoaded', initRepairTab);

})();
