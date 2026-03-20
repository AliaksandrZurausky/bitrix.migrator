(function() {
    'use strict';

    document.addEventListener('DOMContentLoaded', function() {
        initTabs();
        initConnectionHandlers();
        initDryRunHandlers();
        loadDryRunResults();
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

        if (!cloudUrl && !boxUrl) {
            alert('Укажите хотя бы один вебхук (cloud или box)');
            return;
        }

        var formData = new FormData();
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
        var statusText  = document.getElementById('connection-status-text-' + type);
        var statusBlock = document.getElementById('connection-status-block-' + type).querySelector('.migrator-status');
        var webhookUrl  = document.getElementById(type + '_webhook_url').value.trim();

        if (!webhookUrl) {
            statusBlock.className  = 'migrator-status migrator-status-error';
            statusText.textContent = 'Укажите URL вебхука';
            return;
        }

        statusBlock.className  = 'migrator-status migrator-status-checking';
        statusText.textContent = 'Проверка подключения...';

        var formData = new FormData();
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

    // =========================================================================
    // Dry Run
    // =========================================================================
    function initDryRunHandlers() {}

    function runDryRun() {
        var btn = document.getElementById('btn-run-dryrun');
        btn.disabled    = true;
        btn.textContent = 'Анализ выполняется...';

        var formData = new FormData();
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

    // =========================================================================
    // Display Results — Accordion-based UI
    // =========================================================================
    function displayDryRunResults(data) {
        var summary = document.getElementById('dryrun-summary');
        summary.style.display   = 'block';
        document.getElementById('dryrun-no-results').style.display = 'none';
        summary.innerHTML = '';

        var title = el('h3', {}, 'Результаты анализа портала');
        summary.appendChild(title);

        var depts = data.departments || {};
        var users = data.users       || {};
        var crm   = data.crm         || {};
        var deptNameMap = users.dept_name_map || {};

        // 1. Подразделения
        summary.appendChild(makeAccordion(
            'Подразделения',
            val(depts.count),
            function(body) {
                if (depts.list && depts.list.length > 0) {
                    buildDepartmentTree(depts.list, body);
                } else {
                    body.appendChild(el('p', { style: 'color:#999' }, 'Нет данных'));
                }
            }
        ));

        // 2. Пользователи
        var usersBadges = [];
        usersBadges.push(badge('В облаке', val(users.cloud_active_count), '#0080C8'));
        if (users.box_active_count !== undefined) {
            usersBadges.push(badge('В коробке', val(users.box_active_count), '#17a2b8'));
        }
        if (users.matched_count !== undefined) {
            usersBadges.push(badge('Совпали', val(users.matched_count), '#28a745'));
        }
        if (users.new_count !== undefined) {
            usersBadges.push(badge('Новых', val(users.new_count), '#e67e22'));
        }

        summary.appendChild(makeAccordion(
            'Пользователи',
            usersBadges,
            function(body) {
                var statsRow = el('div', { className: 'migrator-stats' });
                statsRow.appendChild(statCard(val(users.cloud_count), 'Всего в облаке'));
                statsRow.appendChild(statCard(val(users.cloud_active_count), 'Активных в облаке'));
                if (users.box_count !== undefined) {
                    statsRow.appendChild(statCard(val(users.box_count), 'Всего в коробке'));
                    statsRow.appendChild(statCard(val(users.box_active_count), 'Активных в коробке'));
                }
                if (users.matched_count !== undefined) {
                    statsRow.appendChild(statCard(val(users.matched_count), 'Совпали по email', 'success'));
                }
                if (users.new_count !== undefined) {
                    statsRow.appendChild(statCard(val(users.new_count), 'Новых к миграции', 'warning'));
                }
                body.appendChild(statsRow);

                if (users.new_list && users.new_list.length > 0) {
                    body.appendChild(el('h4', { style: 'margin-top:16px' }, 'Новые пользователи (будут созданы):'));
                    buildNewUsersList(users.new_list, deptNameMap, body);
                }
            }
        ));

        // 3. CRM
        var crmBadges = [];
        var companiesLabel = val(crm.companies_regular !== undefined ? crm.companies_regular : crm.companies);
        if (crm.companies_my > 0) companiesLabel += ' (+' + crm.companies_my + ' мои)';
        crmBadges.push(badge('Компании', companiesLabel, '#6c757d'));
        crmBadges.push(badge('Контакты', val(crm.contacts), '#6c757d'));
        crmBadges.push(badge('Сделки', val(crm.deals), '#6c757d'));
        crmBadges.push(badge('Лиды', val(crm.leads), '#6c757d'));

        summary.appendChild(makeAccordion(
            'CRM',
            crmBadges,
            function(body) {
                var statsRow = el('div', { className: 'migrator-stats' });
                var compVal = crm.companies_regular !== undefined ? crm.companies_regular : crm.companies;
                statsRow.appendChild(statCard(val(compVal), crm.companies_my > 0 ? 'Компаний (+ ' + crm.companies_my + ' мои)' : 'Компаний'));
                statsRow.appendChild(statCard(val(crm.contacts), 'Контактов'));
                statsRow.appendChild(statCard(val(crm.deals), 'Сделок'));
                statsRow.appendChild(statCard(val(crm.leads), 'Лидов'));
                body.appendChild(statsRow);

                // Custom fields
                if (data.crm_custom_fields && !data.crm_custom_fields.error) {
                    var labels = { deal: 'Сделки', contact: 'Контакты', company: 'Компании', lead: 'Лиды' };
                    Object.keys(data.crm_custom_fields).forEach(function(entity) {
                        var fields = data.crm_custom_fields[entity];
                        if (!fields || Object.keys(fields).length === 0) return;

                        body.appendChild(makeSubAccordion(
                            'Пользовательские поля: ' + (labels[entity] || entity),
                            Object.keys(fields).length + ' полей',
                            function(subBody) {
                                var table = el('table', { className: 'migrator-table' });
                                table.innerHTML = '<thead><tr><th>Код</th><th>Название</th><th>Тип</th></tr></thead>';
                                var tbody = document.createElement('tbody');
                                Object.keys(fields).forEach(function(code) {
                                    var f = fields[code];
                                    var row = document.createElement('tr');
                                    row.innerHTML = '<td>' + esc(code) + '</td><td>' + esc(f.title || '') + '</td><td>' + esc(f.type || '') + '</td>';
                                    tbody.appendChild(row);
                                });
                                table.appendChild(tbody);
                                subBody.appendChild(table);
                            }
                        ));
                    });
                }
            }
        ));

        // 4. Воронки сделок
        var pipelines = (data.pipelines || {}).list || [];
        summary.appendChild(makeAccordion(
            'Воронки сделок',
            val((data.pipelines || {}).count) + ' воронок',
            function(body) {
                pipelines.forEach(function(pipeline) {
                    body.appendChild(makeSubAccordion(
                        pipeline.name + ' (' + (pipeline.stages_count || 0) + ' стадий)',
                        null,
                        function(subBody) {
                            if (pipeline.stages && pipeline.stages.length > 0) {
                                var stagesEl = el('div', { className: 'migrator-pipeline-stages' });
                                pipeline.stages.forEach(function(stage) {
                                    var chip = el('span', { className: 'migrator-stage-chip' }, stage.NAME || stage.name || stage.ID || '');
                                    if (stage.COLOR || stage.color) {
                                        chip.style.borderLeftColor = '#' + (stage.COLOR || stage.color).replace('#', '');
                                        chip.style.borderLeftWidth = '3px';
                                    }
                                    stagesEl.appendChild(chip);
                                });
                                subBody.appendChild(stagesEl);
                            } else {
                                subBody.appendChild(el('p', { style: 'color:#999' }, 'Нет стадий'));
                            }
                        }
                    ));
                });
            }
        ));

        // 5. Задачи
        summary.appendChild(makeAccordion(
            'Задачи',
            val((data.tasks || {}).count),
            null
        ));

        // 6. Рабочие группы
        summary.appendChild(makeAccordion(
            'Рабочие группы',
            val((data.workgroups || {}).count),
            null
        ));

        // 7. Смарт-процессы
        var smartProcs = (data.smart_processes || {}).list || [];
        summary.appendChild(makeAccordion(
            'Смарт-процессы',
            val((data.smart_processes || {}).count),
            function(body) {
                if (smartProcs.length === 0) {
                    body.appendChild(el('p', { style: 'color:#999' }, 'Нет смарт-процессов'));
                    return;
                }
                smartProcs.forEach(function(sp) {
                    var cfCount = sp.custom_fields ? Object.keys(sp.custom_fields).length : 0;
                    var spLabel = sp.title + ' (entityTypeId: ' + sp.entityTypeId + ', записей: ' + (sp.count || 0) + ')';

                    body.appendChild(makeSubAccordion(
                        spLabel,
                        cfCount > 0 ? cfCount + ' польз. полей' : null,
                        function(subBody) {
                            if (cfCount > 0) {
                                var table = el('table', { className: 'migrator-table' });
                                table.innerHTML = '<thead><tr><th>Код</th><th>Название</th><th>Тип</th></tr></thead>';
                                var tbody = document.createElement('tbody');
                                Object.keys(sp.custom_fields).forEach(function(code) {
                                    var f = sp.custom_fields[code];
                                    var row = document.createElement('tr');
                                    row.innerHTML = '<td>' + esc(code) + '</td><td>' + esc(f.title || '') + '</td><td>' + esc(f.type || '') + '</td>';
                                    tbody.appendChild(row);
                                });
                                table.appendChild(tbody);
                                subBody.appendChild(table);
                            } else {
                                subBody.appendChild(el('p', { style: 'color:#999' }, 'Нет пользовательских полей'));
                            }
                        }
                    ));
                });
            }
        ));
    }

    // =========================================================================
    // Accordion builders
    // =========================================================================
    function makeAccordion(title, badgeContent, bodyBuilder) {
        var section = el('div', { className: 'migrator-accordion' });

        var header = el('div', { className: 'migrator-accordion-header' });
        var arrow  = el('span', { className: 'migrator-accordion-arrow' }, '\u25B6');
        var titleEl = el('span', { className: 'migrator-accordion-title' }, title);
        header.appendChild(arrow);
        header.appendChild(titleEl);

        // Badges
        if (badgeContent) {
            var badgeWrap = el('span', { className: 'migrator-accordion-badges' });
            if (Array.isArray(badgeContent)) {
                badgeContent.forEach(function(b) { badgeWrap.appendChild(b); });
            } else {
                badgeWrap.appendChild(badge('', badgeContent, '#0080C8'));
            }
            header.appendChild(badgeWrap);
        }

        section.appendChild(header);

        if (bodyBuilder) {
            var body = el('div', { className: 'migrator-accordion-body' });
            bodyBuilder(body);
            section.appendChild(body);

            header.addEventListener('click', function() {
                var isOpen = section.classList.contains('open');
                section.classList.toggle('open');
                arrow.textContent = isOpen ? '\u25B6' : '\u25BC';
            });
            header.style.cursor = 'pointer';
        }

        return section;
    }

    function makeSubAccordion(title, badgeText, bodyBuilder) {
        var section = el('div', { className: 'migrator-sub-accordion' });

        var header = el('div', { className: 'migrator-sub-accordion-header' });
        var arrow  = el('span', { className: 'migrator-accordion-arrow' }, '\u25B6');
        var titleEl = el('span', {}, title);
        header.appendChild(arrow);
        header.appendChild(titleEl);

        if (badgeText) {
            header.appendChild(badge('', badgeText, '#6c757d'));
        }

        section.appendChild(header);

        var body = el('div', { className: 'migrator-sub-accordion-body' });
        if (bodyBuilder) bodyBuilder(body);
        section.appendChild(body);

        header.addEventListener('click', function() {
            var isOpen = section.classList.contains('open');
            section.classList.toggle('open');
            arrow.textContent = isOpen ? '\u25B6' : '\u25BC';
        });
        header.style.cursor = 'pointer';

        return section;
    }

    // =========================================================================
    // Department Tree
    // =========================================================================
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

        var ul = el('ul', { className: 'migrator-tree' });
        rootDeps.forEach(function(dep) { ul.appendChild(createTreeNode(dep, true)); });
        container.appendChild(ul);
    }

    function createTreeNode(department, expandRoot) {
        var li = el('li', { className: 'migrator-tree-item' + (expandRoot ? ' expanded' : '') });
        var nodeContent = el('div', { className: 'migrator-tree-node' });

        if (department.children && department.children.length > 0) {
            var toggle = el('span', { className: 'migrator-tree-toggle' }, expandRoot ? '\u25BC' : '\u25B6');
            toggle.addEventListener('click', function() {
                li.classList.toggle('expanded');
                toggle.textContent = li.classList.contains('expanded') ? '\u25BC' : '\u25B6';
            });
            nodeContent.appendChild(toggle);
        } else {
            nodeContent.appendChild(el('span', { className: 'migrator-tree-spacer' }));
        }

        var label = el('span', { className: 'migrator-tree-label' }, department.NAME);
        var idSpan = el('span', { className: 'migrator-tree-id' }, ' #' + department.ID);
        label.appendChild(idSpan);
        nodeContent.appendChild(label);

        li.appendChild(nodeContent);

        if (department.children && department.children.length > 0) {
            var childUl = el('ul', { className: 'migrator-tree-children' });
            department.children.forEach(function(child) { childUl.appendChild(createTreeNode(child, false)); });
            li.appendChild(childUl);
        }

        return li;
    }

    // =========================================================================
    // New Users List
    // =========================================================================
    function buildNewUsersList(users, deptNameMap, container) {
        var table = el('table', { className: 'migrator-table' });
        table.innerHTML = '<thead><tr><th>ФИО</th><th>Email</th><th>Должность</th><th>Отдел</th></tr></thead>';

        var tbody = document.createElement('tbody');
        users.forEach(function(u) {
            var row  = document.createElement('tr');
            var name = [u.LAST_NAME, u.NAME, u.SECOND_NAME].filter(Boolean).join(' ');

            // Resolve department names
            var deptIds = u.UF_DEPARTMENT || u.DEPARTMENT || [];
            if (!Array.isArray(deptIds)) deptIds = [deptIds];
            var deptNames = deptIds.map(function(id) {
                return deptNameMap[String(id)] || ('ID:' + id);
            }).join(', ');

            row.innerHTML =
                '<td>' + esc(name)                    + '</td>' +
                '<td>' + esc(u.EMAIL || '')            + '</td>' +
                '<td>' + esc(u.WORK_POSITION || '')    + '</td>' +
                '<td>' + esc(deptNames)                + '</td>';
            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        container.appendChild(table);
    }

    // =========================================================================
    // Helpers
    // =========================================================================
    function el(tag, attrs, text) {
        var node = document.createElement(tag);
        if (attrs) {
            Object.keys(attrs).forEach(function(k) {
                if (k === 'className') node.className = attrs[k];
                else if (k === 'style') node.style.cssText = attrs[k];
                else node.setAttribute(k, attrs[k]);
            });
        }
        if (text) node.textContent = text;
        return node;
    }

    function badge(label, value, color) {
        var b = el('span', { className: 'migrator-badge' });
        b.style.borderColor = color || '#0080C8';
        b.style.color = color || '#0080C8';
        if (label) b.textContent = label + ': ' + value;
        else b.textContent = value;
        return b;
    }

    function statCard(value, label, variant) {
        var card = el('div', { className: 'migrator-stat-card' + (variant ? ' migrator-stat-card--' + variant : '') });
        card.appendChild(el('div', { className: 'migrator-stat-value' }, String(value)));
        card.appendChild(el('div', { className: 'migrator-stat-label' }, label));
        return card;
    }

    function val(v) {
        return v !== undefined && v !== null ? String(v) : '—';
    }

    function esc(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
})();
