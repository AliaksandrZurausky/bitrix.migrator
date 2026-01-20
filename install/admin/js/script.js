(function() {
    'use strict';

    let departmentsData = [];

    document.addEventListener('DOMContentLoaded', function() {
        initTabs();
        initConnectionHandlers();
        initDryRunHandlers();
        loadDryRunResults();
    });

    function initTabs() {
        const tabButtons = document.querySelectorAll('.migrator-tab-btn');
        const tabContents = document.querySelectorAll('.migrator-tab-content');

        tabButtons.forEach(button => {
            button.addEventListener('click', function() {
                const targetTab = this.getAttribute('data-tab');

                tabButtons.forEach(btn => btn.classList.remove('active'));
                tabContents.forEach(content => content.classList.remove('active'));

                this.classList.add('active');
                document.getElementById('tab-' + targetTab).classList.add('active');
            });
        });
    }

    function initConnectionHandlers() {
        document.getElementById('btn-save-connection')?.addEventListener('click', saveConnection);
        document.getElementById('btn-check-connection-cloud')?.addEventListener('click', () => checkConnection('cloud'));
        document.getElementById('btn-check-connection-box')?.addEventListener('click', () => checkConnection('box'));
        document.getElementById('btn-run-dryrun')?.addEventListener('click', runDryRun);
    }

    function initDryRunHandlers() {
        document.getElementById('btn-show-structure')?.addEventListener('click', showStructureSlider);
        
        // Close slider
        const closeBtn = document.querySelector('.migrator-slider-close');
        const overlay = document.querySelector('.migrator-slider-overlay');
        
        if (closeBtn) {
            closeBtn.addEventListener('click', closeStructureSlider);
        }
        if (overlay) {
            overlay.addEventListener('click', closeStructureSlider);
        }
    }

    function saveConnection() {
        const cloudUrl = document.getElementById('cloud_webhook_url').value.trim();
        const boxUrl = document.getElementById('box_webhook_url').value.trim();

        if (!cloudUrl && !boxUrl) {
            alert('Укажите хотя бы один вебхук (cloud или box)');
            return;
        }

        const formData = new FormData();
        formData.append('sessid', window.BITRIX_MIGRATOR.sessid);
        formData.append('cloud_webhook_url', cloudUrl);
        formData.append('box_webhook_url', boxUrl);

        fetch('/local/ajax/bitrix_migrator/save_connection.php', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert('Настройки сохранены');
                location.reload();
            } else {
                alert('Ошибка: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Ошибка сохранения');
        });
    }

    function checkConnection(type) {
        const statusText = document.getElementById('connection-status-text-' + type);
        const statusBlock = document.getElementById('connection-status-block-' + type).querySelector('.migrator-status');
        
        statusBlock.className = 'migrator-status migrator-status-checking';
        statusText.textContent = 'Проверка подключения...';

        const formData = new FormData();
        formData.append('sessid', window.BITRIX_MIGRATOR.sessid);
        formData.append('type', type);

        fetch('/local/ajax/bitrix_migrator/check_connection.php', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                statusBlock.className = 'migrator-status migrator-status-success';
                statusText.textContent = 'Подключение успешно установлено';
            } else {
                statusBlock.className = 'migrator-status migrator-status-error';
                statusText.textContent = 'Ошибка: ' + (data.error || 'Unknown error');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            statusBlock.className = 'migrator-status migrator-status-error';
            statusText.textContent = 'Ошибка проверки подключения';
        });
    }

    function runDryRun() {
        const btn = document.getElementById('btn-run-dryrun');
        btn.disabled = true;
        btn.textContent = 'Анализ выполняется...';

        const formData = new FormData();
        formData.append('sessid', window.BITRIX_MIGRATOR.sessid);

        fetch('/local/ajax/bitrix_migrator/start_dryrun.php', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert('Анализ завершён');
                loadDryRunResults();
                // Switch to Dry Run tab
                document.querySelector('[data-tab="dryrun"]').click();
            } else {
                alert('Ошибка: ' + (data.error || 'Unknown error'));
            }
            btn.disabled = false;
            btn.textContent = 'Запустить Dry Run';
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Ошибка запуска анализа');
            btn.disabled = false;
            btn.textContent = 'Запустить Dry Run';
        });
    }

    function loadDryRunResults() {
        fetch('/local/ajax/bitrix_migrator/get_dryrun_status.php')
            .then(response => response.json())
            .then(data => {
                if (data.success && data.data && data.data.departments) {
                    departmentsData = data.data.departments;
                    displayDryRunResults(departmentsData);
                } else {
                    document.getElementById('dryrun-summary').style.display = 'none';
                    document.getElementById('dryrun-no-results').style.display = 'block';
                }
            })
            .catch(error => {
                console.error('Error loading dry run results:', error);
            });
    }

    function displayDryRunResults(departments) {
        if (!departments || departments.length === 0) {
            document.getElementById('dryrun-summary').style.display = 'none';
            document.getElementById('dryrun-no-results').style.display = 'block';
            return;
        }

        document.getElementById('dryrun-summary').style.display = 'block';
        document.getElementById('dryrun-no-results').style.display = 'none';
        document.getElementById('departments-count').textContent = departments.length;

        // Build tree in the tab
        const treeContainer = document.getElementById('department-tree');
        treeContainer.innerHTML = '';
        buildDepartmentTree(departments, treeContainer);
        document.getElementById('department-tree-container').style.display = 'block';
    }

    function buildDepartmentTree(departments, container) {
        const depMap = {};
        const rootDeps = [];

        // Create map
        departments.forEach(dep => {
            depMap[dep.ID] = { ...dep, children: [] };
        });

        // Build hierarchy
        departments.forEach(dep => {
            if (dep.PARENT && depMap[dep.PARENT]) {
                depMap[dep.PARENT].children.push(depMap[dep.ID]);
            } else {
                rootDeps.push(depMap[dep.ID]);
            }
        });

        // Render tree
        const ul = document.createElement('ul');
        ul.className = 'migrator-tree';
        rootDeps.forEach(dep => {
            ul.appendChild(createTreeNode(dep));
        });
        container.appendChild(ul);
    }

    function createTreeNode(department) {
        const li = document.createElement('li');
        li.className = 'migrator-tree-item';

        const nodeContent = document.createElement('div');
        nodeContent.className = 'migrator-tree-node';

        if (department.children && department.children.length > 0) {
            const toggle = document.createElement('span');
            toggle.className = 'migrator-tree-toggle';
            toggle.textContent = '▶';
            toggle.addEventListener('click', function() {
                li.classList.toggle('expanded');
                toggle.textContent = li.classList.contains('expanded') ? '▼' : '▶';
            });
            nodeContent.appendChild(toggle);
        } else {
            const spacer = document.createElement('span');
            spacer.className = 'migrator-tree-spacer';
            nodeContent.appendChild(spacer);
        }

        const label = document.createElement('span');
        label.className = 'migrator-tree-label';
        label.textContent = `${department.NAME} (ID: ${department.ID})`;
        nodeContent.appendChild(label);

        li.appendChild(nodeContent);

        if (department.children && department.children.length > 0) {
            const childUl = document.createElement('ul');
            childUl.className = 'migrator-tree-children';
            department.children.forEach(child => {
                childUl.appendChild(createTreeNode(child));
            });
            li.appendChild(childUl);
        }

        return li;
    }

    function showStructureSlider() {
        const slider = document.getElementById('structure-slider');
        slider.classList.add('active');

        // Build tree in slider
        const sliderTree = document.getElementById('slider-department-tree');
        sliderTree.innerHTML = '';
        buildDepartmentTree(departmentsData, sliderTree);
    }

    function closeStructureSlider() {
        const slider = document.getElementById('structure-slider');
        slider.classList.remove('active');
    }
})();
