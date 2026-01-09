/**
 * Bitrix Migrator Admin UI - JavaScript
 */

(function() {
    'use strict';

    // Полытры нагружения
    const queueStatsUrl = '/bitrix/admin/queue_stat.php';
    const logsUrl = '/bitrix/admin/logs.php';

    // Основные дом-элементы
    const queueStatsElement = document.getElementById('queue-stats');
    const logsContainer = document.getElementById('logs-container');
    const logsTbody = document.getElementById('logs-tbody');

    /**
     * Загружаем статистику очереди
     */
    function loadQueueStats() {
        if (!queueStatsElement) return;

        fetch(queueStatsUrl)
            .then(response => response.json())
            .then(data => {
                if (data.success && data.data) {
                    updateQueueStats(data.data);
                }
            })
            .catch(error => console.error('Error loading queue stats:', error));
    }

    /**
     * Обновляем данные статистики
     */
    function updateQueueStats(data) {
        // Обновляем показатели
        updateStatValue('.stat-total', data.total);
        updateStatValue('.stat-completed', data.completed);
        updateStatValue('.stat-pending', data.pending);
        updateStatValue('.stat-errors', data.errors);
    }

    /**
     * Обновляем значение
     */
    function updateStatValue(selector, value) {
        const element = document.querySelector(selector);
        if (element) {
            const color = value > 0 ? (selector.includes('error') ? '#ff4444' : '#3fa43f') : '#999';
            element.textContent = value;
            element.style.color = color;
        }
    }

    /**
     * Загружаем логи
     */
    function loadLogs(limit = 50, offset = 0, level = null) {
        if (!logsContainer) return;

        const params = new URLSearchParams();
        params.append('limit', limit);
        params.append('offset', offset);
        if (level) params.append('level', level);

        fetch(logsUrl + '?' + params.toString())
            .then(response => response.json())
            .then(data => {
                if (data.success && data.data) {
                    renderLogs(data.data);
                }
            })
            .catch(error => console.error('Error loading logs:', error));
    }

    /**
     * Отображаем логи
     */
    function renderLogs(logs) {
        if (!logsTbody) return;

        if (logs.length === 0) {
            logsTbody.innerHTML = '<tr><td colspan="4" style="text-align: center; padding: 30px; color: #999;"><em>Нет логов</em></td></tr>';
            return;
        }

        logsTbody.innerHTML = logs.map(log => `
            <tr style="border-bottom: 1px solid #e0e0e0;">
                <td style="padding: 8px;">${escapeHtml(log.date)}</td>
                <td style="padding: 8px;">
                    <span class="log-level-${log.level.toLowerCase()}">${escapeHtml(log.level)}</span>
                </td>
                <td style="padding: 8px;">${escapeHtml(log.scope)}</td>
                <td style="padding: 8px;">${escapeHtml(log.message)}</td>
            </tr>
        `).join('');
    }

    /**
     * Экранируем HTML
     */
    function escapeHtml(text) {
        if (!text) return '';
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.toString().replace(/[&<>"']/g, m => map[m]);
    }

    /**
     * Обработчики на странице
     */
    function setupEventListeners() {
        // Фильтры логов
        const logFilters = document.querySelectorAll('.log-filter');
        logFilters.forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                const selectedLevels = Array.from(logFilters)
                    .filter(cb => cb.checked)
                    .map(cb => cb.value);
                loadLogs(50, 0, selectedLevels.length > 0 ? selectedLevels.join(',') : null);
            });
        });
    }

    /**
     * Нициализация при загружке страницы
     */
    document.addEventListener('DOMContentLoaded', function() {
        // Лоадим статистику исновные логи
        loadQueueStats();
        loadLogs();

        // Обновляем статистику каждые 5 секунд
        setInterval(loadQueueStats, 5000);

        // Настраиваем слушатели
        setupEventListeners();
    });
})();
