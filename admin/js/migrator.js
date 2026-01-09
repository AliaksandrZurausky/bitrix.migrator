// Bitrix Migrator Admin JavaScript
// Загружает статистику и логи в реал-тайме

(function() {
    'use strict';
    
    function loadStats() {
        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/bitrix/admin/bitrix_migrator_queue_stat.php', true);
        
        xhr.onload = function() {
            if (xhr.status === 200) {
                try {
                    var response = JSON.parse(xhr.responseText);
                    if (response.success) {
                        document.querySelector('.stat-total').textContent = response.data.total || 0;
                        document.querySelector('.stat-completed').textContent = response.data.completed || 0;
                        document.querySelector('.stat-pending').textContent = response.data.pending || 0;
                        document.querySelector('.stat-errors').textContent = response.data.errors || 0;
                    }
                } catch (e) {
                    console.error('Failed to parse stats response:', e);
                }
            }
        };
        
        xhr.send();
    }
    
    function loadLogs() {
        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/bitrix/admin/bitrix_migrator_logs.php', true);
        
        xhr.onload = function() {
            if (xhr.status === 200) {
                try {
                    var response = JSON.parse(xhr.responseText);
                    if (response.success) {
                        var tbody = document.querySelector('#logs-tbody');
                        if (tbody) {
                            tbody.innerHTML = '';
                            
                            if (response.data.length === 0) {
                                tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; padding: 20px; color: #999;">Логи отсутствуют</td></tr>';
                                return;
                            }
                            
                            response.data.forEach(function(log) {
                                var row = document.createElement('tr');
                                var levelClass = 'log-level-' + (log.level || 'info').toLowerCase();
                                
                                row.innerHTML = '<td style="padding: 8px;">' + (log.date || '-') + '</td>' +
                                                '<td style="padding: 8px;" class="' + levelClass + '">' + (log.level || '-') + '</td>' +
                                                '<td style="padding: 8px;">' + (log.scope || '-') + '</td>' +
                                                '<td style="padding: 8px;">' + (log.message || '-') + '</td>';
                                
                                tbody.appendChild(row);
                            });
                        }
                    }
                } catch (e) {
                    console.error('Failed to parse logs response:', e);
                }
            }
        };
        
        xhr.send();
    }
    
    // Initial load
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            loadStats();
            loadLogs();
        });
    } else {
        loadStats();
        loadLogs();
    }
    
    // Update every 5 seconds
    setInterval(loadStats, 5000);
    setInterval(loadLogs, 5000);
})();
