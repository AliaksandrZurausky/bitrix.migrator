# Admin Interface for Bitrix Migrator

## Installation

The admin interface files are automatically copied to `/bitrix/admin/` during module installation via `install.php`:

```php
private function copyAdminFiles(): void
{
    // Copies to /bitrix/admin/:
    // - bitrix_migrator.php (main page)
    // - menu.php (left menu registration)
    // - queue_stat.php (AJAX endpoint for stats)
    // - logs.php (AJAX endpoint for logs)
    // - js/bitrix_migrator.js (real-time updates)
}
```

## Files

- **bitrix_migrator.php** - Main admin page with 3 tabs (Settings, Queue, Logs)
- **menu.php** - Registers in left admin menu under "Settings"
- **queue_stat.php** - AJAX endpoint returning queue statistics
- **logs.php** - AJAX endpoint for log retrieval with filtering
- **../admin/js/migrator.js** - JavaScript for real-time updates

## Access

After installation:
- Go to: **Admin > Settings > Bitrix Migrator**
- Direct URL: `/bitrix/admin/bitrix_migrator.php?tab=settings`

## Features

### Tab 1: Settings (Подключение)
- Configure webhook URL
- Test connection to cloud portal
- Set batch size for migration processing
- Start/stop migration toggle

### Tab 2: Queue (Очередь)
- Add entities to migration queue
- View queue statistics (total, completed, pending, errors)
- Real-time updates every 5 seconds

### Tab 3: Logs (Логи)
- View migration logs with timestamps
- Filter by log level (ERROR, WARNING, INFO)
- Auto-updates as migration progresses

## Design

UI uses standard Bitrix admin classes:
- `CAdminTabControl` for tabs
- `adm-detail-content-table` for forms
- `adm-btn`, `adm-btn-green`, `adm-btn-red` for buttons
- `adm-info-message`, `adm-error-message` for alerts

Looks and feels like native Bitrix admin pages.
