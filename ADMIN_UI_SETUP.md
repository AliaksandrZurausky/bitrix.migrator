# Admin UI Setup Instructions

## Problem
The error "File not found" when accessing `/bitrix/admin/bitrix_migrator.php` means the admin file hasn't been copied to the correct location.

## Solution

The module installation process automatically handles this via `install.php`:

### Step 1: Install Module
1. Go to **Admin > Marketplace > Extensions**
2. Find "Bitrix Migrator" 
3. Click **Install**

### Step 2: Verify Installation
During installation, `install.php` will:
1. Register the module with Bitrix
2. Copy admin files to `/bitrix/admin/`:
   - `bitrix_migrator.php` (main page)
   - `menu.php` (menu registration)
   - `queue_stat.php` (AJAX stats)
   - `logs.php` (AJAX logs)
3. Copy JS files to `/bitrix/admin/js/`
4. Register the admin menu item
5. Register migration agent

### Step 3: Access Admin Interface
After installation:
1. Go to **Admin > Settings > Bitrix Migrator**
2. Or visit: `/bitrix/admin/bitrix_migrator.php`

## File Locations

```
bitrix_migrator/
├── admin/                          # Development source files
│   ├── bitrix_migrator.php        # Main page (copied to /bitrix/admin/)
│   ├── menu.php                   # Menu (copied to /bitrix/admin/)
│   ├── queue_stat.php             # Stats AJAX (copied to /bitrix/admin/)
│   ├── logs.php                   # Logs AJAX (copied to /bitrix/admin/)
│   └── js/
│       └── migrator.js            # JS logic (copied to /bitrix/admin/js/)
├── admin_install/                 # Installation copies from here
│   ├── bitrix_migrator.php        # Proxy to actual file
│   ├── menu.php                   # Menu registration
│   ├── queue_stat.php             # Stats endpoint
│   └── logs.php                   # Logs endpoint
└── install.php                    # Handles copying during installation
```

## Troubleshooting

### "File not found" error
**Cause:** Admin files not copied to `/bitrix/admin/`

**Fix:**
1. Re-install the module
2. Or manually copy files:
   ```bash
   cp admin/bitrix_migrator.php /path/to/bitrix/admin/
   cp admin/menu.php /path/to/bitrix/admin/
   cp admin/queue_stat.php /path/to/bitrix/admin/
   cp admin/logs.php /path/to/bitrix/admin/
   cp -r admin/js /path/to/bitrix/admin/
   ```

### Menu doesn't appear
**Cause:** menu.php not registered properly

**Fix:**
1. Clear browser cache
2. Go to **Admin > System > Cache**
3. Click **Clear cache**

### AJAX requests failing
**Cause:** JavaScript file not loaded

**Fix:**
1. Verify `/bitrix/admin/js/bitrix_migrator.js` exists
2. Check browser console for 404 errors
3. Clear cache and reload page

## Next Steps

1. **Configure webhook:**
   - Go to Settings tab
   - Enter cloud portal webhook URL
   - Click "Test connection"

2. **Start migration:**
   - Go to Queue tab
   - Select entity type
   - Click "Add to queue"
   - Go to Settings tab
   - Click "Start"

3. **Monitor progress:**
   - Watch Queue tab for statistics
   - Check Logs tab for details
   - Adjust batch size if needed

## Support

For issues, check the module documentation at:
https://github.com/AliaksandrZurausky/bitrix.migrator
