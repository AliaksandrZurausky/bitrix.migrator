# Полная инструкция по установке

## Проблема
"File not found" - админ-файл не оказался в `/bitrix/admin/`.

## Решение

### Вариант 1: Автоматическая установка через модули

1. Полностью очистить модуль (uдалить источник):
   ```bash
   rm -rf /path/to/bitrix/local/modules/bitrix_migrator/
   ```

2. Вселить снова с кнопки Install в Админ > Marketplace

3. Проверить что файлы на месте:
   ```bash
   ls -la /path/to/bitrix/bitrix/admin/bitrix_migrator.php
   ls -la /path/to/bitrix/bitrix/admin/js/bitrix_migrator.js
   ```

### Вариант 2: Ручная установка

```bash
# Копируем админ-файлы
cp /path/to/bitrix/local/modules/bitrix_migrator/install/admin/bitrix_migrator.php /path/to/bitrix/bitrix/admin/
cp /path/to/bitrix/local/modules/bitrix_migrator/install/admin/menu.php /path/to/bitrix/bitrix/admin/
cp /path/to/bitrix/local/modules/bitrix_migrator/install/admin/queue_stat.php /path/to/bitrix/bitrix/admin/
cp /path/to/bitrix/local/modules/bitrix_migrator/install/admin/logs.php /path/to/bitrix/bitrix/admin/

# Копируем JS
mkdir -p /path/to/bitrix/bitrix/admin/js/
cp /path/to/bitrix/local/modules/bitrix_migrator/admin/js/migrator.js /path/to/bitrix/bitrix/admin/js/bitrix_migrator.js
```

Открыть: `/bitrix/admin/bitrix_migrator.php`

### Вариант 3: PHP скрипт

```bash
php /path/to/bitrix/local/modules/bitrix_migrator/install_admin_file.php
```

## После установки

1. Открыть админ: Admin > Settings > Bitrix Migrator
2. Убедитесь что видны три вкладки: "Подключение", "Очередь", "Логи"
3. При клике статистика должна обновляться через 5 секунд

## Правомерная структура файлов

```
/bitrix/admin/
├── bitrix_migrator.php          ← entry point выставляет источник
├── menu.php                    ← регистрация меню
├── queue_stat.php              ← AJAX эндпоинт
├── logs.php                   ← AJAX эндпоинт
├── js/
│   └── bitrix_migrator.js       ← реальные скрипты
```

## Проверка работы

```bash
# Консоль
 curl http://example.com/bitrix/admin/queue_stat.php
 Ой видеть JSON со статистикой
```
