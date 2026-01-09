# Полная установка Bitrix Migrator

## Шаг 1: Основная установка

Выбор А: через диск и Marketplace (uрекомендую)
```bash
cd /path/to/bitrix/local/modules/
git clone https://github.com/AliaksandrZurausky/bitrix.migrator.git bitrix_migrator
```

Тогда в Bitrix Admin: Marketplace → Find → Search "Bitrix Migrator" → Install

Выбор Б: директно (CLI)
```bash
cd /path/to/bitrix/local/modules/bitrix_migrator
php ../../../admin/settings.php?action=module_install&install=bitrix_migrator
```

## Шаг 2: Проверка

```bash
# Проверь наличие файлов
ls -la /path/to/bitrix/admin/bitrix_migrator.php
ls -la /path/to/bitrix/admin/menu.php
ls -la /path/to/bitrix/admin/queue_stat.php
ls -la /path/to/bitrix/admin/logs.php
ls -la /path/to/bitrix/admin/js/bitrix_migrator.js

# Норма, если квадратные скобки обозначают работу
```

## Шаг 3: Админка

1. Открыть: Admin → Settings → Bitrix Migrator
2. Должно быть три вкладки
3. Цифры в очереди должны обновляться каждые 5 секунд

## Нести

- Нет трем вкладок – помощь: клир кэша Bitrix
- Файлы не копировались – стр. INSTALL_FIX.md "Вариант 2"
- JS не загружается – стр. INSTALL_FIX.md "Вариант 3"
