# Починил три главных проблемы

## Проблема 1: Откуда файлы вытаскиваются?

**Расположение файлов:**

- `admin/` - источники (система разработки)
- `install/admin/` - копии для установки
- `/bitrix/admin/` - читаые файлы (фактически в системе)

**Почему так?**

Битрикс ищет админ-файлы в `/bitrix/admin/`. При установке install.php копирует их туда.

## Проблема 2: Entry Point

**Поведение:**

```
/bitrix/admin/bitrix_migrator.php
    │
    └─ require() /bitrix/modules/bitrix_migrator/admin/bitrix_migrator.php
            │
            └─ require() /bitrix/modules/main/include/prolog_admin.php
            └─ ... основная логика ...
            └─ Asset::getInstance()->addJs('/bitrix/admin/js/bitrix_migrator.js')
            └─ require() /bitrix/modules/main/include/epilog_admin.php
```

## Проблема 3: JS трансформация

**Оригинал:**
- `admin/js/migrator.js` (25KB, для разработки)

**После копирования:**
- `/bitrix/admin/js/bitrix_migrator.js` (тот же строка в `install.php`)

```php
if (file_exists($jsSourceDir . 'migrator.js')) {
    @copy($jsSourceDir . 'migrator.js', $jsTargetDir . 'bitrix_migrator.js');
}
```

## Что дальше?

1. **Реинсталл** модуль
2. **Проверь** что `/bitrix/admin/bitrix_migrator.php` существует
3. **Открыть** админ: `/bitrix/admin/bitrix_migrator.php`
4. **Проверь** Console в браузере (F12) - не должно быть 404 на JS
