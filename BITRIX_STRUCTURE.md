# Понимание структуры Bitrix

## Как Битрикс ищет админ-страницы

```
Когда юзер входит в Admin
    │
    └─ /bitrix/components/bitrix/system.admin.interface/.default.php
            │
            └─ Просматривает /bitrix/admin/menu.php
                    │
                    └─ Пойманная ваше приложение
                            │
                            └─ "url" => "/bitrix/admin/bitrix_migrator.php?tab=settings"
                                    │
                                    └─ Когда кликнуть достанет и load выполнится
```

## Почему всегда `/bitrix/admin/`?

Это встроенные страницы Bitrix и страницы модулей.

Есть /bitrix/admin/*.php для:
- settings.php (настройки)
- tools.php (тулы)
- user_admin.php (пользователи)
- bitrix_migrator.php (НАШЭ)

## Что в `/bitrix/modules/`?

Это сами источники модулей.

Например:
- /bitrix/modules/bitrix_migrator/admin/bitrix_migrator.php – админ-пагина источник
- /bitrix/modules/bitrix_migrator/admin/js/migrator.js – JS источник

## Релация

```
Что делает install.php
    │
    └─ Копирует /bitrix/modules/bitrix_migrator/install/admin/bitrix_migrator.php
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━▶
            ↑                 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            │                       ↳
         Который include на /bitrix/admin/bitrix_migrator.php
            │
            └─ /bitrix/modules/bitrix_migrator/admin/bitrix_migrator.php
                    │ (главная логика)
                    └─ Asset::getInstance()->addJs('/bitrix/admin/js/bitrix_migrator.js')
                            │ (лоадит JS)
                            └─ На кторый install.php также копирует
                                    от /bitrix/modules/bitrix_migrator/admin/js/migrator.js
```

## Ок структура!

Если все файлы копировались (проверь точку 2 выше), то Bitrix будет:
1. Равинг меню
2. Клик Bitrix Migrator → load /bitrix/admin/bitrix_migrator.php
3. Тем выполнится и пагина отобразится
4. JS будет load валидно и всего сработает
