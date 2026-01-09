# Bitrix Migrator (bitrix_migrator)

Модуль для миграции данных Bitrix24 «облако → коробка» с переносом максимально возможной части CRM-таймлайна.

## Возможности

- Перенос CRM-сущностей: сделки, контакты, компании, лиды
- Перенос таймлайна: комментарии, активности (звонки/письма/задачи)
- Очередь задач с ретраями и логированием
- Маппинг ID облако → коробка
- Автоматическое выполнение через агенты Bitrix

## Архитектура

### Слои

```
┌─────────────────────────────────────┐
│   Admin UI (будет добавлено)        │
└─────────────────────────────────────┘
           │
┌─────────────────────────────────────┐
│   Application Layer              │
│   - QueueBuilder                  │
│   - TaskRunner                    │
└─────────────────────────────────────┘
           │
┌─────────────────────────────────────┐
│   Service/Import                  │
│   - DealImporter                  │
│   - ContactImporter               │
│   - TimelineImporter              │
└─────────────────────────────────────┘
           │
   ┌───────────────────────────────────┐
   │   Infrastructure             │
   │   - Cloud REST Client        │
   │   - Box Writers (CRM/Timeline)│
   │   - HL Repositories          │
   └───────────────────────────────────┘
```

### Ключевые компоненты

**Integration/Cloud:**
- `RestClient` — REST-клиент для облачного портала
- `Api/*` — обёртки над REST-методами (Deal, Contact, Timeline...)

**Writer/Box:**
- `CrmWriter` — создание CRM-сущностей в коробке через D7
- `TimelineWriter` — создание записей таймлайна (комменты/активности)

**Repository/Hl:**
- `QueueRepository` — управление очередью задач
- `MapRepository` — соответствия cloud_id → box_id
- `LogRepository` — логирование

**Service:**
- `QueueBuilder` — формирование очереди задач
- `TaskRunner` — выполнение пачек задач из очереди
- `Import/*` — импортеры для каждого типа сущности

**Agent:**
- `MigratorAgent` — агент Bitrix, который запускает TaskRunner

## HL-блоки

При установке модуля создаются 3 Highload-блока:

### 1. BitrixMigratorQueue (очередь задач)

| Поле | Тип | Описание |
|------|------|------------|
| UF_TYPE | string | Тип задачи (ENTITY_CREATE, TIMELINE_IMPORT) |
| UF_ENTITY_TYPE | string | Тип сущности (DEAL, CONTACT, COMPANY, LEAD) |
| UF_CLOUD_ID | string | ID сущности в облаке |
| UF_BOX_ID | int | ID сущности в коробке |
| UF_STEP | string | Шаг выполнения (CREATE, IMPORT, etc.) |
| UF_STATUS | string | Статус (NEW, RUNNING, DONE, ERROR, RETRY) |
| UF_RETRY | int | Количество попыток |
| UF_NEXT_RUN_AT | datetime | Следующее время запуска |
| UF_PAYLOAD | text | Доп. данные (JSON) |
| UF_ERROR | text | Текст ошибки |

### 2. BitrixMigratorMap (маппинг ID)

| Поле | Тип | Описание |
|------|------|------------|
| UF_ENTITY | string | Тип сущности |
| UF_CLOUD_ID | string | ID в облаке |
| UF_BOX_ID | int | ID в коробке |
| UF_HASH | string | Хэш для идемпотентности |
| UF_META | text | Метаданные (JSON) |

### 3. BitrixMigratorLog (журнал)

| Поле | Тип | Описание |
|------|------|------------|
| UF_LEVEL | string | Уровень (INFO, WARNING, ERROR) |
| UF_SCOPE | string | Область логирования |
| UF_MESSAGE | text | Сообщение |
| UF_CONTEXT | text | Контекст (JSON) |
| UF_ENTITY | string | Тип сущности |
| UF_CLOUD_ID | string | ID в облаке |
| UF_BOX_ID | int | ID в коробке |

## Установка

### 1. Разместить модуль

```bash
cd /path/to/bitrix/local/modules/
git clone https://github.com/AliaksandrZurausky/bitrix.migrator.git bitrix_migrator
```

### 2. Установить модуль

Перейти в: **Администрирование → Marketplace → Установленные решения → Bitrix Migrator**

Нажать «Установить».

Модуль:
- Создаст 3 HL-блока (или использует существующие)
- Зарегистрирует агента `MigratorAgent`

### 3. Настройка

Добавить в настройки модуля (`.settings.php` или через `Option::set`):

```php
use Bitrix\Main\Config\Option;
use BitrixMigrator\Config\Module;

// Webhook облачного портала
Option::set(Module::ID, 'CLOUD_WEBHOOK_URL', 'https://your-cloud.bitrix24.ru/rest/1/your_webhook_key');

// Включение миграции
Option::set(Module::ID, 'MIGRATION_ENABLED', 'Y');

// Размер пачки (по умолчанию 50)
Option::set(Module::ID, 'BATCH_SIZE', 50);
```

### 4. Перевод агентов на cron (рекомендуется)

Добавить в crontab:

```bash
* * * * * /usr/bin/php /path/to/bitrix/modules/main/tools/cron_events.php
```

В `.settings.php` установить:

```php
'agents' => [
    'value' => [
        'use_crontab' => true,
    ],
],
```

## Использование

### Формирование очереди

```php
use Bitrix\Main\Loader;
use BitrixMigrator\Repository\Hl\QueueRepository;
use BitrixMigrator\Integration\Cloud\RestClient;
use BitrixMigrator\Service\QueueBuilder;

Loader::includeModule('bitrix_migrator');

$queueRepo = new QueueRepository();
$cloudClient = new RestClient();
$queueBuilder = new QueueBuilder($queueRepo, $cloudClient);

// Добавить все сделки в очередь
$count = $queueBuilder->buildForDeals();
echo "Added {$count} deals to queue";

// Или с фильтром
$count = $queueBuilder->buildForDeals(['STAGE_ID' => 'WON']);
```

### Запуск миграции

Миграция запускается автоматически агентом. Для включения/остановки:

```php
use Bitrix\Main\Config\Option;
use BitrixMigrator\Config\Module;

// Запустить
Option::set(Module::ID, 'MIGRATION_ENABLED', 'Y');

// Остановить
Option::set(Module::ID, 'MIGRATION_ENABLED', 'N');
```

### Ручной запуск пачки

```php
use BitrixMigrator\Service\TaskRunner;
use BitrixMigrator\Repository\Hl\QueueRepository;
use BitrixMigrator\Repository\Hl\LogRepository;
// ... и другие зависимости

$taskRunner = new TaskRunner($queueRepo, $logRepo, $importers);
$processed = $taskRunner->runBatch(50);
echo "Processed {$processed} tasks";
```

## Рабочий флоу

1. **Формирование очереди**: `QueueBuilder` читает список сущностей из облака и добавляет задачи в `BitrixMigratorQueue`.

2. **Выполнение**: Агент `MigratorAgent::run()` запускается каждую минуту (или через cron).

3. **Обработка пачки**: `TaskRunner` берёт N задач со статусом `NEW/RETRY`.

4. **Импорт**: Для каждой задачи:
   - Импортер читает данные из облака (`CloudRestClient`).
   - Создаёт сущность в коробке (`CrmWriter`, `TimelineWriter`).
   - Сохраняет маппинг в `BitrixMigratorMap`.
   - Добавляет дочерние задачи (например, перенос таймлайна).

5. **Логирование**: Все операции логируются в `BitrixMigratorLog`.

6. **Ретраи**: При ошибке задача помечается как `RETRY` (до 3 попыток).

## Удаление модуля

При удалении модуль спрашивает:
- Сохранить данные (HL-блоки и настройки)?

Если галочка снята — все HL-блоки и настройки удаляются.

## TODO / Дальнейшее развитие

- [ ] Admin UI (страница с вкладками: настройки, очередь, логи)
- [ ] Перенос файлов и записей звонков (`FileWriter`)
- [ ] Поддержка OAuth (альтернатива webhook)
- [ ] Улучшенная обработка связей (контакты↔компании, сделки↔контакты)
- [ ] Перенос пользователей (с маппингом по email)
- [ ] Поддержка других типов таймлайна (лог-сообщения, документы)

## Лицензия

MIT

## Автор

Aliaksandr Zurausky
