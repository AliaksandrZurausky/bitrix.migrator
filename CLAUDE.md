# Bitrix Migrator — Модуль миграции из облака на коробку

## Описание
Модуль `bitrix_migrator` переносит данные из облачного Bitrix24 на коробочную версию. Данные из облака получаются через REST API, на коробке создаются через D7 (минуя nginx и лимиты). Админ-панель с real-time прогрессом, pause/resume/stop, фоновый CLI worker.

## Окружение
- **PHP**: 8.2 (Docker: `quay.io/bitrix24/php:8.2.29-fpm-v1-alpine`)
- **Nginx**: Docker контейнер `dev_nginx` (порт 8588->80, 8589->443)
- **БД**: MySQL в Docker `dev_mysql`
- **Облако**: `https://regiuslab.bitrix24.by`
- **Коробка**: `https://dev.regius24.by`

## Карта модуля

```
bitrix_migrator/
├── lib/
│   ├── Integration/
│   │   └── CloudAPI.php          — REST-обёртка для облака И коробки (запросы, пагинация, rate-limit)
│   └── Service/
│       ├── MigrationService.php  — Главный оркестратор: 14 фаз, cleanup, кэши, прогресс (~2600 строк)
│       ├── TaskMigrationService.php — Миграция задач: комментарии, файлы, чеклисты, чат-сообщения
│       ├── BoxD7Service.php      — D7 операции на коробке (users, CRM, disk, workgroups, IM chat)
│       ├── DryRunService.php     — Предварительный анализ (подсчёт сущностей, конфликты)
│       ├── MapService.php        — Маппинг ID облако->коробка
│       ├── LogService.php        — Логирование
│       ├── StateService.php      — Состояние миграции
│       └── QueueService.php      — Очередь
├── install/
│   ├── admin/
│   │   └── bitrix_migrator.php   — Админ-панель (5 вкладок: подключение, анализ, план, миграция, логи)
│   ├── ajax/                     — AJAX endpoints (check_connection, start/stop/pause/resume, status)
│   └── cli/
│       └── migrate.php           — Фоновый CLI worker (nohup)
└── ajax/                         — Legacy AJAX endpoints
```

## 14 фаз миграции (const PHASES)

Диспатч: `'migrate' . ucwords($phase, '_')` -> автовызов метода.

| # | Фаза | Метод | API | Статус |
|---|------|-------|-----|--------|
| 1 | departments | migrateDepartments() | REST | проверено |
| 2 | users | migrateUsers() | D7 CUser | проверено |
| 3 | crm_fields | migrateCrmFields() | REST | реализовано |
| 4 | pipelines | migratePipelines() | REST | реализовано |
| 5 | currencies | migrateCurrencies() | REST | реализовано |
| 6 | companies | migrateCompanies() | D7 CCrmCompany | реализовано |
| 7 | contacts | migrateContacts() | D7 CCrmContact | реализовано |
| 8 | deals | migrateDeals() | D7 CCrmDeal | реализовано |
| 9 | leads | migrateLeads() | D7 CCrmLead | реализовано |
| 10 | requisites | migrateRequisites() | REST | реализовано |
| 11 | timeline | migrateTimeline() | D7 CCrmActivity + CommentEntry | проверено |
| 12 | workgroups | migrateWorkgroups() | D7 CSocNetGroup | проверено |
| 13 | smart_processes | migrateSmartProcesses() | REST | реализовано |
| 14 | tasks | migrateTasks() | REST + D7 | проверено |

## Stable — do NOT modify

- **Подразделения** — `MigrationService::migrateDepartments()`
- **Пользователи** — `BoxD7Service::createUser()` + `MigrationService::migrateUsers()`
- **Рабочие группы** — `BoxD7Service::createWorkgroup/addWorkgroupMember/getAllWorkgroups/deleteWorkgroup` + `MigrationService::migrateWorkgroups()`
- **Задачи** — `TaskMigrationService`:
  - Комментарии: REST `task.commentitem.add` с `AUTHOR_ID`
  - Файлы в комментариях: REST `im.disk.file.commit` (admin как AUDITOR)
  - Чат-сообщения с файлами: D7 `ChatFactory::addUniqueChat()` + `CIMDisk::UploadFileFromDisk(USER_ID, SKIP_USER_CHECK, SYMLINK)`
  - Загрузка файлов: D7 через личное хранилище пользователя — минует nginx
  - Чеклисты, base64 картинки, связи CRM

## Технические решения и ограничения

### Файлы в чатах задач (D7 путь)
1. Создать чат: `ChatFactory::addUniqueChat(TYPE='X', ENTITY_TYPE='TASKS_TASK', ENTITY_ID=$taskId)`
2. Загруз��ть файл: `Driver::getStorageByUserId(1)->getFolderForUploadedFiles()->addFile()`
3. Отправить: `CIMDisk::UploadFileFromDisk($chatId, ['disk'.$id], $text, [USER_ID=>1, SKIP_USER_CHECK=>true, SYMLINK=>true])`

### Таймлайн CRM (D7) — проверено
- Комментарии: `CommentEntry::create(['TEXT'=>..., 'AUTHOR_ID'=>..., 'BINDINGS'=>[['ENTITY_TYPE_ID'=>2, 'ENTITY_ID'=>$id]]])`
- ВАЖНО: ключи `ENTITY_TYPE_ID`/`ENTITY_ID`, НЕ `OWNER_TYPE_ID`/`OWNER_ID` — иначе bindings не создаются
- Файлы в комментариях: `'FILES' => ['n'.$diskId]` + `'SETTINGS' => ['HAS_FILES' => 'Y']` — без SETTINGS фронтенд не рендерит файлы
- Перед созданием: `FileUserType::setValueForAllowEdit('CRM_TIMELINE', true)`
- Файлы комментариев скачивать через `disk.file.get` → `DOWNLOAD_URL` (с auth), НЕ через `urlDownload` из комментария (без auth → HTML)
- Активности: `CCrmActivity::Add()` с датами в формате `DD.MM.YYYY HH:MI:SS`
- Записи звонков: сначала `Add()` без файлов, потом `Update($id, ['STORAGE_TYPE_ID'=>3, 'STORAGE_ELEMENT_IDS'=>[$diskId]])`
- ВАЖНО: для плеера записей нужен `ORIGIN_ID = 'VI_imported_' . $cloudActId . '.' . time()`
- Документы (crm.documentgenerator.document.list): скачать PDF, создать как комментарий с файлом
- Реквизиты: D7 `EntityRequisite::add()` + `EntityBankDetail::add()`
- M:N связи: D7 `ContactCompanyTable::bindCompanyIDs()`, `DealContactTable::bindContactIDs()`
- Smart Invoice: D7 `Container::getFactory(SmartInvoice)->createItem()->getAddOperation()->launch()`

### Что НЕ работает для комментариев задач
- D7 `Forum\Comments\Feed::add()` — проверяет `canAdd()`, не проходит из мигратора
- D7 `Feed::addComment()` — пропускает права, но не привязывает к задаче
- D7 `CTaskCommentItem::add` — требует `global $USER` + форум-права
- REST `task.commentitem.add` — не поддерживает `UF_FORUM_MESSAGE_DOC` для файлов

### Nginx лимиты
- Внешний прокси хостинга: ~1-2MB по умолчанию, 413 для больших файлов через REST
- Решение: D7 загрузка файлов минует nginx

## Правила разработки

- Облако — только REST API (получение данных)
- Коробка — всё через D7 (создание данных, файлов, чатов)
- Всю разработку вести в `/local/`
- PSR-4 автозагрузка, namespace
- Сперва обсуждаем, потом кодим
- Коммиты по комплексу работ

## Рекомендуемый порядок миграции (обновлённый)

### Фаза 0: Предварительные данные
0.1 Users / Departments — ASSIGNED_BY_ID, CREATED_BY_ID
0.2 Currencies — CURRENCY_ID
0.3 Statuses / Sources — STATUS_ID, SOURCE_ID, STAGE_ID
0.4 Deal Categories (Pipelines) — CATEGORY_ID
0.5 Smart Process Types — crm.type.add (перед элементами)
0.6 Custom Fields (UF) — перед данными
0.7 Products / Catalog — товарные позиции

### Фаза 1: Базовые CRM сущности
1.1 Company (LEAD_ID=0, backfill позже)
1.2 Contact (COMPANY_ID заполнен, LEAD_ID=0)
1.3 Lead (COMPANY_ID, CONTACT_ID заполнены)
1.4 Backfill: Company.LEAD_ID, Contact.LEAD_ID

### Фаза 2: Зависимые CRM сущности
2.1 Deal (COMPANY_ID, CONTACT_ID, LEAD_ID, CATEGORY_ID)
2.2 Quote/Предложение (entityTypeId=7: dealId, companyId, contactId)
2.3 Requisites + BankDetails (ENTITY_TYPE_ID=3/4)

### Фаза 3: Smart Processes
3.1 SmartInvoice (31) — companyId, contactId, parentId2(deal)
3.2 Custom SPs (>=1030) — с backfill circular dependencies
3.3 Product Rows (товарные позиции: Deal, Quote, Invoice, SP)

### Фаза 4: Кросс-сущностные данные
4.1 Workgroups
4.2 Tasks (UF_CRM_TASK: "CO_20", "D_123") + чат-файлы
4.3 Activities (M:N привязки к любым сущностям) + записи звонков
4.4 Timeline Comments (TYPE_ID=7, CommentEntry::create)

### Фаза 5: Backfill
5.1 Circular parentId references в SmartProcesses
5.2 Contact<->Company M:N (crm.contact.company.items)
5.3 Deal<->Contact M:N (crm.deal.contact.items)

## Карта связей CRM сущностей

```
Company(4) <--M:N--> Contact(3)       via b_crm_contact_company
     |                    |
     +---> Lead(1) <------+            COMPANY_ID, CONTACT_ID
           |
           +---> Deal(2)               COMPANY_ID, CONTACT_ID, LEAD_ID
                 |
                 +---> Quote(7)        dealId, companyId, contactId
                 +---> SmartInvoice(31) parentId2, companyId, contactId
                 +---> SmartProcess(>=1030) parentId<X>, companyId, contactId
                 
Activity(6) --M:N--> ANY entity        via b_crm_act_bind
ProductRow  --> Deal/Quote/Invoice/SP   via b_crm_product_row (OWNER_TYPE=abbr)
Requisite(8) --> Contact/Company        ENTITY_TYPE_ID=3/4
Timeline    --M:N--> ANY entity         via b_crm_timeline_bind
```

## Типы записей таймлайна

Мигрировать: TYPE_ID=7 (COMMENT) через CommentEntry::create, TYPE_ID=25 subtype 5 (LOG_MESSAGE REST)
НЕ мигрировать (авто): 1(ACTIVITY), 2(CREATION), 3(MODIFICATION), 6(MARK), 9(BIZPROC), 10(CONVERSION), 12(DOCUMENT), 13(RESTORATION), 25(TODO/PING/EMAIL)

## Ключевые аббревиатуры (CCrmOwnerTypeAbbr)
L=Lead, D=Deal, C=Contact, CO=Company, I=Invoice, Q=Quote, SI=SmartInvoice, O=Order, RQ=Requisite
Dynamic types: T + hex(entityTypeId), например 1038 = T40e

## Реализовано: Файлы в чате задач от реального автора

- `migrateChatFiles()` собирает всех участников задачи (createdBy, responsibleId, accomplices, auditors) и передаёт в `createTaskChat()` как USERS
- `sendFileToChatD7()` вызывается с замапленным USER_ID автора сообщения (`$msg['senderId']`), fallback на admin (1)
- Все участники добавлены в чат до отправки файлов

## Реализовано: Сохранение оригинальных дат

- `BoxD7Service::backdateEntity($table, $id, $dateCreate, $dateModify)` — прямой SQL UPDATE после создания (CCrm*::Add игнорирует DATE_CREATE)
- Таблицы и колонки: `b_crm_company/contact/deal/lead` (DATE_CREATE, DATE_MODIFY), `b_crm_act` (CREATED, LAST_UPDATED), `b_crm_timeline` (CREATED), `b_crm_dynamic_items_*` (CREATED_TIME, UPDATED_TIME)
- Вызывается после каждого создания: компании, контакты, сделки, лиды, активности, комментарии таймлайна, smart items
- Каждый вызов обёрнут в try/catch — ошибка даты не ломает миграцию

## Реализовано: Инкрементальная (дельта) миграция

- `MigrationService::getIncrementalFilter()` — возвращает `['>DATE_CREATE' => $lastTs]` если включён режим incremental
- `getIncrementalTaskFilter()` — то же для задач (`>CREATED_DATE`)
- `loadExistingMappings($entityType)` — загружает маппинги из HL-блока в кэш (для timeline/activities ранее созданных сущностей)
- `MapService::getAllMappings($entityType)` — новый метод, возвращает cloudId->localId из HL-блока
- `CloudAPI`: методы `getCompanies/Contacts/Deals/Leads/Tasks/SmartProcessItems` принимают `$filter = []`
- CLI worker: `$migrateType === 'incremental'` -> `$plan['settings']['incremental'] = true`
- Timestamp сохраняется в `Option::set('last_migration_timestamp', date('c'))` при завершении
- Admin UI: кнопка "Инкрементальная миграция" (disabled если нет предыдущего timestamp)

## Последние изменения (сессия 2026-04-05, обновлено)

### Реализованы все TODO (сессия 2026-04-05):
- **TODO 1**: Файлы в чатах задач от реального автора (TaskMigrationService::migrateChatFiles)
- **TODO 2**: Сохранение оригинальных дат (BoxD7Service::backdateEntity + SQL UPDATE после создания)
- **TODO 3**: Инкрементальная миграция (CloudAPI фильтры, MapService::getAllMappings, admin UI кнопка)

### Внедрено в основной мигратор:
- **migrateTimeline()**: активности через D7 `CCrmActivity::Add()` вместо REST, даты в формате DD.MM.YYYY
- **migrateTimeline()**: записи звонков через двухфазный подход (Add → Update STORAGE_ELEMENT_IDS)
- **migrateTimeline()**: комментарии через D7 `CommentEntry::create` с FILES + SETTINGS['HAS_FILES']
- **migrateTimeline()**: `FileUserType::setValueForAllowEdit('CRM_TIMELINE', true)` в начале фазы
- **BoxD7Service**: добавлены `bindContactCompany()`, `bindDealContacts()`, `addRequisite()`, `addBankDetail()`, `addSmartItem()`
- **TaskMigrationService**: `migrateChatFiles()` — перенос файлов из чата задач через D7 `CIMDisk::UploadFileFromDisk`
- **CloudAPI**: `getChatMessages()` для получения сообщений чата, исправлен `getTimelineComments()` (ENTITY_TYPE строкой)

### Тестовые скрипты (в `/home/bitrix/www/local/Claude code/Мигратор/`):
- `test_full_company20.php` — полная выгрузка компании #20 со всеми связями (эталонный тест)
- `test_contact_1442_v2.php` — контакт с лидом, сделкой, звонками, записями, таймлайном
- `test_task_3762.php` — задача с файлами в чате через D7
- `test_comment_file.php` — тест прикрепления файлов к комментариям таймлайна
- Запуск тестов через Docker: `curl -k -s "https://127.0.0.1:8589/local/Claude%20code/Мигратор/<script>.php"`

### Проверено на реальных данных (компания #20):
- ✅ Компания + контакт + 2 сделки через D7
- ✅ Реквизиты через D7 EntityRequisite
- ✅ Счёт через D7 Factory SmartInvoice
- ✅ 10 активностей через D7 CCrmActivity
- ✅ Комментарий таймлайна с файлом (SETTINGS['HAS_FILES'] + disk.file.get auth URL)
- ✅ 15 документов как комментарии с PDF
- ✅ M:N связи через D7 ContactCompanyTable, DealContactTable

### Проверено (контакт #1442):
- ✅ 3 звонка VOXIMPLANT_CALL с MP3 записями (ORIGIN_ID='VI_imported_...')
- ✅ 9 комментариев таймлайна с анализом ИИ (ENTITY_TYPE_ID/ENTITY_ID bindings)

### REST через внешний прокси НЕ работает для:
- Реквизиты (crm.requisite.add) — "Entity not found" 
- M:N bindings (crm.deal.contact.items.set) — "Not found"
- Счета (crm.item.add) — "Неверное значение поля"
- Причина: внешний прокси хостинга или DNS, D7-сущности не видны из REST в другом процессе
- Решение: ВСЁ через D7

## Что НЕ нужно делать

- ��е рефакторить код, не связанный с задачей
- Не добавлять docblock к неизменённым функциям
- Не менять рабочий код "для улучшения"
- Не использовать устаревший API без необходимости
