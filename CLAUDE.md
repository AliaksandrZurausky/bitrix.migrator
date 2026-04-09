# Bitrix Migrator — Модуль миграции из облака на коробку

## Описание
Переносит данные из облачного Bitrix24 в коробочную версию. Облако — через REST API, коробка — через D7 (минуя nginx-лимиты). Админ-панель с real-time прогрессом, pause/resume/stop, фоновый CLI worker.

## Окружение
- **PHP**: 8.2 (Docker: `quay.io/bitrix24/php:8.2.29-fpm-v1-alpine`)
- **БД**: MySQL в Docker `dev_mysql`, база `bitrix`, root creds — в `/home/bitrix/www/bitrix/.settings.php`
- **Контейнеры**: `dev_php`, `dev_mysql`, `dev_nginx` (порты 8588/8589)
- **Облако (тестовое)**: `https://regiuslab.bitrix24.by`
- **Коробка**: `https://dev.regius24.by`

## Карта модуля

```
bitrix_migrator/
├── lib/
│   ├── Integration/
│   │   └── CloudAPI.php          REST-обёртка для облака И коробки (rate-limit, cursor-пагинация)
│   └── Service/
│       ├── MigrationService.php  Оркестратор: 15 фаз, scope, респаун, cleanup (~4600 строк)
│       ├── TaskMigrationService.php  Задачи + комменты/чекликсты/чат-файлы
│       ├── BoxD7Service.php      D7-операции на коробке (users, CRM, disk, workgroups, IM chat)
│       ├── DryRunService.php     Предварительный анализ (подсчёты, конфликты)
│       └── MapService.php        HL-блок cloud↔box маппинга (`MigratorMap`)
├── install/
│   ├── admin/bitrix_migrator.php Админ-панель (5 вкладок + тестовый прогон)
│   ├── ajax/                     check_connection, start/stop/pause/resume, status
│   └── cli/migrate.php           Фоновый CLI worker (nohup + auto-respawn)
```

## 15 фаз миграции (const PHASES)
Диспатч: `'migrate' . ucwords($phase, '_')` → автовызов метода.

| # | Фаза | API |
|---|------|-----|
| 1 | departments | REST |
| 2 | users | D7 CUser |
| 3 | crm_fields | REST |
| 4 | pipelines | REST + D7 |
| 5 | currencies | REST |
| 6 | companies | D7 CCrmCompany |
| 7 | contacts | D7 CCrmContact |
| 8 | leads | D7 CCrmLead |
| 9 | deals | D7 CCrmDeal |
| 10 | invoices | `crm.item.list?entityTypeId=31` + D7 addSmartItem |
| 11 | requisites | D7 EntityRequisite/BankDetail |
| 12 | smart_processes | REST + D7 Factory |
| 13 | workgroups | D7 CSocNetGroup |
| 14 | timeline | D7 CCrmActivity + CommentEntry |
| 15 | tasks | REST + D7 (через TaskMigrationService) |

## Правила разработки
- Облако — только REST (чтение). Коробка — всё через D7 (создание/удаление).
- Вся разработка в `/local/`, не трогать `/bitrix/`.
- PSR-4 автозагрузка, namespace.
- Сперва обсуждаем, потом кодим.

## Ключевые технические решения

### Файлы в чате задачи (D7)
1. `ChatFactory::addUniqueChat(TYPE='X', ENTITY_TYPE='TASKS_TASK', ENTITY_ID=$taskId)`
2. `Driver::getStorageByUserId(1)->getFolderForUploadedFiles()->addFile(...)`
3. `CIMDisk::UploadFileFromDisk($chatId, ['disk'.$id], $text, [USER_ID=>N, SKIP_USER_CHECK=>true, SYMLINK=>true])`

### Таймлайн CRM
- Комментарии: `CommentEntry::create(['TEXT','AUTHOR_ID','BINDINGS'=>[['ENTITY_TYPE_ID'=>2,'ENTITY_ID'=>$id]]])`. Ключи `ENTITY_TYPE_ID`/`ENTITY_ID`, НЕ `OWNER_TYPE_ID`.
- Файлы в комментах: `'FILES'=>['n'.$diskId]` + `'SETTINGS'=>['HAS_FILES'=>'Y']` (без SETTINGS фронт не рендерит).
- Перед созданием: `FileUserType::setValueForAllowEdit('CRM_TIMELINE', true)`.
- Файлы качать через `disk.file.get.DOWNLOAD_URL` (с auth), НЕ `urlDownload` (вернёт HTML логина).
- Активности: `CCrmActivity::Add()`, даты в формате `DD.MM.YYYY HH:MI:SS`.
- Записи звонков: сначала `Add()` без файлов, потом `Update(['STORAGE_TYPE_ID'=>3,'STORAGE_ELEMENT_IDS'=>[$diskId]])`, `ORIGIN_ID='VI_imported_'.$cloudId.'.'.time()`.
- M:N связи: `ContactCompanyTable::bindCompanyIDs()`, `DealContactTable::bindContactIDs()`.
- SmartInvoice: `Container::getFactory(SmartInvoice)->createItem()->getAddOperation()->launch()`.

### Что НЕ работает для комментов задач
- `Forum\Comments\Feed::add()` — `canAdd()` не проходит из мигратора.
- `Feed::addComment()` — пропускает права, но не привязывает к задаче.
- REST `task.commentitem.add` — не поддерживает `UF_FORUM_MESSAGE_DOC` для файлов.
- **Рабочий путь**: файлы из комментов → `im.disk.file.commit` в чат задачи (admin должен быть в `AUDITORS`).

### Nginx-лимиты хостинга
Внешний прокси режет ~1-2MB → 413 для файлов через REST. **Решение**: D7-загрузка минует nginx.

### REST через внешний прокси НЕ работает для
- `crm.requisite.add` → "Entity not found"
- `crm.deal.contact.items.set` → "Not found"
- `crm.item.add` → "Неверное значение поля"
- Решение: всё через D7.

## Маппинг ID и кросс-ран миграции

**Два места хранения:**
1. **In-memory кэши** в `MigrationService`: `$companyMapCache`, `$contactMapCache`, `$dealMapCache`, `$leadMapCache`, `$pipelineMapCache`, `$stageMapCache`, `$smartProcessMapCache`, `$smartItemsByType`, `$invoiceMapCache`, `$userMapCache`, `$deptMapCache` — живут только в рамках одного PHP-процесса.
2. **HL-блок `MigratorMap`** (`MapService::addMap`/`loadMap`/`getAllMappings`): persistent `(entity_type, cloud_id, box_id)`. Пишется **всегда** через `saveToMap()` (не за флагом).

**Инварианты для работы через несколько запусков / респауна:**
- Каждая зависимая фаза в начале зовёт `loadExistingMappings($entityType)` для всех нужных типов. Для деалов — `deal/company/contact/lead`. Для timeline — `company/contact/deal/lead` (критично! без этого фаза молча проходит по 0 сущностей).
- Smart process caches (`smartProcessMapCache`, `smartItemsByType`) персистятся в `Option`, т.к. их нет в HL-блоке. Аналогично: `pipeline_map_cache`, `stage_map_cache`, `uf_field_schema`, `uf_enum_map`.
- Респаун (batched execution) = новый PHP-процесс. `migrate()` детектит его по `migration_phases` в Option и восстанавливает Option-кэши. CRM-сущностные кэши каждая фаза поднимает из HL сама.
- Облако должно быть доступно в каждом запуске (даже "пост-обработка" `linkDealCompanies`/`linkDealContacts` делает REST-вызовы).

**Частый симптом**: "сделки созданы, но `COMPANY_ID = 0`" или "timeline пустой" или "tasks не нашли CRM entity по `UF_CRM_TASK`".

**Диагностика**:
```sql
SELECT UF_ENTITY_TYPE, COUNT(*) FROM b_hlbd_migratormap GROUP BY UF_ENTITY_TYPE;
-- 0 для нужного типа → HL не записывался в предыдущем запуске, зависимая фаза не увидит связи
```

## Scoped (тестовый) прогон

Через UI-блок "Тестовый прогон" (вкладка Миграция) или `plan.scope = {entity_type, entity_ids, skip_prereqs}`:
- `entity_type=company|contact` — мигрирует выбранную сущность + всё что с ней прямо связано: контакты/леды/сделки/счета/smart-items/timeline/tasks. Глубина = 1 уровень (не тянет соседей через M:N).
- `entity_type=task` — пропускает все CRM-фазы, мигрирует только указанные задачи.
- `skip_prereqs` — пропускает `departments/users/crm_fields/pipelines/currencies/requisites`.
- Логика в `MigrationService::initScope()`, `isScopedMode()`, `inScope($entityKey, $cloudId)`. Реализация: server-side фильтр `filter[ID]=<whitelist>` + PHP-safety check.

## Batched execution / респаун

При миграции 2000+ CRM-сущностей процесс ест память (Bitrix eval'ит ORM-классы, не чистятся). Решение: фаза создаёт N сущностей → `requestBatchRespawn()` → `migrate.php` спавнит свежий процесс через `nohup`.
- `batch_size` — настройка плана (default 2000).
- Курсоры `phase_cursor_<phase>` в Option.
- Stats накопительные: новый воркер читает `migration_stats` и продолжает счёт.
- В каждой CRM-фазе в цикле: `if ($processed % 50 === 0) $self->freeMemory();` — `USER_FIELD_MANAGER->CleanCache` + `ManagedCache::cleanAll` + `gc_collect_cycles`. **НЕ вызывать `TaggedCache::clearByTag('*')`** — валит ServiceLocator и даёт фатал `Cannot declare class Mediator...`.

## Очистка (cleanup)

Все методы используют **D7 + ORM**, без raw SQL, без rate-limit пауз:
- CRM: `CompanyTable/ContactTable/DealTable/LeadTable::getList(['select'=>['ID']])` → `CCrmCompany::Delete` и т.д.
- Pipelines: `Bitrix\Crm\Category\Entity\DealCategoryTable::getList` → `\Bitrix\Crm\Category\DealCategory::delete` (каскадно удаляет стадии). Остатки default-стадий через `StatusTable::getList` → `CCrmStatus::Delete`.
- Tasks: `\Bitrix\Tasks\Internals\TaskTable::getList` → `CTaskItem::delete(['SKIP_NOT_FOUND_ERROR'=>true])`.
- Smart processes: `\Bitrix\Crm\Model\Dynamic\TypeTable::getList` → для каждого типа items через `Container::getFactory($etId)->getItems()` → `getDeleteOperation()->launch()`. **Системные типы** (CODE=`BX_SMART_*` — SmartInvoice=31, SmartDocument=36, SmartB2eDocument=39) чистятся только по items, **сам тип не удалять** (сломает портал).
- Порядок в `runAllCleanups`: `tasks → deals → contacts → companies → leads → pipelines → workgroups → smart_processes` (FK-safe).

## Карта связей CRM

```
Company(4) <--M:N--> Contact(3)        via b_crm_contact_company
     |                    |
     +---> Lead(1) <------+             COMPANY_ID, CONTACT_ID
           |
           +---> Deal(2)                COMPANY_ID, CONTACT_ID, LEAD_ID
                 |
                 +---> Quote(7)         dealId, companyId, contactId
                 +---> SmartInvoice(31) parentId2, companyId, contactId
                 +---> SmartProcess(>=1030) parentId<X>, companyId, contactId

Activity(6) --M:N--> ANY entity         via b_crm_act_bind
Timeline    --M:N--> ANY entity         via b_crm_timeline_bind
Requisite(8) --> Contact/Company        ENTITY_TYPE_ID=3/4
```

**Аббревиатуры** (CCrmOwnerTypeAbbr, для `UF_CRM_TASK`): `L`=Lead, `D`=Deal, `C`=Contact, `CO`=Company, `SI`=SmartInvoice, `Q`=Quote, `RQ`=Requisite. Dynamic types: `T` + hex(entityTypeId) (напр. `1038` = `T40e`).

## Что НЕ делать
- Не рефакторить код, не связанный с задачей.
- Не добавлять docblock к неизменённым функциям.
- Не менять рабочий код "для улучшения".
- Не вызывать `TaggedCache::clearByTag('*')` — валит ServiceLocator.
- Не удалять системные smart process types (CODE=`BX_SMART_*`).
- Не использовать `fetchAll('crm.company.list')` для очистки — есть быстрый `BoxD7Service::list<Entity>Ids()`.
