# Архитектура job_aggregator

Статус: черновик v1 для согласования. Кода нет. Все числа (интервалы, лимиты, веса) — стартовые значения, а не измеренные. Их надо настраивать по факту.

Если этот документ расходится с `spec.md`, приоритет такой: `AGENTS.md` (после правок из `decisions.md`) → этот документ → `spec.md`.

---

## 1. Противоречия и дыры в ТЗ

| # | Проблема | Решение |
|---|----------|---------|
| 1 | HARD FILTERS стоят до STRUCTURAL EXTRACTION, но «офис при remote only», «командировки», «звонки» зависят от extraction | Extraction выполняется **до** фильтров, один раз на вакансию (не на пользователя), результат кешируется в БД с версией. Text- и structural-hard-reject правила вычисляются вместе, все причины собираются (не short-circuit), hard reject побеждает. |
| 2 | PERSIST в конце pipeline, но ТЗ требует сохранять всё (в т.ч. дубликаты, мусор) | Наблюдение (`vacancy_sources`) пишется сразу после validate, до dedup. Падение воркера не теряет данные. |
| 3 | В п.7 просят не смешивать статусы, но в примере SENT/BLOCKED/APPROVED в одном enum; в п.31 единый `status` | Три независимые оси (см. §5). |
| 4 | «Мне в личный Telegram», но схема с `users`/`filter_rules`, а `relevance_score` лежит в `vacancy` | Один пользователь сейчас, схема multi-user-ready. `relevance_score` живёт в `vacancy_evaluations (vacancy, user)`. `risk_score` — свойство вакансии. |
| 5 | «Redis только если нужен», но `REDIS_URL` в `.env` | Redis не используется до Phase 4, из `.env.example` убирается. Очереди на PostgreSQL. |
| 6 | Негации не описаны, а «без звонков» = positive, «звонки» = hard reject | Алгоритм в §8. |
| 7 | Формула score не задана | Формула и веса в конфиге, §7. |
| 8 | Similarity-dedup на сотнях тысяч записей без метода | SimHash + индекс по «корзинам» (bands) для отбора кандидатов, `pg_trgm` не для dedup, §4. |
| 9 | `/search` без описания реализации | FTS (`russian`) + `pg_trgm` как fallback для опечаток, §4. |
| 10 | Реальные критерии пользователя есть только как примеры | Seed `config/default_rules.yaml`, я предлагаю черновик, ты правишь (вопрос 4 в §11). |
| 11 | Telethon = юзербот на реальном аккаунте: лимиты, FloodWait, риск бана | Отдельный аккаунт, единый rate limiter на аккаунт, stagger + jitter, §2. |
| 12 | «Step 1–9, потом сразу реализуй Phase 1» в один заход | Сначала документы, потом Phase 0 (вертикальный срез), потом Phase 1 кусками. |
| 13 | В `spec.md` pipeline: DEDUP до сохранения, но ТЗ §17 требует хранить связь вакансия ↔ несколько источников | `vacancy_sources.vacancy_id` nullable: сначала наблюдение, потом dedup привязывает его к каноничной вакансии. `ingest_status` переезжает с `vacancies` на `vacancy_sources`. |
| 14 | §32 предлагает `processing_results` + `processing_flags`, но §6 хочет то же самое | Объединено: `vacancy_evaluations.reasons` (jsonb) + `vacancies.risk_flags`. |
| 15 | §29 `/search` «сортировать по relevance»: неясно, это ts_rank или `relevance_score` | Фильтр — совпадение FTS; сортировка — `relevance_score` пользователя, затем `published_at`; trgm-similarity только при пустой выдаче FTS. |
| 16 | §43: кнопки «Подходит/Не подходит» без описания эффекта | В MVP пишем в `feedback`, ни на что не влияет (влияние → Phase 5). «Скрыть источник» — `user_source_mutes`, действует сразу. |
| 17 | §14: `FIXED` и `PROJECT` пересекаются | `FIXED` — единая сумма за всю работу; `PROJECT` — оплата за проект/заказ с диапазоном или ставкой. Не сравниваются между собой. |
| 18 | §20 и §21: как взаимодействуют rate limit, digest и quiet hours | Порядок: quiet hours gate → rate limit → digest-схлопывание (§6). |
| 19 | Бот открыт всем, кто нашёл его в Telegram | Allowlist по `OWNER_TG_ID`; остальные сообщения игнорируются. |
| 20 | Одно сообщение канала может содержать несколько вакансий (подборки) | MVP: 1 сообщение = 1 вакансия. Подборки помечаются флагом `MULTI_VACANCY_POST` и не отправляются автоматически. |

---

## 2. Компоненты и границы

```
┌────────────────────── один процесс (MVP) ──────────────────────┐
│  Bot (aiogram polling)      Scheduler (APScheduler, 1 tick/30s) │
│  Worker pool (N корутин)    Notifier (1 корутина)               │
│  Telethon client (1 аккаунт, общий rate limiter)                │
└─────────────────────────────────┬───────────────────────────────┘
                                  │ SQLAlchemy async
                            PostgreSQL (данные + очереди)
```

**Bot.** Команды, настройки, управление источниками, показ уведомлений. В БД только читает и меняет настройки/источники. Не парсит, не шлёт вакансии сам, кроме ответов на команды. Игнорирует всех, кроме `OWNER_TG_ID`.

**Scheduler.** Раз в 30 секунд: `SELECT source_instances WHERE status IN (ACTIVE, DEGRADED) AND next_run_at <= now()` и вставка jobs. Плюс тики для digest и выхода из quiet hours. Почему один tick, а не 1000 cron-задач APScheduler: расписание хранится в БД (`next_run_at`), переживает рестарт, легко менять на лету, нет 1000 объектов в памяти.

**Worker.** Забирает job из БД, вызывает adapter, гонит pipeline до `PERSIST EVALUATION` и постановки уведомления. Bounded concurrency: N воркеров (по умолчанию 4) + семафор на HTTP + общий limiter на Telethon.

**Notifier.** Единственный, кто шлёт вакансии. Забирает `notifications`, соблюдает лимиты, quiet hours, FloodWait, retries.

**PostgreSQL.** Единственное хранилище и единственная очередь.

**Telethon.** Отдельная сессия отдельного (не основного) аккаунта. Читает каналы **поллингом** (`get_messages(min_id=last_processed_message_id)`), а не подпиской на updates: при 1000 каналов updates могут теряться, и всё равно нужен catch-up. Публичные каналы, насколько я знаю, читаются без вступления, но `ResolveUsername` жёстко лимитируется, поэтому после первого resolve сохраняем `channel_id` + `access_hash`. Приватные каналы требуют вступления аккаунта; это надо проверить вручную (вопрос 2). Единый limiter на аккаунт: стартово ≤ 1 запрос/сек с jitter, конкурентность 2; цикл по 1000 каналов ≈ 17–35 минут. Интервал опроса канала адаптивный (5–60 мин по активности). Всё настраивается.

**Границы импортов:** `bot` не импортирует `workers`/`pipeline`; `scheduler` не импортирует `pipeline`; `pipeline` не импортирует `bot`; `notifier` знает про `bot` только через тонкий интерфейс `MessageSender`. Общие модели/репозитории — в `database`/`models`.

**Redis: не используем.** Разбор по назначениям: queue → `SKIP LOCKED` в PG (тысячи задач/час — далеко до предела); lock → advisory lock/`SKIP LOCKED`; cache → нужные данные (правила, настройки) читаются раз в минуту в память процесса; temporary state → колонки в БД; rate limiting → in-process limiter + подсчёт отправленных в БД (переживает рестарт). Критерий пересмотра: >1 процесса-воркера с общим rate limiter или замеренная нагрузка на PG-очередь.

**Из MVP в разделение процессов позже:** каждый компонент — отдельная точка входа (`python -m app.bot`, `app.scheduler`, `app.worker`, `app.notifier`); в MVP один `app.main` запускает их как корутины. Разделение = запуск тех же модулей в разных контейнерах. Ограничение: Telethon-сессию использует ровно один процесс.

---

## 3. Структура проекта

```
job_aggregator/
├── AGENTS.md
├── spec.md
├── docs/                         # architecture.md, decisions.md, scoring.md, sources/
├── config/
│   ├── default_rules.yaml        # seed правил (hard/soft/positive/risk)
│   ├── scoring.yaml              # веса, группы, капы, пороги risk_level
│   └── synonyms.yaml             # удалёнка/remote, ИИ/AI и т.п.
├── app/
│   ├── main.py                   # запуск всех компонентов как корутин (MVP)
│   ├── config/                   # Pydantic Settings из .env
│   ├── database/                 # engine, session, репозитории, queue.py (SKIP LOCKED)
│   ├── models/                   # SQLAlchemy модели + enums + таблица переходов статусов
│   ├── sources/                  # base.py, registry.py, telegram.py, hh.py (stub) …
│   ├── pipeline/                 # runner.py + шаги: parse, validate, persist_raw, dedup, analyze, evaluate, notify_decision
│   ├── normalizer/               # текст: NFKC, HTML, ё→е, гомоглифы, морфология, синонимы
│   ├── salary_parser/            # суммы, валюты, периоды, типы оплаты
│   ├── extraction/               # remote/график/опыт/звонки/командировки/навыки
│   ├── filters/                  # keyword engine + негации + hard/soft/positive
│   ├── scoring/                  # relevance по формуле из scoring.yaml
│   ├── risk_engine/              # risk_score, risk_level, risk_flags
│   ├── notifier/                 # очередь, rate limit, quiet hours, digest, форматирование
│   ├── bot/                      # handlers/, keyboards/, middlewares/ (allowlist), states/
│   ├── scheduler/                # tick, создание jobs
│   ├── workers/                  # цикл claim job → pipeline, sweeper зависших
│   └── utils/                    # limiter, backoff, время, логирование (маскирование секретов)
├── migrations/
├── tests/
│   ├── unit/
│   ├── integration/              # реальный PostgreSQL
│   └── fixtures/                 # тексты вакансий, ответы Telegram (моки)
├── scripts/                      # phase0_demo.py, load_channels.py
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
└── README.md
```

Отличие от структуры в ТЗ: `salary`/`filters`/`scoring` не размазаны по `pipeline/` и верхнему уровню, добавлен `extraction/`, шаги pipeline — тонкие обёртки над доменными модулями (их можно тестировать без БД).

---

## 4. Схема БД

Типы: `bigint` PK (identity), `timestamptz` везде, JSON — `jsonb`. Enum-ы — как `text` + CHECK (проще миграции, чем PG enum).

### Пользователи и настройки

**users**: `id`, `tg_user_id` UNIQUE, `created_at`.

**user_settings** (PK `user_id`): `timezone`, `quiet_hours_start/end`, `quiet_policy` (QUEUE_AND_SEND_LATER | SKIP | DIGEST_AFTER_QUIET), `min_relevance_score` (def 70), `max_risk_level` (def LOW), `max_age_hours` (def 72), `remote_only`, `min_salary_monthly` + `min_salary_currency`, `max_notifications_per_minute` (def 3), `max_notifications_per_hour` (def 20), `digest_threshold` (def 8), `updated_at`.

**filter_rules**: `id`, `user_id`, `category` (HARD_REJECT | SOFT_PENALTY | POSITIVE | RISK), `code` (машинный код причины), `match_type` (PHRASE | LEMMA | REGEX | STRUCTURED), `pattern` / `condition` (jsonb для STRUCTURED, напр. `{"remote": "OFFICE"}` при remote_only), `weight` (nullable, для soft/positive), `negation_mode` (IGNORE | FLIP_TO: `<code>`), `enabled`, `updated_at`. INDEX `(user_id, category, enabled)`. Seed грузится из YAML при первом запуске (upsert по `(user_id, code)`; пользовательские правки не затираются).
Категория нельзя менять через UPDATE напрямую в HARD_REJECT ↔ SOFT: смена категории = удалить и создать (защита от «случайно понизил hard до soft»); в боте такая операция требует подтверждения.

**user_source_mutes**: `(user_id, source_instance_id)` PK.
**feedback**: `(user_id, vacancy_id)` PK, `value` (LIKE | DISLIKE), `created_at`.

### Источники

**source_instances**: `id`, `adapter_type` (telegram | hh | …), `ref` (username / URL нормализованный), `title`, `external_channel_id`, `access_hash`, `status` (ACTIVE | PAUSED | DEGRADED | UNSUPPORTED | AUTH_REQUIRED | RATE_LIMITED | ERROR), `status_reason`, `config` jsonb (freshness override, interval), `last_processed_message_id`, `sync_state` jsonb (INITIAL_SYNC: NOT_STARTED | RUNNING | DONE; BACKFILL — то же), `consecutive_errors`, `next_run_at`, `last_check_at`, `last_success_at`, `created_at`. UNIQUE `(adapter_type, ref)`. INDEX `(status, next_run_at)`.

**jobs**: `id`, `source_instance_id`, `mode` (LIVE_MONITORING | INITIAL_SYNC | BACKFILL), `status` (PENDING | RUNNING | DONE | FAILED), `priority`, `run_after`, `locked_until`, `attempts`, `max_attempts`, `checkpoint` jsonb, `error`, `created_at`, `finished_at`. Partial UNIQUE `(source_instance_id, mode) WHERE status IN ('PENDING','RUNNING')` — scheduler не может создать второй активный job. INDEX `(status, priority, run_after)`.

**source_runs**: `id`, `job_id`, `source_instance_id`, `started_at`, `finished_at`, `fetched`, `parsed`, `invalid`, `duplicates`, `hard_rejected`, `approved`, `error_class`, `error_message`. INDEX `(source_instance_id, started_at DESC)`.

### Вакансии

**vacancy_sources** (наблюдения; одна строка = «объявление увидели в источнике»):
`id`, `source_instance_id`, `external_id`, `source_url`, `vacancy_id` (nullable → FK), `mode`, `ingest_status` (PENDING | INVALID | CANONICAL | DUPLICATE), `invalid_reason`, `raw_payload` jsonb, `raw_text`, `normalized_text`, `published_at`, `collected_at`, `job_id`.
UNIQUE `(source_instance_id, external_id)`. INDEX `(vacancy_id)`, `(ingest_status) WHERE ingest_status='PENDING'`, `(source_instance_id, collected_at)`.

**vacancies** (каноничные объявления):
`id`, `title`, `description`, `company`, `contact`, `source_url` (первичный), `published_at` (самое раннее), `collected_at`,
`raw_text`, `normalized_text`, `content_hash`, `normalized_hash`, `simhash` (bigint), `simhash_b0..b3` (smallint, 4 полосы по 16 бит),
`salary_min`, `salary_max`, `salary_currency`, `payment_type`, `salary_period`, `salary_raw`, `salary_status`,
`employment_type`, `schedule`, `remote`, `location`, `skills` jsonb, `tags` jsonb, `extraction` jsonb, `extraction_version`,
`risk_score`, `risk_level`, `risk_flags` jsonb, `analyzed_at`, `analysis_version`,
`search_tsv` tsvector (GENERATED из title+description, конфигурация `russian`).
UNIQUE `(content_hash)` — гарантия, что два воркера не создадут две каноничные вакансии с идентичным текстом; INDEX `(normalized_hash)`, `(simhash_b0)`, `(simhash_b1)`, `(simhash_b2)`, `(simhash_b3)`, `(published_at DESC)`, `(collected_at DESC)`, `(risk_level, risk_score)`, `(analyzed_at) WHERE analyzed_at IS NULL`, GIN `(search_tsv)`, GIN trgm `(title gin_trgm_ops)`.

Salary-поля и `remote/schedule/...` — денормализованный результат extraction для фильтрации/поиска; `extraction` jsonb хранит полный результат со спанами.

### Оценки и уведомления

**vacancy_evaluations**: `vacancy_id`, `user_id` (PK вместе), `decision` (PENDING | HARD_REJECTED | FILTERED | APPROVED), `relevance_score` (nullable — при hard reject не считается), `reasons` jsonb `{positive:[], negative:[], hard_reject:[], filtered:[], risk_flags:[]}`, `notify_eligible` bool, `rules_version`, `evaluated_at`. INDEX `(user_id, decision, relevance_score DESC)`, `(user_id, evaluated_at DESC)`.

**notifications**: `id`, `user_id`, `vacancy_id`, `delivery` (SINGLE | DIGEST), `status` (QUEUED | SENDING | SENT | UNCONFIRMED | FAILED | SKIPPED), `priority`, `not_before`, `attempts`, `next_attempt_at`, `locked_until`, `tg_message_id`, `last_error`, `created_at`, `sent_at`. UNIQUE `(user_id, vacancy_id)` — **одна вакансия = одно уведомление на пользователя навсегда**. INDEX `(status, next_attempt_at)`.

**notification_attempts**: `id`, `notification_id`, `started_at`, `finished_at`, `outcome` (STARTED | OK | RETRYABLE_ERROR | FATAL_ERROR), `error_code`, `retry_after_s`.

### Как это отвечает на гонки

| Сценарий гонки | Защита |
|---|---|
| Два воркера тащат одно сообщение канала | UNIQUE `(source_instance_id, external_id)` + `INSERT … ON CONFLICT DO NOTHING RETURNING id`: пустой RETURNING = кто-то уже записал |
| Два воркера создают каноничную вакансию с одним текстом (в т.ч. из разных источников) | UNIQUE `content_hash`; проигравший ловит конфликт и привязывает наблюдение к победителю |
| Два воркера берут одно и то же наблюдение на dedup | `FOR UPDATE SKIP LOCKED` на `vacancy_sources WHERE ingest_status='PENDING'` |
| Два воркера анализируют/оценивают одну вакансию | Оценка: UNIQUE `(vacancy_id, user_id)` + `ON CONFLICT DO NOTHING`; анализ: `UPDATE … WHERE analyzed_at IS NULL` в транзакции с `FOR UPDATE` |
| Дубликат уведомления | UNIQUE `(user_id, vacancy_id)` + создание строки в той же транзакции, что и `decision=APPROVED` |
| Два notifier-цикла шлют одно | Claim через `SKIP LOCKED` + `SENDING` + lease |
| Scheduler создал два job на один источник | Partial UNIQUE на jobs |

### Очереди на SKIP LOCKED

Job:
```sql
UPDATE jobs SET status='RUNNING', locked_until = now() + interval '5 minutes', attempts = attempts + 1
WHERE id = (
  SELECT id FROM jobs
  WHERE status='PENDING' AND run_after <= now()
  ORDER BY priority, run_after
  FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING *;
```
Воркер каждые ~60 с продлевает `locked_until` (heartbeat). Sweeper раз в минуту: `RUNNING AND locked_until < now()` → `PENDING` (если `attempts < max_attempts`, с backoff в `run_after`) иначе `FAILED` + статус источника DEGRADED. Retry: экспоненциальный backoff с jitter (30 с → 1 → 2 → … до 30 мин).

Notifications — тот же паттерн по `(status='QUEUED' AND next_attempt_at <= now() AND not_before <= now())`, lease 2 минуты.

**Гарантия доставки: at-most-once для single-уведомления.** Перед `send_message` пишем attempt `STARTED`, после ответа — `OK`. Если процесс упал между отправкой и записью, sweeper видит `SENDING` с просроченным lease и attempt `STARTED` без исхода и ставит `UNCONFIRMED` вместо автоматического повтора: редкая потеря лучше дубля, вакансию можно найти через `/recent`. Retry делаем только для явных ошибок Telegram (FloodWait, сеть до отправки, 5xx). `/status` показывает число UNCONFIRMED.

---

## 5. Статусы: три оси

**Ось 1 — `vacancy_sources.ingest_status`:** PENDING (сохранено, ждёт dedup) → CANONICAL (создана новая каноничная вакансия) | DUPLICATE (привязано к существующей); INVALID (не прошло validate, терминальное). Переходы:

| Из → В | Кто |
|---|---|
| (нет) → PENDING | persist_raw |
| (нет) → INVALID | persist_raw (невалидное тоже сохраняем, для отладки) |
| PENDING → CANONICAL | dedup |
| PENDING → DUPLICATE | dedup |

**Ось 2 — `vacancy_evaluations.decision`** (на пару vacancy × user):

| Из → В | Условие |
|---|---|
| (нет) → PENDING | вакансия проанализирована, оценка создана |
| PENDING → HARD_REJECTED | сработал любой hard reject |
| PENDING → FILTERED | нет hard reject, но: устарела / relevance < порога / risk_level > максимума / источник скрыт |
| PENDING → APPROVED | всё прошло |
| HARD_REJECTED / FILTERED / APPROVED → PENDING | **только** при пересчёте после смены правил (`rules_version` изменился) |

**Ось 3 — `notifications.status`:**

| Из → В | Условие |
|---|---|
| (нет) → QUEUED | `decision=APPROVED` и `notify_eligible` |
| QUEUED → SENDING | claim notifier |
| SENDING → SENT | Telegram ответил OK |
| SENDING → QUEUED | retryable ошибка (attempts++ , backoff) |
| SENDING → FAILED | fatal ошибка или attempts исчерпаны |
| SENDING → UNCONFIRMED | lease истёк без исхода (см. выше) |
| QUEUED → SKIPPED | политика SKIP_NOTIFICATION в quiet hours / вакансия устарела к моменту отправки |

Все переходы описаны словарём в `models/transitions.py`; функция `transition(current, target)` бросает `InvalidTransition`. Тесты: каждый допустимый переход проходит, каждый недопустимый падает.

---

## 6. Pipeline и что пишется в БД

```
FETCH → PARSE → NORMALIZE → VALIDATE
→ PERSIST RAW (vacancy_sources)              ← точка, после которой данные не теряются
→ DEDUPLICATE (PENDING → CANONICAL/DUPLICATE)
→ ANALYZE (vacancy-level, один раз): STRUCTURAL EXTRACTION → RISK ENGINE   → vacancies.analyzed_at
→ EVALUATE (per user): FRESHNESS → HARD REJECTS → RELEVANCE SCORING → DECISION
→ PERSIST EVALUATION
→ NOTIFICATION DECISION → NOTIFICATION QUEUE → SEND
```

Ключевая идея: **состояние pipeline выводится из данных, а не из памяти воркера.** Незавершённая работа находится запросами:
- есть `vacancy_sources.ingest_status='PENDING'` → нужен dedup;
- есть `vacancies.analyzed_at IS NULL` → нужен analyze;
- есть проанализированная вакансия без строки в `vacancy_evaluations` для пользователя → нужна оценка;
- есть `APPROVED` без `notifications` при `notify_eligible` → нужно уведомление (на практике в одной транзакции с решением).

| Шаг | Что пишется | Если воркер упал |
|---|---|---|
| FETCH/PARSE/NORMALIZE/VALIDATE | ничего (в памяти) | job вернётся в очередь по lease, checkpoint не сдвинулся → повторный fetch того же диапазона, UNIQUE отсечёт повторы |
| PERSIST RAW | `vacancy_sources` пачкой + **checkpoint job** в одной транзакции | атомарно: либо пачка и checkpoint, либо ничего |
| DEDUPLICATE | `vacancies` (для CANONICAL) + `vacancy_id` и `ingest_status` в наблюдении, одна транзакция | наблюдение остаётся PENDING, подберёт следующий цикл |
| ANALYZE | extraction, risk, `analyzed_at` одним UPDATE | `analyzed_at` NULL → повтор (шаг чистый, без побочных эффектов) |
| EVALUATE | `vacancy_evaluations` + (если APPROVED и eligible) `notifications` в одной транзакции | нет строки оценки → повтор; UNIQUE не даст задвоить |
| SEND | attempts + `notifications.status` | см. at-most-once выше |

Дополнительно:
- **Freshness** (по `published_at`, при его отсутствии — по `collected_at` с флагом `PUBLISHED_AT_UNKNOWN`) применяется в EVALUATE, потому что зависит от пользователя и источника.
- **Hard reject не пропускает risk и extraction** — они уже посчитаны на уровне вакансии (для статистики «High risk»). `relevance_score` при hard reject не считается (NULL).
- **DUPLICATE-наблюдения** дальше не идут: оценка и уведомление привязаны к каноничной вакансии.
- **Dedup, уровни:** (1) `(source_instance_id, external_id)` — на INSERT; (2) `content_hash` (UNIQUE); (3) `normalized_hash`; (4) SimHash 64 бита, 4 полосы по 16 бит. Кандидаты = вакансии с совпадением хотя бы одной полосы **и** `published_at` в окне ±30 дней; сравнение по Хэммингу ≤ 3 бита. Никакого попарного сравнения всего корпуса. URL-уровень — только для сайтовых адаптеров, где URL стабилен.
- **Notification decision:** `notify_eligible = decision=APPROVED AND mode=LIVE AND fresh AND источник не в mutes`. Затем:
  1. quiet hours gate: политика QUEUE_AND_SEND_LATER → `not_before = конец тихих часов`; SKIP → `SKIPPED`; DIGEST_AFTER_QUIET → `delivery=DIGEST`, `not_before = конец`;
  2. rate limit (в Notifier): считаем `SENT` за последнюю минуту/час из БД;
  3. burst guard: если `QUEUED` > `digest_threshold`, всё лишнее уходит в **один** дайджест (`delivery=DIGEST`), а не в N сообщений.

---

## 7. Relevance и Risk

### Relevance (`config/scoring.yaml`, документируется в `docs/scoring.md`)

```
relevance = clamp( base + Σ_group min(cap_group, Σ w_positive) − Σ w_soft_penalty , 0, 100 )
base = 40
```
Группы положительных сигналов ограничены капом, чтобы 10 совпавших ключевых слов не давали 100 баллов:

| Группа | Сигналы (вес) | Кап |
|---|---|---|
| WORK_FORMAT | REMOTE +15 | 15 |
| SCHEDULE | FLEXIBLE_SCHEDULE +8, FREE_SCHEDULE +8 (считаются как один сигнал), PART_TIME +8 | 16 |
| PAYMENT_MODEL | PROJECT_BASED +5, PIECE_RATE +4 | 8 |
| COMMUNICATION | NO_CALLS +6 | 6 |
| TOPIC | CONTENT +12, WRITING +10, SEO +10, AI +10 | 25 |

Soft penalties (без капа, но общий минимум 0): EXPERIENCE_REQUIRED −10 (если ≥3 лет — −15), NO_EXPERIENCE_REQUIRED — не штраф (POSITIVE, +3 в SCHEDULE-подобной группе EXPERIENCE, кап 3), LOW_SALARY −10 (только если единицы сопоставимы с `min_salary`), SALARY_NOT_SPECIFIED −5, INFO_INSUFFICIENT −10 (описание < 200 символов и нет обязанностей), HEAVY_COMMUNICATION −8, SCHEDULE_NOT_IDEAL −5.

Решение (порядок вычисления, побеждает первое сработавшее):
1. любой hard reject → `HARD_REJECTED`;
2. устарела по freshness → `FILTERED` (`STALE`);
3. источник скрыт → `FILTERED` (`SOURCE_MUTED`);
4. `relevance < min_relevance_score` → `FILTERED` (`LOW_RELEVANCE`);
5. `risk_level > max_risk_level` → `FILTERED` (`RISK_TOO_HIGH`);
6. иначе `APPROVED`.

### Risk (`risk_engine/`)

`risk_score = min(100, Σ весов сработавших флагов)`. Уровни: LOW 0–29, MEDIUM 30–59, HIGH ≥ 60. Флаги «жёсткого» класса (`PAYMENT_REQUIRED`, `MLM_SCHEME`, `TRAINING_FEE`) принудительно дают HIGH независимо от суммы. Один слабый флаг сам по себе HIGH не даёт, так что «по одному слову» не срабатывает.

Веса (стартовые): PAYMENT_REQUIRED 60 (hard), MLM_SCHEME 60 (hard), UNREALISTIC_SALARY 30, VAGUE_JOB_DESCRIPTION 20, CONTACT_ONLY_MESSENGER 10, NO_COMPANY_INFO 5, URGENCY_PRESSURE 10, CRYPTO_SCHEME 40, EQUIPMENT_PURCHASE 40, MONEY_TRANSFER 50. `UNREALISTIC_SALARY` в MVP: зарплата > порога (в конфиге, по типу оплаты) **и** нет требований к опыту/навыкам; таблица «типичных» ставок — Phase 5.

### Три примера (по реальным типам объявлений)

**Пример 1.** «Контент-менеджер (SEO, AI-тексты). Удалённо, гибкий график, частичная занятость. 50 000–80 000 ₽/мес. Опыт от 1 года.»
Positive: REMOTE 15 + SCHEDULE (8+8=16, кап 16) + TOPIC (CONTENT 12 + SEO 10 + AI 10 = 32 → кап 25) = 56. Soft: EXPERIENCE_REQUIRED −10. Relevance = 40 + 56 − 10 = **86**. Risk: флагов нет → **0, LOW**. Решение: `86 ≥ 70`, `LOW ≤ LOW` → **APPROVED**.
Reasons: `positive:[REMOTE,FLEXIBLE_SCHEDULE,PART_TIME,CONTENT,SEO,AI], negative:[EXPERIENCE_REQUIRED]`.

**Пример 2.** «Менеджер по продажам без опыта, 150 000–300 000 ₽, 2 часа в день, свободный график, удалённо. Подробности в личку @manager.»
Extraction: зарплата высокая, требований нет. Positive: REMOTE 15 + FREE_SCHEDULE 8 = 23. Soft: INFO_INSUFFICIENT −10. Relevance = 40 + 23 − 10 = **53**. Risk: UNREALISTIC_SALARY 30 + VAGUE_JOB_DESCRIPTION 20 + CONTACT_ONLY_MESSENGER 10 = **60, HIGH**. Решение: relevance < 70 → **FILTERED** (`LOW_RELEVANCE`, `RISK_TOO_HIGH`). Сохраняется, не отправляется.
Вариант с фразой «стартовый взнос 5 000 ₽»: hard reject `PAYMENT_REQUIRED` → **HARD_REJECTED**, relevance не считается; risk_flag `PAYMENT_REQUIRED`, HIGH.

**Пример 3.** «Копирайтер. Удалённо, частичная занятость, 1 500 ₽ за статью, звонки не нужны, опыт от 2 лет, портфолио обязательно.»
Positive: REMOTE 15 + PART_TIME 8 + PIECE_RATE 4 + NO_CALLS 6 (негация «звонки не нужны» → FLIP) + TOPIC (WRITING 10 + CONTENT 12 = 22) = 55. Soft: EXPERIENCE_REQUIRED −10. Зарплата `PER_ITEM` — с `min_salary` (месячная) не сравнивается, штрафа LOW_SALARY нет. Relevance = 40 + 55 − 10 = **85**. Risk 0, LOW → **APPROVED**.

---

## 8. Негации

Проблема: «холодные звонки» → hard reject, «без холодных звонков» → positive `NO_CALLS`.

Алгоритм в `filters/negation.py`, работает по **нормализованному** тексту с леммами:

1. **Сегментация.** Текст → предложения → клаузы. Границы клауз: `. ! ? ; \n`, а также `,` перед союзами `но, а, однако, зато, кроме`.
2. **Поиск совпадения.** Правило (лемма/фраза/regex) даёт span в токенах клаузы.
3. **Поиск негатора в окне:** 4 токена **перед** span и 3 токена **после**, только внутри той же клаузы. Негаторы: `не, нет, без, ни, никаких, никакой, отсутствует, отсутствуют, исключены, исключено, не требуется, не нужен/нужна/нужны/нужно, не надо, не предполагает, не обязательно, не придётся, не будет`.
4. **Полярность:** число негаторов в окне нечётное → `NEGATED`, чётное (двойное отрицание, «нельзя без звонков») → `AFFIRMED`.
5. **Исключения, которые не считаются негацией:** `не только`, `не менее/не более/не выше/не ниже` (числовые границы), `не позднее`, `не раньше`.
6. **Действие по правилу.** У правила поле `negation_mode`: `IGNORE` (негация не меняет: правило не срабатывает вообще) или `FLIP_TO: <code>` (срабатывает другой код). Например: `COLD_CALLS` (HARD_REJECT, `FLIP_TO: NO_CALLS` POSITIVE); `EXPERIENCE_REQUIRED` (SOFT, `FLIP_TO: NO_EXPERIENCE_REQUIRED`); `BUSINESS_TRIPS` (HARD_REJECT, `FLIP_TO: NO_TRIPS`).
7. **Явные фразы приоритетнее.** Отдельные regex в конфиге вроде `без (холодных )?звонков`, `звонки не нужны`, `без опыта` матчатся первыми и сразу дают итоговый код, полагаясь на общий алгоритм лишь для остального.

Обязательный набор тестов (фикстуры, русские предложения): «холодные звонки» → AFFIRMED; «без холодных звонков» → NEGATED; «звонки не нужны» → NEGATED (негатор после); «звонков нет» → NEGATED; «не требуется опыт» → NEGATED; «опыт не менее 2 лет» → AFFIRMED (исключение); «звонки, но не холодные» → «звонки» AFFIRMED, «холодные» NEGATED (разные клаузы — известное ограничение, тест фиксирует поведение); «нельзя без звонков» → AFFIRMED (двойное); «командировок не будет» → NEGATED; «возможны редкие командировки» → AFFIRMED; «не только удалённо» → AFFIRMED (исключение). Известное ограничение: сложные конструкции без явных маркеров не разбираются, это ложные срабатывания, которые в MVP допустимы (ошибка в сторону hard reject безопаснее для пользователя, чем пропуск).

---

## 9. INITIAL_SYNC / BACKFILL

**Checkpoint** (jsonb в `jobs.checkpoint`): `{"direction":"backward","oldest_id":123,"imported":18500,"total_estimate":null,"batch":100}`. Пачка сообщений через `iter_messages(offset_id=oldest_id, limit=100)`; **пачка `vacancy_sources` и обновление checkpoint пишутся одной транзакцией.** Падение в любой момент → продолжаем с последнего checkpoint, повторные вставки отсечёт UNIQUE. Между пачками пауза (лимитер), при FloodWait — `jobs.run_after = now + retry_after`, job возвращается в PENDING.

**Добавление источника без пропусков и без дублей:** при `/add_source telegram @x`: (1) resolve канала, (2) запоминаем `last_processed_message_id = id самого нового сообщения` **до** старта sync, (3) создаём job `INITIAL_SYNC` (идёт назад от этого id), (4) LIVE идёт вперёд от `last_processed_message_id`. Диапазоны не пересекаются, а если пересеклись — UNIQUE защитит.

**Защита от рассылки старья, четыре слоя:**
1. `vacancy_sources.mode` ≠ LIVE → `notify_eligible=false` (решение и score считаются нормально, вакансия ищется и попадает в статистику);
2. Freshness: старше `max_age_hours` не отправляется даже в LIVE (канал вернулся после простоя);
3. Burst guard: >`digest_threshold` в очереди → один дайджест;
4. Глобальные лимиты Notifier: N/мин, M/час.
Каналу разрешён «стартовый дайджест» только по явной команде пользователя (`/recent` или кнопка), автоматически — никогда.

**Оценка объёма:** INITIAL_SYNC по умолчанию ограничен последними N днями/сообщениями (по умолчанию 30 дней, настраивается), полная история — режим BACKFILL по явной команде. Прогресс: `Imported: 18 500 / ~unknown`, последний checkpoint виден в `/status`.

**Повторный сеанс после смены правил:** пересчёт оценок (`rules_version` изменился) не создаёт уведомлений для вакансий старше freshness и для `mode≠LIVE`.

---

## 10. План фаз

| Фаза | Содержание | Критерий «готово» (проверяется командой) |
|---|---|---|
| **0. Вертикальный срез** | 1 Telegram-канал → normalize → 1 hard reject (`PAYMENT_REQUIRED`) → простой score → БД → 1 уведомление боту. Без quiet hours, digest, settings UI | `docker compose up -d db && alembic upgrade head && pytest tests/phase0 && python scripts/phase0_demo.py --channel @X` приходит ≥1 сообщение владельцу; повторный запуск: 0 новых записей, 0 новых сообщений |
| **1.1 БД и модели** | Полная схема, миграции, репозитории, таблица переходов | `alembic upgrade head` на чистой БД; `pytest tests/integration -k "constraints or transitions or idempotency"` |
| **1.2 Normalizer + salary + extraction** | модули + фикстуры | `pytest tests/unit -k "normalizer or salary or extraction"`, на всех форматах из ТЗ §14 |
| **1.3 Filters + scoring + risk** | keyword engine, негации, 3 типа правил, формула, risk, seed YAML | `pytest tests/unit -k "filters or negation or scoring or risk"`; три примера из §7 воспроизводят числа |
| **1.4 Notifier** | очередь, rate limit, quiet hours, digest, FloodWait, UNCONFIRMED | `pytest tests/integration -k notifier`; сценарий «500 старых → 0 отправок, 1 дайджест по команде» |
| **1.5 Bot + settings + sources + INITIAL_SYNC + Docker + README** | все команды из ТЗ §27 | `docker compose up`, `/add_source`, `/settings`, `/stats`, `/search` работают; `pytest` весь зелёный |
| **2. HH.ru** | adapter | Только при наличии документации в `docs/sources/hh.md`; иначе stub → `UNSUPPORTED`. Тесты на сохранённых фикстурах ответов |
| **3. Kwork/FL/Workzilla** | по одному, каждый по тому же правилу | то же |
| **4. Scaling** | отдельные процессы, Redis по доказанной необходимости | нагрузочный замер, показывающий узкое место; без замера фаза не начинается |
| **5. Advanced** | similarity-tuning, таблицы ставок, аналитика, обучение на feedback | начинается только при стабильных 0–1 |

Правило: не начинаем следующую подфазу, пока в текущей не зелёные тесты и не выполнен критерий.

---

## 11. Риски и открытые вопросы

**Открытые вопросы (по важности):**

1. **Single-user или multi-user?** Я заложил single-user с multi-user-ready схемой. Подтверди или скажи, что multi-user не нужен (тогда часть таблиц упростится).
2. **Аккаунт для Telethon.** Есть отдельный аккаунт/номер, не основной? Нужны ли каналы, требующие вступления (приватные, с заявкой)? Лимит подписок на аккаунт ограничен, для тысяч каналов чтение без вступления возможно только для публичных.
3. **Откуда список каналов?** Есть готовый список (файл `.txt`/`.csv`)? Тогда в Phase 1.5 добавим `scripts/load_channels.py`.
4. **Реальные критерии.** Мне нужен твой список: что именно ищешь (контент, SEO, AI, remote, частичная занятость, что ещё), что категорически нет, минимальная зарплата, какие форматы оплаты интересны. Я соберу `default_rules.yaml`, ты поправишь.
5. **Где будет работать система?** Твой ПК (тогда мониторинг только когда включён) или VPS. От этого зависят Docker-конфигурация и требования к перезапуску.
6. **Quiet hours по умолчанию:** какая политика (QUEUE_AND_SEND_LATER, SKIP, дайджест утром) и какое окно?
7. **Один пост = несколько вакансий (подборки).** MVP помечает и не отправляет. Устраивает?
8. **Стартовое окно INITIAL_SYNC:** 30 дней? Или больше?
9. **Порог «нереалистичной» зарплаты** для UNREALISTIC_SALARY: есть интуиция по ставкам в твоих нишах (₽ за статью/час/месяц), или начинаем с грубых порогов?
10. **Хранение контактов** из объявлений (`contact`: @ник, телефон): хранить в БД (это персональные данные третьих лиц) или маскировать?

**Риски:**
- **Бан/ограничение Telethon-аккаунта.** Митигируем отдельным аккаунтом, низким темпом, единым limiter. Полностью риск не устраняется.
- **Качество морфологии/негаций** — главный источник ложных срабатываний. Митигируем тестовым набором и пересчётом после смены правил (по `rules_version`), т.к. все вакансии хранятся.
- **Рост БД:** сотни тысяч записей с `raw_payload` и текстом. План: `raw_payload` хранить только для последних N дней либо сжимать, решение в Phase 4.
- **Юридическая сторона сайтов:** для каждого сайта проверка ToS/robots и доступности API; при сомнении — `UNSUPPORTED`.
- **Окно at-most-once:** UNCONFIRMED — редкая потеря уведомления вместо дубля; принято осознанно.
- **Один процесс = единая точка отказа** в MVP; митигируем docker `restart: unless-stopped` и sweeper на старте (подхватывает всё, что осталось в PENDING/RUNNING).

---

## Итог

**Решено:** PostgreSQL как единственное хранилище и единственная очередь, Redis откладывается; три оси статусов; наблюдения (`vacancy_sources`) отделены от каноничных вакансий, сохраняются до dedup; extraction и risk считаются один раз на вакансию, фильтры и score на пару вакансия × пользователь; hard reject абсолютен; исторический импорт не уведомляет; доставка at-most-once.

**Нужно твоё решение:** вопросы 1–5 выше (1, 2, 4 и 5 блокируют Phase 0; остальные могут подождать).
