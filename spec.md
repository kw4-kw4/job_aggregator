Действуй как Senior Python Developer / Software Architect с опытом разработки высоконагруженных асинхронных парсеров, Telegram-ботов, систем агрегации данных, очередей задач и PostgreSQL.

Мне нужно разработать полноценную систему автоматического поиска вакансий и подработок.

Система должна мониторить большое количество Telegram-каналов и сайтов с вакансиями, извлекать объявления, нормализовать данные, определять дубликаты, фильтровать объявления по заданным критериям, оценивать их релевантность и риск, сохранять результаты в PostgreSQL и отправлять подходящие вакансии мне в личный Telegram через бота.

Главная цель — создать не одноразовый парсер, а модульную, расширяемую и отказоустойчивую систему, в которую можно постепенно добавлять новые источники без переписывания ядра.

---

# 1. Основные архитектурные принципы

Приоритеты проекта:

1. Надежность.
2. Корректность обработки данных.
3. Идемпотентность.
4. Расширяемость.
5. Масштабируемость.
6. Производительность.
7. Простота сопровождения.
8. Объяснимость результатов.

Не усложняй систему без необходимости.

Не добавляй технологии только ради "enterprise architecture".

Каждую существенную технологическую зависимость необходимо обосновать.

Особенно это касается Redis, отдельных worker-процессов и дополнительных брокеров.

---

# 2. Стек

Используй:

* Python 3.11+
* aiogram 3.x
* asyncio
* PostgreSQL
* SQLAlchemy 2.x
* Alembic
* Pydantic 2.x
* httpx или aiohttp
* Playwright только там, где действительно требуется браузер/JavaScript
* BeautifulSoup4 / lxml / selectolax при необходимости
* APScheduler либо другой подходящий scheduler
* pytest
* Docker / Docker Compose

Для Telegram-мониторинга используй подходящий Telegram client API, например Telethon, если для получения сообщений это действительно необходимо.

Раздели:

1. Telegram Bot API — пользовательский интерфейс и уведомления.
2. Telegram client/API — мониторинг доступных Telegram-источников.

Не предполагай, что Telegram Bot API позволяет читать любые произвольные каналы.

Не реализовывай обход:

* CAPTCHA
* авторизации без разрешения
* paywall
* rate limit
* антибот-защиты
* ограничений доступа
* других технических механизмов защиты.

Если источник недоступен легальным/разрешенным способом, зафиксируй его как unsupported или auth_required.

---

# 3. Главное архитектурное разделение: Adapter и Source Instance

Не смешивай тип источника и конкретный источник.

Есть:

## Source Adapter

Это реализация способа получения данных:

```text
TelegramAdapter
HHAdapter
KworkAdapter
FLAdapter
WorkzillaAdapter
```

## Source Instance

Это конкретный экземпляр источника, который создается пользователем.

Например:

```text
Adapter:
HHAdapter

Instances:
HH → "Удалённая работа"
HH → "Контент"
HH → конкретный URL поиска
```

Для Telegram:

```text
TelegramAdapter

Instances:
@channel_1
@channel_2
@channel_3
...
```

Добавление нового конкретного источника не должно требовать изменения Python-кода.

Добавление нового типа источника требует нового adapter.

Например:

```python
class BaseSourceAdapter(ABC):
    async def fetch(...):
        ...
```

и:

```python
class SomeNewSourceAdapter(BaseSourceAdapter):
    ...
```

---

# 4. Масштаб

Целевая архитектура должна позволять работать минимум с:

* 1000+ Telegram-каналами;
* десятками экземпляров сайтов;
* сотнями тысяч вакансий;
* потенциально миллионами сохраненных объявлений.

Фраза "1000+ источников" означает прежде всего возможность мониторинга 1000+ Telegram-каналов и большого количества экземпляров поисковых запросов сайтов.

Не создавай чрезмерно сложную distributed-систему только ради самого числа 1000.

Архитектура должна масштабироваться постепенно.

---

# 5. Pipeline обработки вакансии

Сделай явный и документированный pipeline:

```text
FETCH
  ↓
PARSE
  ↓
NORMALIZE
  ↓
VALIDATE
  ↓
DEDUPLICATE
  ↓
HARD FILTERS
  ↓
STRUCTURAL EXTRACTION
  ↓
RELEVANCE SCORING
  ↓
RISK ENGINE
  ↓
PERSIST
  ↓
NOTIFICATION DECISION
  ↓
NOTIFICATION
```

Этот порядок должен быть четко реализован в коде.

---

# 6. Хранение вакансий

Даже если вакансия не подходит пользователю, она по возможности должна сохраняться в БД.

Например:

```text
vacancy
processing_result
notification
```

должны позволять понять:

* что было найдено;
* когда найдено;
* откуда пришло;
* как было обработано;
* почему отфильтровано;
* какой score получило;
* какой risk получило;
* было ли отправлено пользователю.

Не удаляй неподходящие вакансии только потому, что они не прошли фильтр.

Это необходимо для:

* статистики;
* повторной обработки;
* изменения фильтров;
* отладки;
* анализа качества фильтрации.

---

# 7. Жизненный цикл вакансии

Определи явную state machine.

Например:

```text
NEW
PARSED
INVALID
DUPLICATE
FILTERED
SCORED
BLOCKED
APPROVED
QUEUED_FOR_NOTIFICATION
SENT
SEND_FAILED
```

Можно использовать другую структуру, если она архитектурно лучше.

Но жизненный цикл должен быть явным.

Не смешивай:

* статус самой вакансии;
* результат фильтрации;
* статус уведомления.

При необходимости храни их отдельно.

---

# 8. Hard Reject / Soft Penalty / Positive Signal

Это обязательное правило.

Все правила фильтрации раздели на три типа.

## Hard Reject

Вакансия автоматически не должна отправляться пользователю.

Примеры:

* обязательные холодные звонки;
* физическая работа;
* офис, если включен режим "только удалённо";
* обязательные командировки;
* платное трудоустройство;
* необходимость внести деньги;
* MLM;
* явные признаки мошеннической схемы;
* запрещенные пользователем категории.

Hard Reject имеет абсолютный приоритет.

Score не может отменить Hard Reject.

## Soft Penalty

Снижает relevance score, но не обязательно блокирует вакансию.

Например:

* требуется опыт;
* слишком низкая зарплата;
* недостаточно информации;
* много коммуникации;
* неидеальный график;
* частичная занятость не указана явно.

## Positive Signal

Повышает relevance score.

Например:

* remote;
* flexible schedule;
* project-based;
* part-time;
* piece-rate;
* no calls;
* content;
* writing;
* SEO;
* AI;
* free schedule.

---

# 9. Relevance Score и Risk Score должны быть разными

Не смешивай "насколько вакансия подходит" и "насколько она подозрительная".

Используй:

```text
relevance_score: 0..100
risk_score: 0..100
risk_level: LOW / MEDIUM / HIGH
```

Например:

```text
relevance_score = 91
risk_score = 25
risk_level = MEDIUM
```

Решение о публикации должно основываться на отдельных настройках:

```text
minimum_relevance_score
maximum_risk_level
```

Например:

```text
minimum_relevance_score = 70
maximum_risk_level = LOW
```

---

# 10. Explainability

Каждое решение должно иметь машиночитаемые причины.

Например:

```json
{
  "positive": [
    "REMOTE",
    "FLEXIBLE_SCHEDULE",
    "CONTENT",
    "SEO"
  ],
  "negative": [
    "EXPERIENCE_REQUIRED"
  ],
  "hard_reject": [],
  "risk_flags": [
    "HIGH_SALARY_WITH_LOW_REQUIREMENTS"
  ]
}
```

Telegram-бот должен преобразовывать эти коды в нормальный русский текст.

Например:

```text
Почему подходит:
+ удаленная работа
+ гибкий график
+ работа с текстами

Минусы:
- требуется опыт от 1 года

Риски:
- зарплата заметно выше типичной для указанных обязанностей
```

Это необходимо для отладки системы.

---

# 11. Нормализация русского и английского текста

Создай отдельный модуль:

```text
normalizer/
```

Он должен выполнять:

* приведение регистра;
* Unicode normalization;
* очистку HTML;
* удаление мусора;
* нормализацию пробелов;
* нормализацию дефисов;
* нормализацию похожих символов;
* приведение некоторых вариантов написания слов;
* нормализацию словоформ в разумных пределах.

Примеры:

```text
удаленка
удалёнка
удаленно
удалённо
удалённая работа
remote
remote work
```

должны корректно сопоставляться как релевантные варианты.

Аналогично:

```text
ИИ
ии
AI
ai
нейросети
нейросеть
```

Regex и keyword matching должны работать по нормализованному представлению, а не по сырому HTML.

Не уничтожай оригинальный текст.

Всегда сохраняй raw_text отдельно.

---

# 12. Keyword engine

Поддерживай:

```text
include_keywords
exclude_keywords
```

и:

* точные совпадения;
* фразы;
* словоформы;
* normalized matching;
* regex при необходимости.

Раздели правила по категориям.

Например:

```text
HARD_REJECT_KEYWORDS
NEGATIVE_KEYWORDS
POSITIVE_KEYWORDS
RISK_KEYWORDS
```

Не зашивай пользовательские критерии непосредственно в код.

---

# 13. Structural extraction

Из текста необходимо пытаться извлекать:

* remote / office / hybrid;
* график;
* занятость;
* зарплату;
* валюту;
* опыт;
* тип оплаты;
* необходимость звонков;
* командировки;
* место работы;
* требования;
* навыки;
* тип занятости.

Не полагайся только на ключевые слова.

Храни результат extraction отдельно от оригинального текста.

---

# 14. Зарплата и оплата

Создай отдельный модуль:

```text
salary_parser/
```

Он должен уметь обрабатывать как минимум:

```text
20 000–30 000 ₽
от 30 000 ₽
до 30 000 ₽
30 000 ₽/месяц
1 500 ₽ за статью
500 ₽/час
$500
€1000
по договорённости
зарплата не указана
```

Поддерживай различные варианты разделителей:

```text
30к
30 K
30 000
30.000
30,000
```

Определи отдельные типы оплаты:

```text
MONTHLY
HOURLY
DAILY
WEEKLY
PROJECT
PER_ITEM
FIXED
UNKNOWN
NEGOTIABLE
```

Для каждой суммы храни:

```text
amount_min
amount_max
currency
payment_type
period
raw_salary_text
```

Не пытайся некорректно сравнивать:

```text
50 000 ₽/месяц
```

и:

```text
1 500 ₽ за статью
```

как будто это одна и та же единица.

Для проектной и сдельной оплаты допускай относительную оценку привлекательности, но не выдумывай месячный доход без достаточных данных.

Если валюту определить нельзя:

```text
currency = UNKNOWN
```

Не угадывай валюту.

Если зарплата "по договорённости":

```text
payment_type = NEGOTIABLE
```

Если зарплата отсутствует:

```text
salary_status = NOT_SPECIFIED
```

---

# 15. Freshness Filter

Добавь отдельный freshness filter.

Пользователь должен иметь возможность задавать:

```text
max_age_hours
```

или:

```text
max_age_days
```

Например:

```text
24 часа
3 дня
7 дней
```

Разные источники могут иметь разные настройки свежести.

Используй `published_at`, а если он неизвестен — корректно обрабатывай отсутствие даты.

---

# 16. Исторический импорт

Раздели режимы:

```text
LIVE_MONITORING
INITIAL_SYNC
BACKFILL
```

## LIVE_MONITORING

Обрабатывает новые объявления.

## INITIAL_SYNC

Первоначальная загрузка истории после добавления источника.

## BACKFILL

Дополнительная историческая загрузка.

Критически важно:

исторически импортированные вакансии НЕ должны автоматически отправляться как новые уведомления.

Historical import и notification pipeline должны быть разделены.

Для больших импортов используй:

* pagination;
* batches;
* checkpoint;
* resumability;
* ограничение нагрузки;
* сохранение прогресса.

Например:

```text
Imported: 18 500 / unknown
Last checkpoint: ...
```

После остановки процесс должен уметь продолжить.

---

# 17. Deduplication

Дедупликация должна работать на нескольких уровнях.

Используй:

```text
external_id
source_instance_id
URL
content_hash
normalized_text_hash
similarity
```

при необходимости.

Разные источники могут содержать одну и ту же вакансию.

Поэтому:

```text
source_id
```

сам по себе недостаточен.

---

# 18. Идентификаторы

Разделяй:

```text
source_type
source_instance_id
external_id
```

Например:

```text
HH:
source_type = hh
source_instance_id = 123
external_id = vacancy_456
```

Telegram:

```text
source_type = telegram
source_instance_id = channel_123
external_id = message_456
```

Kwork:

```text
source_type = kwork
source_instance_id = search_123
external_id = project_456
```

Не смешивай эти понятия.

---

# 19. Идемпотентность и race conditions

Система должна быть идемпотентной.

Рассмотри ситуацию:

```text
Worker A → vacancy X
Worker B → vacancy X
```

Оба worker не должны привести к двум одинаковым уведомлениям.

Обеспечь это комбинацией:

* unique constraints PostgreSQL;
* транзакций;
* atomic operations;
* processing state;
* notification state;
* уникальных ключей идемпотентности;
* при необходимости distributed locks.

Особенно важно защитить:

```text
vacancy creation
processing
notification creation
notification sending
```

Повторный запуск worker не должен создавать дубликаты.

---

# 20. Telegram notification queue

Не отправляй уведомления напрямую из любого parser worker.

Используй логический notification pipeline.

Система должна иметь:

```text
notification queue
```

и контролировать:

* rate limit;
* retries;
* Telegram errors;
* FloodWait;
* временную недоступность;
* порядок отправки;
* duplicate notification.

Нужно предусмотреть ограничение количества уведомлений.

Например:

```text
max_notifications_per_minute
max_notifications_per_hour
```

При массовом импорте:

```text
500 старых вакансий
```

не должно привести к отправке 500 сообщений пользователю.

При необходимости используй:

* batching;
* digest;
* grouping;
* deferred notifications.

---

# 21. Quiet Hours

Поддерживай:

```text
timezone
quiet_hours_start
quiet_hours_end
```

Например:

```text
Europe/Moscow
23:00–08:00
```

Во время quiet hours должна быть четко определена политика:

```text
QUEUE_AND_SEND_LATER
SKIP_NOTIFICATION
DIGEST_AFTER_QUIET_HOURS
```

Настройка должна быть пользовательской.

Источники продолжают собираться независимо от quiet hours, если это разрешено конфигурацией.

---

# 22. Anti-scam / Risk Engine

Сделай отдельный модуль:

```text
risk_engine/
```

Проверяй признаки:

* просьбы заплатить;
* вступительные взносы;
* платное обучение;
* покупка оборудования;
* перевод денег;
* сомнительные криптосхемы;
* MLM;
* сетевой маркетинг;
* нереалистично высокая оплата;
* отсутствие четких обязанностей;
* подозрительные контактные схемы.

Не определяй мошенничество по одному слову.

Результат:

```text
risk_score
risk_level
risk_flags
```

Например:

```text
risk_level = HIGH

risk_flags:
PAYMENT_REQUIRED
UNREALISTIC_SALARY
VAGUE_JOB_DESCRIPTION
```

---

# 23. Источники и их состояния

Каждый Source Instance должен иметь status:

```text
ACTIVE
PAUSED
DEGRADED
UNSUPPORTED
AUTH_REQUIRED
RATE_LIMITED
ERROR
```

Например:

```text
Telegram @example
Status: ACTIVE

Last check: 2 min ago
Last successful fetch: 2 min ago
Found today: 142
```

Если источник временно ломается, это не должно останавливать остальные источники.

---

# 24. Scheduler / Worker / Bot

Четко раздели ответственность.

## Bot

Отвечает только за:

* Telegram commands;
* settings;
* source management;
* user interaction;
* notification presentation.

## Scheduler

Отвечает только за:

* определение момента запуска задач;
* создание задач;
* запуск initial sync / live monitoring / backfill jobs.

Scheduler не должен сам выполнять тяжелый parsing.

## Worker

Отвечает за:

* получение задач;
* fetch;
* parse;
* normalize;
* filtering;
* scoring;
* risk analysis;
* persistence;
* постановку уведомлений.

## PostgreSQL

Persistent storage.

## Redis

Использовать только если он реально нужен.

Перед добавлением Redis отдельно обоснуй каждое использование:

```text
queue
lock
cache
temporary state
rate limiting
```

Если PostgreSQL + asyncio достаточно для конкретного компонента на текущей фазе — не добавляй Redis только ради архитектуры.

---

# 25. Telegram мониторинг

Нужно поддерживать большое количество Telegram-каналов.

Для каждого источника храни:

* username;
* channel_id;
* title;
* active;
* last_processed_message_id;
* last_check;
* last_success;
* historical_sync_state.

Поддерживай:

* live monitoring;
* historical import;
* checkpoint;
* FloodWait handling;
* retry;
* backoff;
* rate limit;
* ошибки авторизации.

---

# 26. Сайты

Планируемые типы источников:

* HH.ru
* Kwork
* FL.ru
* Workzilla
* другие сайты вакансий и подработок.

Перед реализацией каждого adapter:

1. Проанализируй способ получения данных.
2. Проверь наличие официального API.
3. Проверь RSS/sitemap/public endpoints, если применимо.
4. Если требуется HTTP parsing — используй его.
5. Если нужен JavaScript — используй Playwright.
6. Не выдумывай несуществующие API.

Если полноценная интеграция невозможна:

```python
SourceNotSupportedError
```

или соответствующий статус.

---

# 27. Bot commands

Минимально:

```text
/start
/help

/settings

/sources
/add_source
/remove_source
/list_sources

/status
/stats

/search
/recent

/test
```

Также можно использовать inline keyboards.

---

# 28. Source management

Пользователь должен иметь возможность создавать экземпляры источников без изменения кода.

Например:

```text
/add_source telegram @channel
```

После этого создается Source Instance.

Для сайтов:

```text
/add_source hh <url>
```

Но команда должна работать с уже существующим adapter.

Она НЕ должна создавать новый Python-класс.

---

# 29. Search

Команда:

```text
/search контент менеджер
```

должна искать только по уже сохраненным данным PostgreSQL.

Она НЕ должна автоматически запускать внешний парсинг сайтов.

Результаты сортировать по:

1. relevance;
2. published_at;
3. при необходимости similarity.

---

# 30. Настройка фильтров через Telegram

Команда:

```text
/settings
```

должна позволять менять:

```text
🔎 Include keywords
🚫 Exclude keywords

🏠 Remote only
⏰ Flexible schedule
📞 Exclude calls
✈️ Exclude business trips

💰 Minimum salary

⭐ Minimum relevance score
⚠️ Maximum risk level

🕐 Freshness limit

🌙 Quiet hours
🌍 Timezone
```

Hard reject правила не должны случайно превращаться в soft penalty.

---

# 31. Модель вакансии

Минимальная структура:

```text
id
source_type
source_instance_id
external_id

source_url
title
description
company

salary_min
salary_max
salary_currency
payment_type
salary_period
salary_raw

employment_type
schedule
remote
location

published_at
collected_at

contact
skills
tags

raw_text
normalized_text

content_hash
normalized_hash

relevance_score
risk_score
risk_level

status
```

При необходимости добавь дополнительные поля.

---

# 32. PostgreSQL schema

Спроектируй нормальную схему, например:

```text
users
sources
source_instances
source_runs
vacancies
vacancy_sources
processing_results
processing_flags
notifications
notification_attempts
filter_rules
user_settings
```

Измени структуру, если предложишь более правильную модель.

Добавь индексы для:

* source_instance_id;
* external_id;
* published_at;
* collected_at;
* content_hash;
* relevance_score;
* risk_score;
* status.

Продумай unique constraints.

---

# 33. Статистика

Команда:

```text
/stats
```

должна показывать:

```text
Всего источников: 126
Активных: 118

Telegram: 84
HH: 20
Kwork: 8
FL: 5
Workzilla: 3
Другие: 6

Всего вакансий: 382421
Найдено сегодня: 1281
Подходящих: 146
Отправлено: 93
Дубликатов: 624
Hard rejected: 412
High risk: 31
```

---

# 34. Производительность

Используй:

* async HTTP;
* connection pooling;
* bounded concurrency;
* semaphore;
* pagination;
* batch inserts;
* batch processing;
* database indexes;
* retry;
* exponential backoff;
* timeouts.

Не создавай неограниченное количество asyncio tasks.

Один источник не должен блокировать остальные.

---

# 35. Ошибки

Обрабатывай:

* timeout;
* connection error;
* HTTP 4xx/5xx;
* parsing errors;
* invalid source;
* authentication errors;
* Telegram FloodWait;
* database errors;
* scheduler errors;
* Playwright errors.

Ошибка одного источника не должна останавливать весь pipeline.

---

# 36. Logging

Логируй:

```text
DEBUG
INFO
WARNING
ERROR
CRITICAL
```

Отслеживай:

* parser run;
* source status;
* количество fetched;
* количество parsed;
* количество duplicates;
* hard rejects;
* approved;
* notifications;
* errors;
* processing time.

Никогда не записывай токены и секреты в логи.

---

# 37. Конфигурация

Используй `.env`:

```env
BOT_TOKEN=
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
DATABASE_URL=
REDIS_URL=
```

Создай:

```text
.env.example
```

Не хранить секреты в исходном коде.

Пользовательские фильтры не должны требовать изменения Python.

---

# 38. Docker

Создай Dockerfile и docker-compose.

На этапе MVP допустимо меньше контейнеров, если это архитектурно оправдано.

Не создавай отдельный Redis/worker/scheduler container только ради количества контейнеров.

Но архитектурно код должен позволять разделить:

```text
bot
scheduler
worker
postgres
redis
```

позже.

---

# 39. Тестирование

Обязательно тестируй:

## Unit

* normalization;
* salary parsing;
* keyword matching;
* hard filters;
* soft penalties;
* relevance scoring;
* risk scoring;
* freshness;
* deduplication;
* lifecycle;
* Telegram formatting.

## Integration

* PostgreSQL repositories;
* transaction boundaries;
* unique constraints;
* notification idempotency;
* scheduler → queue;
* worker → database.

Не делай реальные запросы к внешним сайтам в unit tests.

Используй mocks/fixtures.

---

# 40. Архитектура проекта

Предложи профессиональную структуру примерно:

```text
job_aggregator/
│
├── app/
│   ├── bot/
│   │   ├── handlers/
│   │   ├── keyboards/
│   │   ├── middlewares/
│   │   └── states/
│   │
│   ├── sources/
│   │   ├── base.py
│   │   ├── registry.py
│   │   ├── telegram.py
│   │   ├── hh.py
│   │   ├── kwork.py
│   │   ├── fl.py
│   │   └── workzilla.py
│   │
│   ├── pipeline/
│   │   ├── fetch.py
│   │   ├── parse.py
│   │   ├── normalize.py
│   │   ├── validate.py
│   │   ├── deduplicate.py
│   │   ├── filtering.py
│   │   ├── scoring.py
│   │   ├── risk.py
│   │   └── notification.py
│   │
│   ├── salary/
│   ├── normalizer/
│   ├── filters/
│   ├── scoring/
│   ├── risk_engine/
│   ├── database/
│   ├── models/
│   ├── services/
│   ├── workers/
│   ├── scheduler/
│   ├── config/
│   └── utils/
│
├── tests/
├── migrations/
├── scripts/
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
└── README.md
```

Можешь изменить структуру, если предложишь технически более правильную.

---

# 41. Фазы разработки

Не пытайся реализовать все возможности одновременно.

Работай поэтапно.

Каждая фаза должна быть полностью рабочей и запускаемой.

## Phase 1 — MVP

Реализовать:

* Telegram monitoring;
* Telegram bot;
* PostgreSQL;
* source instances;
* normalization;
* basic filtering;
* hard reject;
* relevance scoring;
* deduplication;
* lifecycle;
* notification queue;
* basic scheduler;
* settings;
* tests;
* Docker;
* README.

В Phase 1 не нужно добавлять сложный distributed orchestration без необходимости.

## Phase 2 — HH.ru

Добавить полноценный HH adapter.

Проверить реальный доступный способ получения данных перед реализацией.

## Phase 3 — Kwork / FL.ru / Workzilla

Добавить новые adapters по той же архитектуре.

## Phase 4 — Scaling

Только после работающего MVP:

* отдельные workers;
* Redis при реальной необходимости;
* distributed locks;
* advanced queue;
* более глубокий concurrency control;
* масштабирование обработки.

## Phase 5 — Advanced processing

Добавить:

* advanced risk engine;
* similarity matching;
* улучшенный salary analysis;
* advanced analytics;
* более сложные рекомендации/фильтры.

Не реализуй Phase 5 раньше, чем Phase 1 стабильно работает.

---

# 42. Как должен работать итоговый сценарий

Пример:

```text
Telegram channel posts vacancy
        ↓
Telegram adapter
        ↓
parse
        ↓
normalize
        ↓
validate
        ↓
deduplicate
        ↓
hard filters
        ↓
structural extraction
        ↓
relevance score
        ↓
risk engine
        ↓
save to PostgreSQL
        ↓
notification decision
        ↓
notification queue
        ↓
Telegram bot
```

Если вакансия не подходит:

```text
save
↓
status = FILTERED / BLOCKED
↓
no notification
```

Если подходит:

```text
save
↓
status = APPROVED
↓
notification queue
↓
send
↓
status = SENT
```

---

# 43. Формат Telegram-сообщения

Пример:

```text
🔥 Новая вакансия

Контент-менеджер / AI-контент

💰 50 000–80 000 ₽/месяц
🏠 Удалённо
⏰ Гибкий график
📌 Частичная занятость

⭐ Подходящесть: 87/100
⚠️ Риск: LOW

Почему подходит:
• удалённая
• гибкий график
• работа с текстами
• частичная занятость

Минусы:
• требуется опыт от 1 года

Источник: HH.ru
```

Кнопки:

```text
🔗 Открыть вакансию
👍 Подходит
👎 Не подходит
🚫 Скрыть источник
```

---

# 44. README

README должен содержать:

* установка;
* требования;
* Python version;
* Docker;
* создание Telegram Bot;
* Telegram API ID/API HASH;
* настройка `.env`;
* миграции;
* запуск;
* добавление источников;
* настройка фильтров;
* initial sync;
* live monitoring;
* backfill;
* тестирование;
* troubleshooting.

---

# 45. Важный принцип работы с внешними источниками

Перед реализацией конкретного adapter не выдумывай доступ.

Для каждого источника сначала определи:

```text
официальный API?
RSS?
public endpoint?
HTML?
JavaScript?
нужна авторизация?
есть rate limits?
```

Если способ не подходит:

```text
UNSUPPORTED
```

или:

```text
AUTH_REQUIRED
```

а не фальшивый "рабочий" парсер.

---

# 46. Самопроверка

После реализации обязательно проверь:

* ошибки импортов;
* cyclic dependencies;
* sync/async ошибки;
* SQLAlchemy 2.x;
* aiogram 3.x;
* scheduler;
* transaction boundaries;
* race conditions;
* duplicate processing;
* notification idempotency;
* Telegram errors;
* parser failures;
* retry logic;
* incorrect state transitions.

Запусти тесты.

Исправь найденные ошибки.

Не ограничивайся описанием того, как проект можно сделать.

Сам реализуй рабочий код.

---

# 47. Порядок действий прямо сейчас

Не начинай сразу писать весь код.

Сначала выполни:

### Step 1

Проанализируй требования и выяви противоречия.

### Step 2

Предложи окончательную архитектуру.

### Step 3

Покажи структуру проекта.

### Step 4

Покажи database schema.

### Step 5

Покажи lifecycle/state machine.

### Step 6

Покажи pipeline.

### Step 7

Покажи границы ответственности Bot / Scheduler / Worker / Database / Redis.

### Step 8

Покажи план Phase 1 → Phase 5.

### Step 9

После этого начни реализовывать Phase 1.

Не переходи к следующей фазе, пока предыдущая не имеет рабочего состояния.

При каждом архитектурном решении объясняй не только "что", но и кратко "почему".

Главное правило проекта:

**не усложнять систему без необходимости, но и не жертвовать корректностью ради простоты.**
