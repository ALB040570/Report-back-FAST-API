Report-back-FAST-API

Backend сервис отчётов (FastAPI, Docker-дистрибутив)

Назначение

Сервис предоставляет backend для отчётного модуля.
Разворачивается отдельно от существующего backend и подключается через Nginx.

Рекомендуемый маршрут в Nginx:
/api/report-fast

Состав дистрибутива

report-back-fast-api_1.0.0.tar — Docker-образ сервиса

.env.example — шаблон переменных окружения

(опционально) README.md — эта инструкция

Требования

Docker (Docker Engine / Docker Desktop)

Nginx (для проксирования, если используется)

Local setup + Running tests from scratch

rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
export NO_NETWORK=1
python -m unittest discover -s tests

Установка и запуск

1. Загрузить Docker-образ
   docker load -i report-back-fast-api_1.0.0.tar

Проверка:

docker images | grep report-back-fast-api

2. Подготовить переменные окружения

Скопировать шаблон и заполнить значения:

cp .env.example .env

Пример .env:
REPORT_FILTERS_MAX_VALUES=200
REPORT_FILTERS_CACHE_TTL=30
REPORT_FILTERS_CACHE_MAX=20
REPORT_MAX_RECORDS=100000
REPORT_REMOTE_ALLOWLIST=77.245.107.213
REPORT_DEBUG_FILTERS=0
REPORT_DEBUG_JOINS=0
CORS_ALLOW_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
REDIS_URL=redis://localhost:6379/0
BATCH_CONCURRENCY=5
BATCH_MAX_ITEMS=100
BATCH_JOB_TTL_SECONDS=3600
BATCH_RESULTS_TTL_SECONDS=3600
UPSTREAM_BASE_URL=http://77.245.107.213
# UPSTREAM_URL=http://77.245.107.213/dtj/api/plan
UPSTREAM_TIMEOUT=30
UPSTREAM_ALLOWLIST=77.245.107.213

Пример .env.production (backend):
UPSTREAM_BASE_URL=http://77.245.107.213
UPSTREAM_ALLOWLIST=77.245.107.213
REPORT_REMOTE_ALLOWLIST=77.245.107.213
# CORS_ALLOW_ORIGINS=https://your-frontend-domain
ASYNC_REPORTS=1
# Для быстрых запросов фронт может добавить ?sync=1 или X-Report-Sync: 1.
REPORT_STREAMING_ON_LIMIT=1

3. Запустить контейнер
   docker rm -f report-back-fast-api 2>/dev/null || true

docker run -d \
 --name report-back-fast-api \
 -p 127.0.0.1:8001:8000 \
 --env-file .env \
 report-back-fast-api:1.0.0

Пояснения:

контейнер слушает порт 8000

наружу проброшен на 127.0.0.1:8001

сервис доступен только локально (через Nginx)

Дополнительные переменные:

REPORT_MAX_RECORDS — лимит записей для обработки (0/пусто = без лимита). При превышении возвращается 422.

REPORT_REMOTE_ALLOWLIST — allowlist для абсолютных remoteSource.url (формат как UPSTREAM_ALLOWLIST).
Если не задан, абсолютные URL блокируются, а приватные адреса/localhost запрещены.

REDIS_URL — при задании используется Redis-кэш для записей/фильтров (TTL задаётся REPORT_FILTERS_CACHE_TTL).

BATCH_RESULTS_TTL_SECONDS — TTL для файлов в ./batch_results (автоочистка).

ASYNC_REPORTS — включает асинхронный режим для /api/report/view (0/1). По умолчанию 0.

REPORT_STREAMING — включает потоковый режим построения /api/report/view (0/1). По умолчанию 0.
REPORT_STREAMING_ON_LIMIT — автоматически переключает /api/report/view на streaming при превышении REPORT_MAX_RECORDS (0/1). По умолчанию 1.
REPORT_STREAMING_MAX_RECORDS — лимит записей для streaming-режима (0 = без лимита).

REPORT_CHUNK_SIZE — размер чанка для потоковой агрегации (по умолчанию 1000).

REPORT_STREAMING_MAX_GROUPS — лимит количества групп (row/column) в streaming-режиме (превышение вернёт 422).

REPORT_STREAMING_MAX_UNIQUE_VALUES_PER_DIM — лимит уникальных значений по измерению (0 = без лимита).

REPORT_PAGING_ALLOWLIST — allowlist хостов для paging при REPORT_STREAMING=1 (формат: host1,host2).

REPORT_PAGING_MAX_PAGES — максимальное число страниц при paging (превышение вернёт 422).

REPORT_UPSTREAM_PAGING — dev override для paging без allowlist (0/1).

REPORT_JOIN_LOOKUP_MAX_KEYS — лимит уникальных ключей в lookup для join-источников (streaming-режим).
REPORT_JOIN_MAX_RECORDS — лимит записей после применения joins (0 = без лимита).
REPORT_JOIN_SOURCE_MAX_RECORDS — лимит записей в join-источниках (0 = без лимита).

REPORT_JOB_TTL_SECONDS — TTL для report job и результатов.

REPORT_JOB_MAX_RESULT_BYTES — лимит размера результата (больше лимита пишется в файл).

REPORT_JOBS_DIR — каталог для файлов результатов report jobs (по умолчанию ./report_results).

REPORT_JOB_MAX_CONCURRENCY — максимум параллельных report-задач.

REPORT_JOB_QUEUE_MAX_SIZE — лимит очереди report-задач (при превышении /api/report/view вернёт 429).

REPORT_JOB_POLL_INTERVAL_MS — рекомендованный интервал polling на фронте.

REPORT_UPSTREAM_PUSHDOWN — включает pushdown фильтров/paging в upstream (0/1). По умолчанию 0.

REPORT_PUSHDOWN_ALLOWLIST — allowlist хостов для pushdown (host1,host2).

REPORT_PUSHDOWN_MAX_FILTERS — максимум фильтров для pushdown.

REPORT_PUSHDOWN_MAX_IN_VALUES — максимум значений для IN фильтра в pushdown.

REPORT_PUSHDOWN_SAFE_ONLY — разрешать pushdown только для безопасных фильтров (0/1). По умолчанию 1.

REPORT_PUSHDOWN_OVERRIDE — dev override для pushdown без allowlist (0/1). По умолчанию 0.

CORS

CORS_ALLOW_ORIGINS — список разрешённых origins через запятую.
Если не задан — используется дефолтный список (localhost + 127.0.0.1 + 192.168.1.81).
Если задано `*` — разрешены все origins (осторожно с `allow_credentials=true`).

Профили окружений (рекомендации)

- Dev: ASYNC_REPORTS=0, REPORT_STREAMING_ON_LIMIT=0 (проще дебажить и быстрее получать ошибки).
- Prod: ASYNC_REPORTS=1, REPORT_STREAMING_ON_LIMIT=1 (нет таймаутов, плавный fallback на streaming).

OTEL_ENABLED — включает OpenTelemetry tracing (0/1). По умолчанию 0.

OTEL_SERVICE_NAME — имя сервиса для tracing (по умолчанию report-back-fast-api).

OTEL_EXPORTER_OTLP_ENDPOINT — endpoint для OTLP exporter. Если NO_NETWORK=1, OTLP не используется.

/metrics — endpoint Prometheus метрик (всегда доступен).

Async report mode (Stage 1)

- Включение: ASYNC_REPORTS=1.
- Переменная задаётся только на backend (env контейнера/процесса).
- По умолчанию /api/report/view возвращает 202 + {job_id, status:"queued"}.
- Принудительный синхронный режим (fallback): добавьте query `?sync=1` или заголовок `X-Report-Sync: 1`.
- Статус/результат: GET /api/report/jobs/{job_id}.
- Отмена/удаление: DELETE /api/report/jobs/{job_id}.
- /api/report/filters и /api/report/details остаются синхронными в любом режиме.

Ограничения in-process режима:

- Очередь и статусы не переживают рестарт процесса.
- При нескольких воркерах (multi-process) in-memory очередь/статусы не шарятся.
- Для продакшена рекомендуется Redis job_store и отдельный worker (следующий этап).

Observability

- Метрики: GET /metrics (доступен всегда).
- Tracing в консоль: OTEL_ENABLED=1.
- Tracing + OTLP: OTEL_ENABLED=1 и OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318.
  При NO_NETWORK=1 OTLP отключается (warning) и используется console exporter.

Upstream pushdown (Stage 3B)

- По умолчанию выключен: REPORT_UPSTREAM_PUSHDOWN=0.
- Включение: REPORT_UPSTREAM_PUSHDOWN=1 + REPORT_PUSHDOWN_ALLOWLIST + remoteSource.pushdown.enabled=true.
- Работает только для mode=jsonrpc_params, paging.strategy=offset, ops=eq|in|range.
- Безопасный режим (REPORT_PUSHDOWN_SAFE_ONLY=1) пушдаунит только фильтры из globalFilters и containerFilters.
- Paging pushdown применяется только в streaming-режиме.
- При 4xx/5xx от upstream в pushdown-режиме выполняется 1 retry без pushdown (fallback).
- REPORT_PUSHDOWN_OVERRIDE=1 отключает проверку allowlist (dev-only).

Пример структуры remoteSource.pushdown:

{
  "enabled": true,
  "mode": "jsonrpc_params",
  "paging": {
    "strategy": "offset",
    "limitPath": "body.params.0.limit",
    "offsetPath": "body.params.0.offset"
  },
  "filters": [
    { "filterKey": "objLocation", "op": "eq", "targetPath": "body.params.0.objLocation" }
  ]
}

Вычисляемые поля (computedFields)

В payload /api/report/view, /api/report/filters, /api/report/details можно передать
remoteSource.computedFields. Поля вычисляются после загрузки/joins и до фильтров/пивота,
поэтому могут использоваться в filters/rows/columns/metrics.

Формат:

{
  "remoteSource": {
    "computedFields": [
      {
        "id": "uuid",
        "fieldKey": "is_equal",
        "expression": "(date({{FactDateEnd}}) == date({{UpdatedAt}})) ? 1 : 0",
        "resultType": "number"
      }
    ]
  }
}

Если формула содержит ошибки, значение вычисляемого поля = null,
а предупреждения возвращаются в view.meta.computedWarnings.

4. Проверка работы сервиса
   docker ps
   docker logs --tail 100 report-back-fast-api
   curl -I http://127.0.0.1:8001/docs

Ожидаемый результат:

контейнер в статусе Up

/docs отвечает (HTTP 200)

Swagger UI:

http://127.0.0.1:8001/docs

Batch очереди и воркер

Новые endpoints:

POST /batch — поставить batch в очередь (вернёт job_id)
GET /batch/{job_id} — статус/результаты
DELETE /batch/{job_id} — отмена

Поле `endpoint` в POST /batch обязательно и определяет путь апстрима (например `/dtj/api/plan`).
Если `endpoint` относительный — он будет склеен с `UPSTREAM_BASE_URL` (обязательно).
Полный URL разрешён только при наличии `UPSTREAM_ALLOWLIST`.
Если `endpoint` не задан, то используется `UPSTREAM_URL` только если он явно выставлен.

Пример запроса:

POST /batch
{
  "endpoint": "/dtj/api/plan",
  "method": "POST",
  "sourceId": 1161,
  "params": [
    {"date": "2026-06-01", "periodType": 41, "objLocation": 1069},
    {"date": "2026-07-01", "periodType": 41, "objLocation": 1069}
  ],
  "meta": {"requestId": "frontend-123"}
}

Запуск Redis (локально):

docker run -d --name report-back-redis -p 6379:6379 redis:7

Если REDIS_URL не задан, используется in-memory хранилище (подходит только для dev,
API и воркер должны работать в одном процессе).

Запуск API (локально):

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

Запуск воркера (в отдельном окне):

python -m app.worker

Результаты batch хранятся в Redis до истечения BATCH_JOB_TTL_SECONDS.
Если результаты больше 2 МБ, они сохраняются в файл ./batch_results/{job_id}.json,
а в ответе возвращается summary + resultsFileRef.

Подключение через Nginx

Рекомендуемый конфиг:

location /api/report-fast/ {
proxy_pass http://127.0.0.1:8001/;
proxy_set_header Host $host;
proxy_set_header X-Real-IP $remote_addr;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;
}

Использование во фронте

Во фронтенде используются два backend:

VITE_REPORT_API_BASE=/dtj/api/report
VITE_REPORT_BACKEND_URL=/api/report-fast

/dtj/api/report — существующий backend

/api/report-fast — новый FastAPI backend

Обслуживание

Остановить контейнер:

docker stop report-back-fast-api

Перезапустить:

docker start report-back-fast-api

Удалить контейнер:

docker rm report-back-fast-api

Логи:

docker logs -f report-back-fast-api

Примечания

Docker-образ не содержит .env — переменные передаются только при запуске

Образ версионируется тегами (1.0.0, 1.0.1, …)

Сервис не конфликтует с текущим backend

Запуск через docker-compose (рекомендуется)
docker load -i report-back-fast-api_1.0.0.tar
docker compose up -d

Остановка:

docker compose down

Логи:

docker compose logs -f

3️⃣ Проверка у тебя локально (по желанию)
docker rm -f report-back-fast-api 2>/dev/null || true
docker compose up -d
docker compose logs -f

Swagger:

http://localhost:8001/docs
