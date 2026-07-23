# Audit Service

Микросервис аудита пользовательских действий на Go, PostgreSQL и Apache Kafka.
Событие сохраняется в базе для быстрого чтения и одновременно публикуется в
Kafka с ключом `user_id`, чтобы сохранить порядок действий одного пользователя.

## Архитектура

```text
POST /api/audit
    ├── PostgreSQL: audit_log
    └── Kafka: user-actions (3 partitions, key=user_id)
                           │
                           ▼
                    analytics-group
                           │
                           ▼
                     stats_cache

GET  /api/audit ──────────────── PostgreSQL
GET  /api/stats ──────────────── PostgreSQL
POST /api/admin/rebuild-stats ── Kafka replay ── stats_cache
```

Kafka работает в режиме KRaft без ZooKeeper. Docker Compose автоматически
создаёт топик `user-actions` с тремя партициями и применяет SQL-миграцию.

## Требования

- Go 1.25 или новее;
- Docker Desktop;
- свободные порты `8080`, `9092` и `5433`.

Порт `5433` на хосте пробрасывается в стандартный PostgreSQL-порт `5432`
внутри контейнера.

## Запуск

1. Запустить инфраструктуру:

```bash
docker compose up -d
docker compose ps -a
```

Ожидаем:

- `kafka` и `postgres` имеют статус `healthy`;
- `kafka-init` и `postgres-init` завершились с кодом `0`.

2. Запустить приложение:

```bash
go run ./cmd/audit-service
```

3. Проверить:

```bash
curl http://localhost:8080/health
```

Ответ:

```json
{"status":"ok"}
```

Swagger UI доступен по адресу:

```text
http://localhost:8080/swagger/
```

Исходная OpenAPI-спецификация:

```text
http://localhost:8080/swagger/openapi.yaml
```

## API

### Записать событие

```bash
curl -X POST http://localhost:8080/api/audit \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-1","action":"view","resource_id":"product-42","meta":{"source":"catalog"}}'
```

Допустимые actions: `login`, `view`, `purchase`. Поля `user_id`, `action` и
`resource_id` обязательны, `meta` по умолчанию равно `{}`.

Ответ:

```json
{
  "event_id": "6f8a13f8-bf05-4958-a574-e6e541f4bcad",
  "timestamp": "2026-07-23T10:00:00Z"
}
```

### История пользователя

```bash
curl "http://localhost:8080/api/audit?user_id=user-1&action=view&from=2026-07-01&to=2026-07-31&page=1&limit=50"
```

Параметры `action`, `from` и `to` необязательны. Даты имеют формат
`YYYY-MM-DD`. `page` по умолчанию равен `1`, `limit` — `50`, максимум — `100`.

### Статистика из PostgreSQL

По типам действий:

```bash
curl "http://localhost:8080/api/stats?user_id=user-1&group_by=action"
```

По UTC-дням:

```bash
curl "http://localhost:8080/api/stats?user_id=user-1&group_by=day"
```

В потоковой архитектуре такую агрегацию можно поддерживать через Kafka Streams
или ksqlDB в materialized view. По условиям задания endpoint читает данные из
PostgreSQL.

### Replay Kafka

```bash
curl -X POST "http://localhost:8080/api/admin/rebuild-stats?from=2026-07-01&to=2026-07-31"
```

Replay получает временные offsets каждой партиции, перечитывает события
отдельным consumer без `group.id` и заменяет данные `stats_cache`. Committed
offsets группы `analytics-group` при этом не изменяются.

## Consumer и offsets

Consumer работает в группе `analytics-group`:

1. Читает и логирует `key`, `partition`, `offset` и `event_id`.
2. Раз в пять минут или при достижении batch size пересчитывает действия за
   последний час.
3. Сохраняет `stats_cache` в транзакции PostgreSQL.
4. Только после успешного commit транзакции вызывает `MarkMessage` и ручной
   `Commit`.

Автоматический commit также включён с интервалом пять секунд, но сообщение
становится доступным для commit только после `MarkMessage`. При перезапуске
чтение продолжается со следующего committed offset.

Sarama вызывает `Setup` после назначения партиций и `Cleanup` перед их отзывом;
оба события логируются для наблюдения за rebalance.

## Конфигурация

| Переменная | Значение по умолчанию |
|---|---|
| `HTTP_ADDR` | `:8080` |
| `DATABASE_URL` | `postgres://audit:audit@127.0.0.1:5433/audit?sslmode=disable` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `user-actions` |
| `KAFKA_GROUP_ID` | `analytics-group` |
| `KAFKA_BATCH_SIZE` | `100` |
| `KAFKA_COMMIT_INTERVAL` | `5s` |
| `ANALYTICS_INTERVAL` | `5m` |
| `SHUTDOWN_PERIOD` | `10s` |

Пример находится в `.env.example`. Приложение читает именно environment
variables; `.env` автоматически не загружается и не должен попадать в Git.

## Тесты

Быстрая сборка всех пакетов:

```bash
go test ./...
```

Интеграционный тест producer → Kafka → consumer:

```bash
go test -tags=integration ./tests/integration -v -count=1
```

Testcontainers автоматически:

1. Поднимает отдельную Kafka в KRaft-режиме.
2. Создаёт топик с тремя партициями.
3. Отправляет событие нашим producer.
4. Читает его consumer и проверяет key, event ID и payload.
5. Удаляет временный контейнер.

## Структура

```text
cmd/audit-service       точка запуска
internal/config         environment-конфигурация
internal/domain         модели предметной области
internal/handler        HTTP и Swagger
internal/service        валидация и бизнес-сценарии
internal/repository     PostgreSQL-запросы
internal/producer       Kafka producer
internal/consumer       analytics consumer group
internal/replay         независимое перечитывание Kafka
migrations              схема PostgreSQL
tests/integration       testcontainers-тест
```

## Полезные команды

```bash
docker compose logs -f kafka
docker compose logs -f postgres
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic user-actions
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --group analytics-group
docker compose down
```

`docker compose down` удаляет контейнеры и сеть, но сохраняет PostgreSQL volume.
Для полного удаления данных требуется отдельная команда `docker compose down -v`.
