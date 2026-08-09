# Bankovskoe

Учебный сервис для обработки платежей: HTTP API на Go, хранение в PostgreSQL,
события платежей в Kafka и простой фронтенд в `index.html`.

## Что внутри

- `cmd/app/main.go` - точка входа приложения.
- `internal/api` - HTTP handlers.
- `internal/service` - бизнес-логика платежей и каскадирование по банкам.
- `internal/storage` - работа с PostgreSQL.
- `internal/kafka` - producer и listener для Kafka.
- `migrations/001_init.sql` - схема таблиц.
- `docker-compose.yml` - Postgres, Zookeeper, Kafka, Kafka UI и Go-приложение.
- `index.html` - простой UI для ручного тестирования.

## Быстрый запуск

Поднять инфраструктуру и приложение:

```bash
docker compose up -d
```

Проверить контейнеры:

```bash
docker compose ps
```

Остановить:

```bash
docker compose down
```

Если нужно удалить данные Postgres вместе с контейнерами:

```bash
docker compose down -v
```

## Адреса

- Backend API: `http://localhost:8090`
- Kafka bootstrap для Go с хоста: `localhost:9092`
- Kafka bootstrap внутри Docker-сети: `kafka:29092`
- Kafka UI: `http://localhost:8081`
- PostgreSQL: `localhost:5432`

## API

Создать платеж:

```bash
curl -X POST http://localhost:8090/payments \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: test-key-1" \
  -d "{\"amount\":1000}"
```

Получить платеж с попытками:

```bash
curl http://localhost:8090/payments/1
```

Повторный `POST /payments` с тем же `X-Idempotency-Key` вернет уже созданный
платеж и не запустит повторную обработку.

## Kafka

Topic для платежных событий:

```text
payment_events
```

После успешной обработки нового платежа приложение публикует событие в Kafka.
Listener в Go-приложении читает тот же topic и пишет сообщения в консоль:

```text
[kafka] published payment event: ...
[kafka] received topic=payment_events ...
```

Kafka можно смотреть через UI:

```text
http://localhost:8081
```

## Переменные окружения

`DATABASE_URL`

PostgreSQL DSN. По умолчанию для локального запуска без Docker:

```text
postgres://bankovskoe:bankovskoe@localhost:5432/bankovskoe?sslmode=disable
```

`KAFKA_BROKERS`

Список Kafka broker'ов через запятую. По умолчанию:

```text
localhost:9092
```

`KAFKA_DISABLED`

Отключает Kafka producer/listener, если значение `true` или `1`.

## Локальный запуск без контейнера app

Можно поднять только зависимости:

```bash
docker compose up -d postgres zookeeper kafka kafka-ui
```

Потом запустить Go-приложение с хоста:

```bash
go run ./cmd/app
```

## Тесты

Интеграционные тесты требуют PostgreSQL и переменную `INTEGRATION_DATABASE_URL`:

```bash
$env:INTEGRATION_DATABASE_URL="postgres://bankovskoe:bankovskoe@localhost:5432/bankovskoe?sslmode=disable"
go test ./...
```

Без `INTEGRATION_DATABASE_URL` интеграционные тесты будут пропущены.
