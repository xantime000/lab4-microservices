# Лабораторная работа №4: Сервис заказов и паттерн Saga (асинхронное взаимодействие)

Проект представляет собой распределенную микросервисную систему, реализующую паттерн распределенных транзакций Saga на основе хореографии событий (Choreography) с использованием брокера сообщений RabbitMQ.

## Архитектура системы

В состав системы входят брокер RabbitMQ и 3 независимых микросервиса:
- `order_service/` — сервис заказов: создает заказы со статусом `PENDING`, публикует событие `OrderCreated`, обрабатывает события оплаты `PaymentCompleted` (перевод в `CONFIRMED`) и `PaymentFailed` (выполнение компенсирующей транзакции: перевод в `CANCELLED` и публикация `OrderCancelled`).
- `payment_service/` — сервис оплаты: слушает `OrderCreated`, валидирует платеж и публикует `PaymentCompleted` либо `PaymentFailed`.
- `notification_service/` — сервис уведомлений: слушает `PaymentCompleted` и `OrderCancelled`, логирует и отправляет уведомления клиентам.
- `common/` — общий модуль для работы с AMQP-брокером сообщений.

## Взаимодействие через события (Saga Choreography)

1. **Успешный сценарий:**
   - Клиент: `POST /orders` (сумма 1500 руб.)
   - Order Service: статус `PENDING`, публикует `order.created` (OrderCreated)
   - Payment Service: получает событие, списывает средства, публикует `payment.completed` (PaymentCompleted)
   - Order Service: обновляет статус заказа на `CONFIRMED`
   - Notification Service: отправляет клиенту уведомление об успешной оплате

2. **Сценарий с ошибкой и компенсацией:**
   - Клиент: `POST /orders` (некорректная сумма / отказ банка)
   - Order Service: статус `PENDING`, публикует `order.created` (OrderCreated)
   - Payment Service: отклоняет платеж, публикует `payment.failed` (PaymentFailed)
   - Order Service (компенсирующая транзакция): переводит заказ в статус `CANCELLED`, публикует `order.cancelled` (OrderCancelled)
   - Notification Service: отправляет клиенту уведомление об отмене заказа

## Требования

- Python 3.10+
- Зависимости: `fastapi`, `uvicorn`, `pika`, `pydantic`, `requests`
- (Опционально) Docker и Docker Compose

## Запуск

### Вариант 1. Через Docker Compose (с брокером RabbitMQ)

```bash
docker compose up --build
```

Доступные адреса:
- Веб-дашборд Order Service: http://localhost:8001/
- OpenAPI Swagger Order Service: http://localhost:8001/docs
- OpenAPI Swagger Payment Service: http://localhost:8002/docs
- OpenAPI Swagger Notification Service: http://localhost:8003/docs
- RabbitMQ Management UI: http://localhost:15672/ (логин: `guest`, пароль: `guest`)

Автоматическое тестирование в отдельном терминале:
```bash
python test_saga.py
```

### Вариант 2. Локальный запуск (без Docker)

```bash
python run_local.py
```

Запуск автотеста:
```bash
python test_saga.py
```
