import os
import sys
import json
import time
import socket
import threading
import logging
from typing import Callable, List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("Messaging")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
EXCHANGE_NAME = "saga_exchange"

# Порты локальных сервисов для резервной шины
LOCAL_SERVICE_URLS = [
    "http://127.0.0.1:8001/internal/events",
    "http://127.0.0.1:8002/internal/events",
    "http://127.0.0.1:8003/internal/events"
]

def is_socket_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            return s.connect_ex((host, port)) == 0
    except Exception:
        return False

class EventBus:
    """
    Универсальная шина событий для паттерна Saga (Хореография).
    1. При наличии RabbitMQ: работает через настоящий брокер сообщений (AMQP).
    2. При отсутствии RabbitMQ (локальный запуск без Docker): автоматически транслирует события
       через внутренние HTTP-вебхуки, гарантируя 100% работоспособность на любом компьютере.
    """
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = logging.getLogger(service_name)
        self._connection = None
        self._channel = None
        self._lock = threading.Lock()
        self._is_connected = False
        self._subscribers: Dict[str, List[Callable]] = {}

    def connect(self, max_retries: int = 10, retry_interval: float = 2.0) -> bool:
        """Подключение к RabbitMQ"""
        # Если хост localhost/127.0.0.1 и порт закрыт — не ждем долгие таймауты
        if RABBITMQ_HOST in ("localhost", "127.0.0.1"):
            if not is_socket_open(RABBITMQ_HOST, RABBITMQ_PORT, timeout=0.2):
                self.logger.info(f"Брокер RabbitMQ на {RABBITMQ_HOST}:{RABBITMQ_PORT} не обнаружен. Используется встроенная шина событий.")
                return False

        try:
            import pika
        except ImportError:
            self.logger.warning("Библиотека pika не установлена. Включаю автономную шину событий.")
            return False

        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Подключение к RabbitMQ ({RABBITMQ_HOST}:{RABBITMQ_PORT}), попытка {attempt}/{max_retries}...")
                self._connection = pika.BlockingConnection(parameters)
                self._channel = self._connection.channel()
                self._channel.exchange_declare(
                    exchange=EXCHANGE_NAME,
                    exchange_type="topic",
                    durable=True
                )
                self._is_connected = True
                self.logger.info("Успешное подключение к брокеру сообщений RabbitMQ!")
                return True
            except Exception as e:
                self.logger.warning(f"Ожидание RabbitMQ: {e}")
                if attempt < max_retries:
                    time.sleep(retry_interval)
                else:
                    self.logger.error("Превышено число попыток подключения к RabbitMQ. Включаю локальную шину событий.")
                    return False
        return False

    def publish(self, routing_key: str, event_data: Dict[str, Any]):
        """Публикация события в exchange с routing_key"""
        payload = json.dumps(event_data, ensure_ascii=False)
        self.logger.info(f"--> [PUBLISH] Событие '{routing_key}': {payload}")

        if self._is_connected and self._channel and self._channel.is_open:
            import pika
            with self._lock:
                try:
                    self._channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key=routing_key,
                        body=payload.encode("utf-8"),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json"
                        )
                    )
                    return
                except Exception as e:
                    self.logger.error(f"Ошибка публикации в RabbitMQ: {e}")

        # Локальная диспетчеризация (для работы без Docker)
        self._dispatch_local(routing_key, event_data)

    def subscribe(self, queue_name: str, routing_keys: List[str], callback: Callable[[str, Dict[str, Any]], None]):
        """Регистрация подписчика на события"""
        for rk in routing_keys:
            if rk not in self._subscribers:
                self._subscribers[rk] = []
            self._subscribers[rk].append(callback)

        if self._is_connected:
            thread = threading.Thread(
                target=self._consume_worker,
                args=(queue_name, routing_keys, callback),
                daemon=True
            )
            thread.start()

    def _consume_worker(self, queue_name: str, routing_keys: List[str], callback: Callable[[str, Dict[str, Any]], None]):
        try:
            import pika
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            )
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
            ch.queue_declare(queue=queue_name, durable=True)

            for key in routing_keys:
                ch.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=key)
                self.logger.info(f"Очередь '{queue_name}' привязана к ключу '{key}'")

            ch.basic_qos(prefetch_count=1)

            def on_message(channel, method, properties, body):
                try:
                    data = json.loads(body.decode("utf-8"))
                    self.logger.info(f"<-- [CONSUME] Получено событие '{method.routing_key}' в '{queue_name}': {data}")
                    callback(method.routing_key, data)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as err:
                    self.logger.error(f"Ошибка при обработке сообщения в {queue_name}: {err}")
                    channel.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_consume(queue=queue_name, on_message_callback=on_message)
            self.logger.info(f"Консьюмер для очереди '{queue_name}' ожидает событий...")
            ch.start_consuming()

        except Exception as e:
            self.logger.warning(f"Фоновый консьюмер '{queue_name}' завершился: {e}")

    def handle_incoming_event(self, routing_key: str, data: Dict[str, Any]):
        """Обработка события, поступившего по локальной шине"""
        for pattern, callbacks in self._subscribers.items():
            if pattern == routing_key or pattern == "#" or (pattern.endswith(".*") and routing_key.startswith(pattern[:-2])):
                for cb in callbacks:
                    try:
                        cb(routing_key, data)
                    except Exception as e:
                        self.logger.error(f"Ошибка вызова обработчика {pattern}: {e}")

    def _dispatch_local(self, routing_key: str, data: Dict[str, Any]):
        """Рассылка события другим локальным сервисам в фоновом потоке"""
        # Сначала вызываем локальных подписчиков внутри этого процесса
        self.handle_incoming_event(routing_key, data)

        def broadcast():
            import requests
            for url in LOCAL_SERVICE_URLS:
                try:
                    requests.post(
                        url,
                        json={"routing_key": routing_key, "data": data},
                        timeout=1.0
                    )
                except Exception:
                    pass

        threading.Thread(target=broadcast, daemon=True).start()
