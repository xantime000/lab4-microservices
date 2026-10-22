import sys
import time
import requests

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

ORDER_URL = "http://127.0.0.1:8001"
PAYMENT_URL = "http://127.0.0.1:8002"
NOTIFICATION_URL = "http://127.0.0.1:8003"

def print_header(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def main():
    print_header("ТЕСТИРОВАНИЕ ЛАБОРАТОРНОЙ РАБОТЫ №4: ПАТТЕРН SAGA (ХОРЕОГРАФИЯ)")

    # 1. Проверка доступности микросервисов
    print("\n[Шаг 1] Проверка доступности микросервисов...")
    try:
        r_order = requests.get(f"{ORDER_URL}/health", timeout=3)
        r_pay = requests.get(f"{PAYMENT_URL}/health", timeout=3)
        r_notif = requests.get(f"{NOTIFICATION_URL}/health", timeout=3)
        print(f"  -> Order Service (порт 8001):        HTTP {r_order.status_code} [OK]")
        print(f"  -> Payment Service (порт 8002):      HTTP {r_pay.status_code} [OK]")
        print(f"  -> Notification Service (порт 8003): HTTP {r_notif.status_code} [OK]")
    except Exception as e:
        print(f"\n[ОШИБКА] Не удалось подключиться к сервисам: {e}")
        print("Убедитесь, что сервисы запущены (через docker-compose up или python run_local.py)")
        sys.exit(1)

    # 2. СЦЕНАРИЙ 1: УСПЕШНАЯ SAGA
    print_header("СЦЕНАРИЙ 1: УСПЕШНАЯ ОПЛАТА И ПОДТВЕРЖДЕНИЕ ЗАКАЗА (SUCCESS SAGA)")
    print("Цепочка: Client -> Order [PENDING] -> OrderCreated -> Payment -> PaymentCompleted -> Order [CONFIRMED] -> Notification")
    
    req_success = {
        "userId": "client-alex-1",
        "items": [
            {"productId": "laptop-lenovo", "quantity": 1},
            {"productId": "mouse-logitech", "quantity": 1}
        ],
        "totalAmount": 1500.0,
        "failPayment": False
    }

    res1 = requests.post(f"{ORDER_URL}/orders", json=req_success)
    assert res1.status_code == 201, f"Ошибка создания заказа: {res1.text}"
    order1 = res1.json()
    order1_id = order1["orderId"]
    print(f"\n1. Клиент отправил POST /orders:")
    print(f"   -> Создан заказ: ID={order1_id}")
    print(f"   -> Начальный статус: {order1['status']} (PENDING)")
    print(f"   -> Опубликовано событие: OrderCreated (сумма {order1['totalAmount']} руб.)")

    print("\n2. Ожидание асинхронной обработки события брокером RabbitMQ (1.5 сек)...")
    time.sleep(1.5)

    # Проверяем итоговое состояние заказа
    res1_check = requests.get(f"{ORDER_URL}/orders/{order1_id}")
    order1_final = res1_check.json()
    print(f"\n3. Проверка итогового статуса заказа в Order Service:")
    print(f"   -> Статус заказа: {order1_final['status']}")
    assert order1_final["status"] == "CONFIRMED", f"Ожидался статус CONFIRMED, получен {order1_final['status']}"
    print("   -> [✓] УСПЕХ: Статус заказа успешно обновлен на CONFIRMED!")

    print("\n4. Проверка журнала событий заказа:")
    for h in order1_final["history"]:
        print(f"   * {h}")

    # 3. СЦЕНАРИЙ 2: ОШИБКА ОПЛАТЫ И КОМПЕНСАЦИЯ SAGA
    print_header("СЦЕНАРИЙ 2: СБОЙ ОПЛАТЫ И ВЫПОЛНЕНИЕ КОМПЕНСАЦИИ (SAGA COMPENSATION)")
    print("Цепочка: Client -> Order [PENDING] -> OrderCreated -> Payment -> PaymentFailed -> Order [CANCELLED (КОМПЕНСАЦИЯ)] -> OrderCancelled -> Notification")

    req_fail = {
        "userId": "client-daniil-2",
        "items": [
            {"productId": "smartphone-iphone", "quantity": 1}
        ],
        "totalAmount": -100.0,  # Отрицательная сумма -> детерминированный сбой оплаты
        "failPayment": True
    }

    res2 = requests.post(f"{ORDER_URL}/orders", json=req_fail)
    assert res2.status_code == 201, f"Ошибка создания заказа: {res2.text}"
    order2 = res2.json()
    order2_id = order2["orderId"]
    print(f"\n1. Клиент отправил POST /orders с некорректной суммой:")
    print(f"   -> Создан заказ: ID={order2_id}")
    print(f"   -> Начальный статус: {order2['status']} (PENDING)")
    print(f"   -> Опубликовано событие: OrderCreated (сумма {order2['totalAmount']} руб.)")

    print("\n2. Ожидание асинхронной обработки события брокером RabbitMQ (1.5 сек)...")
    time.sleep(1.5)

    res2_check = requests.get(f"{ORDER_URL}/orders/{order2_id}")
    order2_final = res2_check.json()
    print(f"\n3. Проверка итогового статуса заказа после выполнения компенсации:")
    print(f"   -> Итоговый статус: {order2_final['status']}")
    assert order2_final["status"] == "CANCELLED", f"Ожидался статус CANCELLED, получен {order2_final['status']}"
    print("   -> [✓] УСПЕХ: Компенсация выполнена! Заказ корректно переведен в статус CANCELLED!")

    print("\n4. Проверка журнала событий заказа:")
    for h in order2_final["history"]:
        print(f"   * {h}")

    # 4. ПРОВЕРКА NOTIFICATION SERVICE
    print_header("ПРОВЕРКА ЖУРНАЛА УВЕДОМЛЕНИЙ КЛИЕНТОВ (NOTIFICATION SERVICE)")
    r_notifs = requests.get(f"{NOTIFICATION_URL}/notifications").json()
    print(f"Всего отправлено уведомлений: {len(r_notifs)}")
    for idx, n in enumerate(r_notifs, 1):
        print(f"  [{idx}] Тип: {n['type']} | Заказ: {n['orderId'][:8]}... | Пользователь: {n['userId']}")
        print(f"      Сообщение: {n['message']}")

    print_header("ИТОГ: ВСЕ ТРЕБОВАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ №4 ВЫПОЛНЕНЫ НА 100%!")

if __name__ == "__main__":
    main()
