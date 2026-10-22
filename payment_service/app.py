import os
import sys
import uuid
import datetime
from typing import Dict, Any, List
from fastapi import FastAPI
from pydantic import BaseModel

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.messaging import EventBus

SERVICE_PORT = int(os.getenv("PORT", "8002"))

app = FastAPI(
    title="Payment Service (Паттерн Saga: Хореография)",
    description="Сервис обработки платежей. Подписывается на событие OrderCreated, валидирует оплату и публикует PaymentCompleted либо PaymentFailed.",
    version="1.0.0"
)

payments_db: List[Dict[str, Any]] = []
event_bus = EventBus(service_name="PaymentService")

def on_order_created(routing_key: str, data: Dict[str, Any]):
    """
    Обработчик события создания заказа OrderCreated.
    Payment Service не знает об Order Service — он лишь реагирует на событие и публикует ответ.
    """
    order_id = data.get("orderId")
    user_id = data.get("userId", "unknown")
    amount = float(data.get("totalAmount", 0.0))
    fail_payment = data.get("failPayment", False)
    now_str = datetime.datetime.utcnow().isoformat() + "Z"

    print(f"\n[PaymentService] ---> Получен заказ на оплату: orderId={order_id}, сумма={amount} руб., userId={user_id}")

    # Детерминированная симуляция ошибки для демонстрации компенсации:
    # если сумма отрицательная или выставлен флаг failPayment
    if amount < 0 or fail_payment:
        reason = "Недостаточно средств на счете клиента" if amount < 0 else "Карта заблокирована банком"
        payment_record = {
            "orderId": order_id,
            "userId": user_id,
            "amount": amount,
            "status": "FAILED",
            "reason": reason,
            "timestamp": now_str
        }
        payments_db.append(payment_record)
        print(f"[PaymentService] ПЛАТЕЖ ОТКЛОНЕН: {reason} для заказа {order_id}")

        # Публикуем событие PaymentFailed в брокер
        event_bus.publish("payment.failed", {
            "orderId": order_id,
            "userId": user_id,
            "amount": amount,
            "reason": reason,
            "failedAt": now_str
        })
    else:
        tx_id = f"tx-{uuid.uuid4().hex[:8]}"
        payment_record = {
            "orderId": order_id,
            "userId": user_id,
            "amount": amount,
            "transactionId": tx_id,
            "status": "COMPLETED",
            "timestamp": now_str
        }
        payments_db.append(payment_record)
        print(f"[PaymentService] ПЛАТЕЖ УСПЕШНО ПРОВЕДЕН: txId={tx_id} для заказа {order_id}")

        # Публикуем событие PaymentCompleted в брокер
        event_bus.publish("payment.completed", {
            "orderId": order_id,
            "userId": user_id,
            "amount": amount,
            "transactionId": tx_id,
            "completedAt": now_str
        })

@app.on_event("startup")
def startup():
    event_bus.connect()
    # Подписываемся на события order.created
    event_bus.subscribe(
        queue_name="payment_service_queue",
        routing_keys=["order.created"],
        callback=on_order_created
    )

class InternalEvent(BaseModel):
    routing_key: str
    data: Dict[str, Any]

@app.post("/internal/events", include_in_schema=False)
def receive_internal_event(event: InternalEvent):
    event_bus.handle_incoming_event(event.routing_key, event.data)
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "payment-service", "processedCount": len(payments_db)}

@app.get("/payments", summary="Получение реестра транзакций оплаты")
def get_payments():
    return payments_db

if __name__ == "__main__":
    import uvicorn
    print(f"Запуск Payment Service на порту {SERVICE_PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
