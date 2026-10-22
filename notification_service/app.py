import os
import sys
import datetime
from typing import Dict, Any, List
from fastapi import FastAPI

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.messaging import EventBus

SERVICE_PORT = int(os.getenv("PORT", "8003"))

app = FastAPI(
    title="Notification Service (Паттерн Saga: Хореография)",
    description="Сервис уведомлений клиентов. Подписывается на события PaymentCompleted и OrderCancelled и отправляет уведомления.",
    version="1.0.0"
)

notifications_log: List[Dict[str, Any]] = []
event_bus = EventBus(service_name="NotificationService")

def on_saga_event(routing_key: str, data: Dict[str, Any]):
    """
    Обработчик событий для информирования пользователей.
    Реагирует на успешную оплату (PaymentCompleted) и на отмену заказа (OrderCancelled).
    """
    order_id = data.get("orderId", "unknown")
    user_id = data.get("userId", "unknown")
    now_str = datetime.datetime.utcnow().isoformat() + "Z"

    if routing_key == "payment.completed":
        tx_id = data.get("transactionId", "unknown")
        amount = data.get("amount", 0.0)
        msg_text = f"Заказ {order_id} успешно оплачен на сумму {amount} руб. (транзакция {tx_id}). Товар передан на сборку!"
        
        record = {
            "type": "PAYMENT_SUCCESS",
            "orderId": order_id,
            "userId": user_id,
            "message": msg_text,
            "timestamp": now_str
        }
        notifications_log.append(record)
        print(f"\n[NotificationService] >>> Отправлено уведомление клиенту '{user_id}': {msg_text}")

    elif routing_key == "order.cancelled":
        reason = data.get("reason", "Причина не указана")
        msg_text = f"Заказ {order_id} был отменен. Причина: {reason}. Складские резервы освобождены."
        
        record = {
            "type": "ORDER_CANCELLED",
            "orderId": order_id,
            "userId": user_id,
            "message": msg_text,
            "timestamp": now_str
        }
        notifications_log.append(record)
        print(f"\n[NotificationService] >>> Отправлено уведомление клиенту '{user_id}': {msg_text}")

@app.on_event("startup")
def startup():
    event_bus.connect()
    # Подписываемся на события PaymentCompleted и OrderCancelled
    event_bus.subscribe(
        queue_name="notification_service_queue",
        routing_keys=["payment.completed", "order.cancelled"],
        callback=on_saga_event
    )

from pydantic import BaseModel

class InternalEvent(BaseModel):
    routing_key: str
    data: Dict[str, Any]

@app.post("/internal/events", include_in_schema=False)
def receive_internal_event(event: InternalEvent):
    event_bus.handle_incoming_event(event.routing_key, event.data)
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "notification-service", "notificationsCount": len(notifications_log)}

@app.get("/notifications", summary="Получение журнала отправленных уведомлений")
def get_notifications():
    return notifications_log

if __name__ == "__main__":
    import uvicorn
    print(f"Запуск Notification Service на порту {SERVICE_PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
