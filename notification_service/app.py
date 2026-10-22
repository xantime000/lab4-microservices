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

from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
def notification_dashboard():
    rows_html = ""
    for n in reversed(notifications_log):
        tp = n.get("type", "UNKNOWN")
        color = "#238636" if tp == "PAYMENT_SUCCESS" else "#da3633"
        rows_html += f"""
        <tr>
            <td style="font-family: monospace; font-size: 12px;">{n.get('orderId', '')[:8]}...</td>
            <td>{n.get('userId', '')}</td>
            <td><span style="background: {color}; color: white; padding: 3px 8px; border-radius: 12px; font-weight: bold; font-size: 11px;">{tp}</span></td>
            <td>{n.get('message', '')}</td>
            <td style="font-size: 11px; color: #8b949e;">{n.get('timestamp', '')}</td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Notification Service — Лента уведомлений Saga</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0d1117; color: #c9d1d9; padding: 24px; }}
        .container {{ max-width: 1000px; margin: 0 auto; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
        h1 {{ color: #58a6ff; margin-top: 0; }}
        .nav {{ margin-bottom: 15px; display: flex; gap: 10px; flex-wrap: wrap; }}
        .nav a {{ background: #21262d; border: 1px solid #30363d; padding: 6px 12px; border-radius: 6px; color: #58a6ff; text-decoration: none; font-size: 13px; font-weight: 500; }}
        .nav a:hover {{ background: #30363d; }}
        .nav a.active {{ background: #1f6feb; color: white; border-color: #58a6ff; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 13px; }}
        th, td {{ border: 1px solid #30363d; padding: 10px; text-align: left; }}
        th {{ background: #21262d; color: #8b949e; }}
        button {{ cursor: pointer; background: #30363d; border: 1px solid #8b949e; color: #c9d1d9; padding: 6px 14px; border-radius: 6px; font-size: 13px; }}
        button:hover {{ background: #484f58; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="http://localhost:8001/">📦 Order Service (8001)</a>
            <a href="http://localhost:8002/">💳 Payment Service (8002)</a>
            <a href="http://localhost:8003/" class="active">🔔 Notification Service (8003)</a>
            <a href="http://localhost:15672/" target="_blank">🐰 RabbitMQ UI (15672)</a>
        </div>

        <div class="card">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h1>Notification Service (Сервис уведомлений)</h1>
                    <p style="margin: 0; color: #8b949e;">Слушает события <code>PaymentCompleted</code> и <code>OrderCancelled</code> и информирует клиентов.</p>
                </div>
                <button onclick="location.reload()">🔄 Обновить</button>
            </div>
            <div style="margin-top: 15px; font-size: 13px; color: #8b949e;">
                Swagger UI документация: <a href="/docs" style="color: #58a6ff;" target="_blank">/docs</a> • Сырой JSON: <a href="/notifications" style="color: #58a6ff;" target="_blank">/notifications</a>
            </div>
        </div>

        <div class="card">
            <h2>Лента отправленных клиентам уведомлений ({len(notifications_log)})</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID заказа</th>
                        <th>Клиент</th>
                        <th>Тип события</th>
                        <th>Текст уведомления</th>
                        <th>Время</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="5" style="text-align:center; padding: 20px; color: #8b949e;">Уведомлений пока нет. При совершении операций Саги они появятся здесь автоматически!</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""

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
