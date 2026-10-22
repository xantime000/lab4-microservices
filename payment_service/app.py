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

from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
def payment_dashboard():
    rows_html = ""
    for p in reversed(payments_db):
        st = p.get("status", "UNKNOWN")
        color = "#238636" if st == "COMPLETED" else "#da3633"
        tx_info = p.get("transactionId", p.get("reason", "-"))
        rows_html += f"""
        <tr>
            <td style="font-family: monospace; font-size: 12px;">{p.get('orderId', '')[:8]}...</td>
            <td>{p.get('userId', '')}</td>
            <td><strong>{p.get('amount', 0.0)} руб.</strong></td>
            <td><span style="background: {color}; color: white; padding: 3px 8px; border-radius: 12px; font-weight: bold; font-size: 11px;">{st}</span></td>
            <td style="font-family: monospace; font-size: 12px; color: #8b949e;">{tx_info}</td>
            <td style="font-size: 11px; color: #8b949e;">{p.get('timestamp', '')}</td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Payment Service — Реестр платежей Saga</title>
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
            <a href="http://localhost:8002/" class="active">💳 Payment Service (8002)</a>
            <a href="http://localhost:8003/">🔔 Notification Service (8003)</a>
            <a href="http://localhost:15672/" target="_blank">🐰 RabbitMQ UI (15672)</a>
        </div>

        <div class="card">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h1>Payment Service (Сервис оплаты)</h1>
                    <p style="margin: 0; color: #8b949e;">Слушает событие <code>OrderCreated</code>, обрабатывает платежи и публикует <code>PaymentCompleted</code> / <code>PaymentFailed</code>.</p>
                </div>
                <button onclick="location.reload()">🔄 Обновить</button>
            </div>
            <div style="margin-top: 15px; font-size: 13px; color: #8b949e;">
                Swagger UI документация: <a href="/docs" style="color: #58a6ff;" target="_blank">/docs</a> • Сырой JSON: <a href="/payments" style="color: #58a6ff;" target="_blank">/payments</a>
            </div>
        </div>

        <div class="card">
            <h2>Реестр обработанных транзакций ({len(payments_db)})</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID заказа</th>
                        <th>Покупатель</th>
                        <th>Сумма</th>
                        <th>Статус оплаты</th>
                        <th>Transaction ID / Причина ошибки</th>
                        <th>Время</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="6" style="text-align:center; padding: 20px; color: #8b949e;">Платежей пока нет. Создайте заказ в Order Service (порт 8001), чтобы увидеть транзакцию!</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""

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
