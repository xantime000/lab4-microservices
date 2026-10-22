import os
import sys
import uuid
import datetime
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

# Подключаем общую библиотеку messaging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.messaging import EventBus

SERVICE_PORT = int(os.getenv("PORT", "8001"))

app = FastAPI(
    title="Order Service (Паттерн Saga: Хореография)",
    description="Сервис управления заказами. Реализует публикацию OrderCreated, подписку на события оплаты и компенсирующую транзакцию (отмена заказа при PaymentFailed).",
    version="1.0.0"
)

# In-memory база данных заказов
orders_db: Dict[str, Dict[str, Any]] = {}
event_bus = EventBus(service_name="OrderService")

class OrderItem(BaseModel):
    productId: str = Field(..., example="prod-1")
    quantity: int = Field(..., ge=1, example=2)

class CreateOrderRequest(BaseModel):
    userId: str = Field(..., example="user-1")
    items: List[OrderItem]
    totalAmount: float = Field(..., example=1500.0)
    failPayment: Optional[bool] = Field(False, description="Флаг для детерминированной симуляции ошибки оплаты (демонстрация компенсации)")

class OrderResponse(BaseModel):
    orderId: str
    userId: str
    items: List[OrderItem]
    totalAmount: float
    status: str
    createdAt: str
    updatedAt: str
    history: List[str]

def on_payment_event(routing_key: str, data: Dict[str, Any]):
    """Обработчик событий оплаты от Payment Service"""
    order_id = data.get("orderId")
    if not order_id or order_id not in orders_db:
        print(f"[OrderService] Получено событие для неизвестного заказа: {order_id}")
        return

    order = orders_db[order_id]
    current_status = order["status"]
    now_str = datetime.datetime.utcnow().isoformat() + "Z"

    if routing_key == "payment.completed":
        # 1. Проверка идемпотентности
        if current_status == "CONFIRMED":
            print(f"[OrderService Idempotency] Заказ {order_id} уже подтвержден. Повторное событие проигнорировано.")
            return
        if current_status == "CANCELLED":
            print(f"[OrderService Conflict] Нельзя подтвердить отмененный заказ {order_id}.")
            return

        order["status"] = "CONFIRMED"
        order["updatedAt"] = now_str
        tx_id = data.get("transactionId", "unknown")
        msg = f"Оплата успешно подтверждена (tx: {tx_id}). Статус заказа изменен на CONFIRMED."
        order["history"].append(f"[{now_str}] {msg}")
        print(f"[OrderService] Заказ {order_id}: {msg}")

    elif routing_key == "payment.failed":
        # 2. Компенсирующая транзакция (Saga Compensation)
        if current_status == "CANCELLED":
            print(f"[OrderService Idempotency] Заказ {order_id} уже отменен. Повторная компенсация не требуется.")
            return

        reason = data.get("reason", "Ошибка авторизации платежа")
        order["status"] = "CANCELLED"
        order["updatedAt"] = now_str
        msg = f"КОМПЕНСАЦИЯ: Оплата отклонена ({reason}). Статус заказа переведен в CANCELLED."
        order["history"].append(f"[{now_str}] {msg}")
        print(f"[OrderService Compensation] {msg}")

        # Публикуем событие отмены заказа для уведомлений и освобождения складских резервов
        event_bus.publish("order.cancelled", {
            "orderId": order_id,
            "userId": order["userId"],
            "reason": reason,
            "cancelledAt": now_str
        })

@app.on_event("startup")
def startup():
    event_bus.connect()
    # Подписываемся на события от Payment Service: payment.completed и payment.failed
    event_bus.subscribe(
        queue_name="order_service_queue",
        routing_keys=["payment.completed", "payment.failed"],
        callback=on_payment_event
    )

@app.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED, summary="Создание нового заказа")
def create_order(req: CreateOrderRequest):
    order_id = str(uuid.uuid4())
    now_str = datetime.datetime.utcnow().isoformat() + "Z"

    # Создаем заказ со статусом PENDING
    order_record = {
        "orderId": order_id,
        "userId": req.userId,
        "items": [item.dict() for item in req.items],
        "totalAmount": req.totalAmount,
        "status": "PENDING",
        "createdAt": now_str,
        "updatedAt": now_str,
        "history": [f"[{now_str}] Заказ создан в статусе PENDING"]
    }
    orders_db[order_id] = order_record

    # Публикуем событие OrderCreated в брокер сообщений
    event_payload = {
        "orderId": order_id,
        "userId": req.userId,
        "items": order_record["items"],
        "totalAmount": req.totalAmount,
        "failPayment": req.failPayment,
        "createdAt": now_str
    }
    event_bus.publish("order.created", event_payload)

    return order_record

@app.get("/orders", response_model=List[OrderResponse], summary="Получение списка всех заказов")
def list_orders():
    return list(orders_db.values())

@app.get("/orders/{order_id}", response_model=OrderResponse, summary="Получение информации о заказе по ID")
def get_order(order_id: str):
    if order_id not in orders_db:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    return orders_db[order_id]

@app.post("/orders/demo/success", summary="Создать успешный заказ (демонстрация успешной Saga)")
def demo_success():
    req = CreateOrderRequest(
        userId="user-student-1",
        items=[OrderItem(productId="prod-laptop", quantity=1)],
        totalAmount=1500.0,
        failPayment=False
    )
    return create_order(req)

@app.post("/orders/demo/fail", summary="Создать заказ с ошибкой (демонстрация компенсации Saga)")
def demo_fail():
    req = CreateOrderRequest(
        userId="user-student-2",
        items=[OrderItem(productId="prod-phone", quantity=1)],
        totalAmount=-100.0,  # Отрицательная сумма -> детерминированный сбой оплаты
        failPayment=True
    )
    return create_order(req)

class InternalEvent(BaseModel):
    routing_key: str
    data: Dict[str, Any]

@app.post("/internal/events", include_in_schema=False)
def receive_internal_event(event: InternalEvent):
    event_bus.handle_incoming_event(event.routing_key, event.data)
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "order-service", "ordersCount": len(orders_db)}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    rows_html = ""
    for o in reversed(list(orders_db.values())):
        st = o["status"]
        color = "#238636" if st == "CONFIRMED" else ("#da3633" if st == "CANCELLED" else "#d29922")
        items_str = ", ".join([f"{i['productId']} (x{i['quantity']})" for i in o["items"]])
        history_str = "<br>".join(o["history"])
        rows_html += f"""
        <tr>
            <td style="font-family: monospace; font-size: 12px;">{o['orderId'][:8]}...</td>
            <td>{o['userId']}</td>
            <td>{items_str}</td>
            <td><strong>{o['totalAmount']} руб.</strong></td>
            <td><span style="background: {color}; color: white; padding: 3px 8px; border-radius: 12px; font-weight: bold; font-size: 11px;">{st}</span></td>
            <td style="font-size: 11px; color: #8b949e;">{history_str}</td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Лабораторная №4: Паттерн Saga (Choreography) с RabbitMQ</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0d1117; color: #c9d1d9; padding: 24px; }}
        .container {{ max-width: 1000px; margin: 0 auto; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
        h1 {{ color: #58a6ff; margin-top: 0; }}
        button {{ cursor: pointer; padding: 10px 18px; border-radius: 6px; font-weight: 600; border: none; font-size: 14px; margin-right: 10px; transition: 0.2s; }}
        .btn-success {{ background: #238636; color: white; }}
        .btn-success:hover {{ background: #2ea043; }}
        .btn-fail {{ background: #da3633; color: white; }}
        .btn-fail:hover {{ background: #f85149; }}
        .nav {{ margin-bottom: 15px; display: flex; gap: 10px; flex-wrap: wrap; }}
        .nav a {{ background: #21262d; border: 1px solid #30363d; padding: 6px 12px; border-radius: 6px; color: #58a6ff; text-decoration: none; font-size: 13px; font-weight: 500; }}
        .nav a:hover {{ background: #30363d; }}
        .nav a.active {{ background: #1f6feb; color: white; border-color: #58a6ff; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 13px; }}
        th, td {{ border: 1px solid #30363d; padding: 10px; text-align: left; }}
        th {{ background: #21262d; color: #8b949e; }}
        a {{ color: #58a6ff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="http://localhost:8001/" class="active">📦 Order Service (8001)</a>
            <a href="http://localhost:8002/">💳 Payment Service (8002)</a>
            <a href="http://localhost:8003/">🔔 Notification Service (8003)</a>
            <a href="http://localhost:15672/" target="_blank">🐰 RabbitMQ UI (15672)</a>
        </div>

        <div class="card">
            <h1>Паттерн Saga (Хореография событий) с RabbitMQ</h1>
            <p>Лабораторная работа №4 • Группа 8И42 • Довидович А. В. и Петрушенко Д. Е.</p>
            
            <div style="margin: 20px 0;">
                <button class="btn-success" onclick="createOrder(false)">▶ 1. Создать успешный заказ (Saga Success)</button>
                <button class="btn-fail" onclick="createOrder(true)">⚡ 2. Создать заказ с ошибкой (Saga Compensation)</button>
                <button style="background: #30363d; color: #c9d1d9;" onclick="location.reload()">🔄 Обновить</button>
            </div>

            <div style="font-size: 13px; color: #8b949e;">
                Брокер RabbitMQ Management UI: <a href="http://localhost:15672" target="_blank">http://localhost:15672</a> (guest / guest)<br>
                OpenAPI Swagger Order Service: <a href="/docs" target="_blank">/docs</a>
            </div>
        </div>

        <div class="card">
            <h2>Реестр заказов и история распределенной транзакции</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID заказа</th>
                        <th>Покупатель</th>
                        <th>Товары</th>
                        <th>Сумма</th>
                        <th>Статус Saga</th>
                        <th>История событий Saga</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="6" style="text-align:center; padding: 20px; color: #8b949e;">Заказов пока нет. Нажмите одну из кнопок выше для создания!</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>

    <script>
        async function createOrder(fail) {{
            const endpoint = fail ? '/orders/demo/fail' : '/orders/demo/success';
            await fetch(endpoint, {{ method: 'POST' }});
            setTimeout(() => location.reload(), 1200);
        }}
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    print(f"Запуск Order Service на порту {SERVICE_PORT}...")
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
