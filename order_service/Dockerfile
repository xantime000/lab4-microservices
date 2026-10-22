FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общие модули и код сервиса
COPY common /app/common
COPY order_service /app/order_service

ENV PYTHONPATH=/app
ENV PORT=8001

EXPOSE 8001

CMD ["python", "order_service/app.py"]
