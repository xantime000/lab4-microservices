FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY notification_service /app/notification_service

ENV PYTHONPATH=/app
ENV PORT=8003

EXPOSE 8003

CMD ["python", "notification_service/app.py"]
