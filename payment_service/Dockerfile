FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY payment_service /app/payment_service

ENV PYTHONPATH=/app
ENV PORT=8002

EXPOSE 8002

CMD ["python", "payment_service/app.py"]
