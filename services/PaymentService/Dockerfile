FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ ./schemas/
COPY services/payment-service/ .

RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

CMD ["python", "main.py"]