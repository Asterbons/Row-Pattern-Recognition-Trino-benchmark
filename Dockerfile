FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Project code is mounted via docker-compose volume — no COPY needed
CMD ["python", "benchmark.py"]
