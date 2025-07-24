# Dockerfile for Book Collector Service
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY trades_collector.py .
CMD ["python", "-u", "trades_collector.py"]