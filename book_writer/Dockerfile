# Dockerfile for Writer Service
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY writer.py .
CMD ["python", "-u", "writer.py"]