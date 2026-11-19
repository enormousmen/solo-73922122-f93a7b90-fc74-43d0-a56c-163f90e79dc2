FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY manifest.json ./manifest.json

ENV DB_PATH=/data/permits.db \
    MANIFEST_PATH=/app/manifest.json

EXPOSE 8000

# 单 worker：SQLite 单写者，应用内串行化并发申请
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
