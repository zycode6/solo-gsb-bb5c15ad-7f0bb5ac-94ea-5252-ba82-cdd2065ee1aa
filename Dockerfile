FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UPLOAD_DB_PATH=/data/coordinator.db \
    DEFAULT_LEASE_TTL_SECONDS=300 \
    MAX_LEASE_TTL_SECONDS=3600

WORKDIR /app

# 依赖安装纳入镜像构建
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# SQLite 数据目录（可挂卷持久化）；建库建表在应用启动时自动完成
RUN mkdir -p /data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
