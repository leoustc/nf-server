FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# pymysql needs cryptography when MySQL is configured with sha256/caching_sha2 auth
RUN pip install --no-cache-dir fastapi uvicorn[standard] pymysql cryptography pydantic==1.10.13

COPY . .

RUN chmod +x /app/entry.sh

EXPOSE 8080

ENTRYPOINT ["/app/entry.sh"]
CMD ["sh", "-c", "uvicorn gateway:app --host 0.0.0.0 --port 8080 --workers ${UVICORN_WORKERS:-2} --log-level error"]
