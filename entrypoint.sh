#!/bin/sh
set -e

DB_HOST=${DB_HOST:-db}
DB_PORT=${DB_PORT:-5432}
# Si quieres esperar más, sube este valor (ej. 120)
MAX_WAIT=${MAX_WAIT:-60}
SLEEP_SECS=1

echo "Waiting for Postgres TCP socket at ${DB_HOST}:${DB_PORT} (max ${MAX_WAIT}s)..."

count=0

while [ $count -lt $MAX_WAIT ]; do
  python - <<PY >/dev/null 2>&1
import socket, sys
try:
    s = socket.create_connection(("${DB_HOST}", int("${DB_PORT}")), 1)
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
  if [ $? -eq 0 ]; then
    echo "TCP socket open (after ${count}s). Now verifying DB accepts SQL connections..."
    break
  fi
  count=$((count+1))
  sleep $SLEEP_SECS
done

if [ $count -ge $MAX_WAIT ]; then
  echo "Timed out waiting for TCP socket at ${DB_HOST}:${DB_PORT}" >&2
  exit 1
fi

count=0
while [ $count -lt $MAX_WAIT ]; do
  python - <<PY
import os, sys
import psycopg2
from urllib.parse import urlparse
db_url = os.environ.get("DATABASE_URL", "")
if not db_url:
    # Construir DSN desde host/port/credenciales por defecto si no existe
    user = os.environ.get("POSTGRES_USER","postgres")
    pwd = os.environ.get("POSTGRES_PASSWORD","postgres")
    host = os.environ.get("DB_HOST","${DB_HOST}")
    port = os.environ.get("DB_PORT","${DB_PORT}")
    dbname = os.environ.get("POSTGRES_DB","postgres")
    dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{dbname}"
else:
    dsn = db_url
try:
    # psycopg2 acepta dsn en formato URI (requires psycopg2>=2.7)
    conn = psycopg2.connect(dsn, connect_timeout=3)
    conn.close()
    print("ok")
    sys.exit(0)
except Exception as e:
    # imprimimos nada para no spammear logs, y salimos con no-cero
    # print("db not ready:", e)
    sys.exit(1)
PY
  if [ $? -eq 0 ]; then
    echo "Postgres accepts SQL connections (after ${count}s)"
    break
  fi
  count=$((count+1))
  sleep $SLEEP_SECS
done

if [ $count -ge $MAX_WAIT ]; then
  echo "Timed out waiting for Postgres to accept SQL connections" >&2
  exit 1
fi

echo "Ensuring DB tables..."
python - <<PY
from app.db import Base, engine
import app.models
Base.metadata.create_all(bind=engine)
print("DB tables ensured")
PY


exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
