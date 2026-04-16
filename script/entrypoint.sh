#!/usr/bin/env bash
set -euo pipefail

# ─── Install Python dependencies ──────────────────────────────────────────────
if [[ -f /opt/airflow/requirements.txt ]]; then
  echo ">>> Installing requirements..."
  pip install --quiet --no-cache-dir -r /opt/airflow/requirements.txt
fi

# ─── Initialize / upgrade metadata DB ────────────────────────────────────────
echo ">>> Upgrading Airflow metadata DB..."
airflow db upgrade

# ─── Create default admin user (idempotent) ───────────────────────────────────
if ! airflow users list | grep -q "^admin "; then
  echo ">>> Creating admin user..."
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# ─── Start the webserver ──────────────────────────────────────────────────────
echo ">>> Starting Airflow webserver..."
exec airflow webserver
