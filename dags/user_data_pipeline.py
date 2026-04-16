"""
dags/user_data_pipeline.py
────────────────────────────────────────────────────────────────────────────
Airflow DAG: pulls random user data from randomuser.me, validates it,
and publishes enriched records to the Kafka 'users_created' topic.
────────────────────────────────────────────────────────────────────────────
"""

import json
import logging
import uuid
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ─── DAG defaults ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

KAFKA_TOPIC   = "users_created"
KAFKA_SERVERS = "broker:29092"
API_URL       = "https://randomuser.me/api/"
BATCH_SIZE    = 50   # records per run


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _fetch_users(n: int = BATCH_SIZE) -> list[dict]:
    """Fetch `n` random users from the public API."""
    resp = requests.get(API_URL, params={"results": n}, timeout=15)
    resp.raise_for_status()
    return resp.json().get("results", [])


def _transform_user(raw: dict) -> dict:
    """Flatten and normalise a raw randomuser.me record."""
    location = raw.get("location", {})
    street    = location.get("street", {})

    return {
        "id":              str(uuid.uuid4()),
        "first_name":      raw["name"]["first"],
        "last_name":       raw["name"]["last"],
        "gender":          raw.get("gender", "unknown"),
        "address":         (
            f"{street.get('number', '')} {street.get('name', '')}, "
            f"{location.get('city', '')}, {location.get('country', '')}"
        ).strip(", "),
        "post_code":       str(location.get("postcode", "")),
        "email":           raw.get("email", ""),
        "username":        raw["login"]["username"],
        "dob":             raw["dob"]["date"],
        "registered_date": raw["registered"]["date"],
        "phone":           raw.get("phone", ""),
        "picture":         raw["picture"]["medium"],
    }


# ─── Tasks ────────────────────────────────────────────────────────────────────

def fetch_and_publish(**context) -> None:
    """Fetch users from API and stream them into Kafka."""
    # Import here so Airflow workers only need kafka-python, not the full env
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        batch_size=32_768,
    )

    raw_users = _fetch_users(BATCH_SIZE)
    success, failed = 0, 0

    for raw in raw_users:
        try:
            user = _transform_user(raw)
            future = producer.send(KAFKA_TOPIC, value=user)
            future.get(timeout=10)   # block to catch per-message errors
            success += 1
        except (KafkaError, Exception) as exc:
            logger.error("Failed to produce message: %s", exc)
            failed += 1

    producer.flush()
    producer.close()

    logger.info(
        "Run complete. Published: %d | Failed: %d | Total fetched: %d",
        success, failed, len(raw_users),
    )

    # Surface metrics in XCom for downstream tasks / monitoring
    context["ti"].xcom_push(
        key="publish_stats",
        value={"success": success, "failed": failed, "total": len(raw_users)},
    )


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="user_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch random users and stream them to Kafka → Spark → Cassandra",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "kafka", "cassandra"],
) as dag:

    publish_task = PythonOperator(
        task_id="fetch_and_publish_to_kafka",
        python_callable=fetch_and_publish,
    )
