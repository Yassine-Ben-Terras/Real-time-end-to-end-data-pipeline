# Real-Time User Data Pipeline

A production-grade streaming data pipeline that ingests random user data, processes it with Apache Spark, and persists it to Apache Cassandra , all orchestrated by Apache Airflow and backed by a Confluent Kafka cluster.

```
randomuser.me API
      │
      ▼
┌─────────────┐    ┌───────────────────┐    ┌────────────────────┐
│   Airflow   │───▶│  Kafka + Schema   │───▶│  Spark Structured  │
│  Scheduler  │    │    Registry       │    │    Streaming       │
└─────────────┘    └───────────────────┘    └────────┬───────────┘
                                                      │
                                                      ▼
                                            ┌─────────────────────┐
                                            │  Apache Cassandra   │
                                            │  (spark_streams)    │
                                            └─────────────────────┘
```

## Services & Ports

| Service              | URL / Port                          |
|----------------------|-------------------------------------|
| Airflow UI           | http://localhost:8080  (admin/admin) |
| Kafka broker         | localhost:9092                      |
| Confluent Control Center | http://localhost:9021           |
| Schema Registry      | http://localhost:8081               |
| Spark Master UI      | http://localhost:9090               |
| Cassandra CQL        | localhost:9042                      |

## Quick Start

### Prerequisites
- Docker ≥ 24 + Docker Compose v2
- 8 GB RAM available to Docker
- `make` (optional but convenient)

### 1 — Clone & configure

```bash
git clone https://github.com/Yassine-Ben-Terras/Real-time-end-to-end-data-pipeline.git
cd realtime-pipeline
cp .env.example .env         
```

### 2 — Start the stack

```bash
make up
# or : docker compose up -d
```

Wait ~2 minutes for all health checks to pass:

```bash
make ps
```

### 3 — Submit the Spark job

```bash
make submit-spark
```

### 4 — Trigger the Airflow DAG

Open http://localhost:8080, log in as **admin / admin**, enable the `user_data_pipeline` DAG, and trigger a run.

### 5 — Verify data in Cassandra

```bash
make cassandra-shell
```

```cql
USE spark_streams;
SELECT id, first_name, last_name, email FROM created_users LIMIT 10;
```

## Project Structure

```
realtime-pipeline/
├── dags/
│   └── user_data_pipeline.py   # Airflow DAG (fetch → Kafka)
├── spark/
│   └── spark_stream.py         # Spark Structured Streaming job
├── script/
│   └── entrypoint.sh           # Airflow webserver bootstrap
├── docker-compose.yml          # Full stack definition
├── requirements.txt            # Python dependencies for Airflow workers
├── Makefile                    # Developer shortcuts
└── .env.example                # Environment variable template
```

## Stopping

```bash
make down          # keep volumes
make clean         # remove volumes too
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Cassandra takes > 60 s to start | Increase `start_period` in healthcheck or `ulimits` on your Docker daemon |
| Spark job can't reach Cassandra | Confirm `CASSANDRA_HOST` in `spark_stream.py` matches the Docker service name (`cassandra`) when running inside the network |
| Airflow DB error on first start | Run `docker compose restart webserver` after postgres is healthy |
