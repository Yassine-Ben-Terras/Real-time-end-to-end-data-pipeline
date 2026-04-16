"""
spark_stream.py
────────────────────────────────────────────────────────────────────────────
Real-time user data pipeline: Kafka → Spark Structured Streaming → Cassandra

Flow:
  1. Spark reads JSON messages from the 'users_created' Kafka topic
  2. Parses and validates each record against a strict schema
  3. Writes the enriched stream to a Cassandra table with checkpoint support

Usage:
  spark-submit --packages <jars> spark/spark_stream.py
────────────────────────────────────────────────────────────────────────────
"""

import logging
import sys
import time
from typing import Optional

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/spark_stream.log"),
    ],
)
logger = logging.getLogger("spark_stream")

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "users_created"
CASSANDRA_HOST          = "localhost"
CASSANDRA_PORT          = 9042
CASSANDRA_USER          = "cassandra"
CASSANDRA_PASSWORD      = "cassandra"
CASSANDRA_KEYSPACE      = "spark_streams"
CASSANDRA_TABLE         = "created_users"
CHECKPOINT_LOCATION     = "/tmp/checkpoints/spark_streams"
SPARK_APP_NAME          = "UserDataStreamingPipeline"

SPARK_PACKAGES = (
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
)

# ─── Schema ───────────────────────────────────────────────────────────────────

USER_SCHEMA = StructType([
    StructField("id",              StringType(), nullable=False),
    StructField("first_name",      StringType(), nullable=True),
    StructField("last_name",       StringType(), nullable=True),
    StructField("gender",          StringType(), nullable=True),
    StructField("address",         StringType(), nullable=True),
    StructField("post_code",       StringType(), nullable=True),
    StructField("email",           StringType(), nullable=True),
    StructField("username",        StringType(), nullable=True),
    StructField("dob",             StringType(), nullable=True),
    StructField("registered_date", StringType(), nullable=True),
    StructField("phone",           StringType(), nullable=True),
    StructField("picture",         StringType(), nullable=True),
])

# ─── Cassandra Helpers ────────────────────────────────────────────────────────

def create_cassandra_connection(retries: int = 5, delay: int = 5):
    """
    Establish a Cassandra session with retry logic.
    Returns a connected session or None on failure.
    """
    auth = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)

    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                auth_provider=auth,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                default_retry_policy=RetryPolicy(),
                connect_timeout=10,
                control_connection_timeout=10,
            )
            session = cluster.connect()
            logger.info("Cassandra connection established (attempt %d/%d)", attempt, retries)
            return session
        except NoHostAvailable as exc:
            logger.warning(
                "Cassandra not reachable (attempt %d/%d): %s", attempt, retries, exc
            )
            if attempt < retries:
                time.sleep(delay)

    logger.error("Could not connect to Cassandra after %d attempts.", retries)
    return None


def provision_cassandra(session) -> None:
    """Create keyspace and table if they don't already exist."""

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)
    logger.info("Keyspace '%s' is ready.", CASSANDRA_KEYSPACE)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
            id              UUID        PRIMARY KEY,
            first_name      TEXT,
            last_name       TEXT,
            gender          TEXT,
            address         TEXT,
            post_code       TEXT,
            email           TEXT,
            username        TEXT,
            dob             TEXT,
            registered_date TEXT,
            phone           TEXT,
            picture         TEXT,
            ingested_at     TIMESTAMP
        );
    """)
    logger.info("Table '%s.%s' is ready.", CASSANDRA_KEYSPACE, CASSANDRA_TABLE)


# ─── Spark Helpers ────────────────────────────────────────────────────────────

def create_spark_session() -> Optional[SparkSession]:
    """Build a SparkSession configured for Kafka + Cassandra connectors."""
    try:
        session = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .config("spark.jars.packages", SPARK_PACKAGES)
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
            .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
            .config("spark.cassandra.auth.username", CASSANDRA_USER)
            .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD)
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession '%s' created.", SPARK_APP_NAME)
        return session
    except Exception:
        logger.exception("Failed to create SparkSession.")
        return None


def read_kafka_stream(spark: SparkSession) -> Optional[DataFrame]:
    """Subscribe to the Kafka topic and return a raw streaming DataFrame."""
    try:
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 10_000)
            .load()
        )
        logger.info("Subscribed to Kafka topic '%s'.", KAFKA_TOPIC)
        return df
    except Exception:
        logger.exception("Failed to subscribe to Kafka.")
        return None


def parse_user_stream(raw_df: DataFrame) -> DataFrame:
    """
    Deserialise the Kafka value bytes as JSON using USER_SCHEMA.
    Adds an 'ingested_at' timestamp for auditability.
    Drops rows where the mandatory 'id' field is null.
    """
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(
            from_json(col("json_str"), USER_SCHEMA).alias("data"),
            col("kafka_ts"),
        )
        .select("data.*", "kafka_ts")
        .withColumn("ingested_at", current_timestamp())
        .filter(col("id").isNotNull())
    )
    logger.info("Schema applied; streaming DataFrame ready.")
    return parsed


def start_cassandra_sink(df: DataFrame):
    """
    Write the parsed stream to Cassandra using foreachBatch for
    fine-grained control and error isolation per micro-batch.
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        row_count = batch_df.count()
        if row_count == 0:
            logger.debug("Batch %d: empty, skipping.", batch_id)
            return

        logger.info("Batch %d: writing %d rows to Cassandra.", batch_id, row_count)
        try:
            (
                batch_df.write
                .format("org.apache.spark.sql.cassandra")
                .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
                .mode("append")
                .save()
            )
            logger.info("Batch %d: committed successfully.", batch_id)
        except Exception:
            logger.exception("Batch %d: write failed.", batch_id)

    query = (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("Streaming query started (id=%s).", query.id)
    return query


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== Starting %s ===", SPARK_APP_NAME)

    # 1. Spark
    spark = create_spark_session()
    if spark is None:
        sys.exit(1)

    # 2. Kafka source
    raw_df = read_kafka_stream(spark)
    if raw_df is None:
        sys.exit(1)

    # 3. Parse
    user_df = parse_user_stream(raw_df)

    # 4. Cassandra provisioning
    cas_session = create_cassandra_connection()
    if cas_session is None:
        sys.exit(1)
    provision_cassandra(cas_session)
    cas_session.shutdown()           # Spark workers manage their own connections

    # 5. Stream to Cassandra
    query = start_cassandra_sink(user_df)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted by user – stopping stream gracefully.")
        query.stop()
    finally:
        spark.stop()
        logger.info("=== %s stopped ===", SPARK_APP_NAME)


if __name__ == "__main__":
    main()
