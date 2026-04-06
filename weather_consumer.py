"""
Kafka consumer for the weather pipeline.

Flow
----
Kafka -> parse JSON -> data-quality filters
      -> raw_weather        (PostgreSQL staging table)
      -> weather_hourly_agg (PostgreSQL analytics table, SQL upsert)

This implementation avoids PySpark Structured Streaming so it can run
reliably on a local Windows machine without Hadoop native libraries.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ModuleNotFoundError as exc:
    if "kafka.vendor.six.moves" in str(exc):
        raise RuntimeError(
            "Incompatible Kafka client detected. "
            "Run: pip uninstall -y kafka kafka-python && pip install -r requirements.txt"
        ) from exc
    raise


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger("weather-consumer")

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_events")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_RAW_TABLE = os.getenv("POSTGRES_RAW_TABLE", "raw_weather")
POSTGRES_AGG_TABLE = os.getenv("POSTGRES_AGG_TABLE", "weather_hourly_agg")

KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "weather-consumer-group")
POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "5000"))
MAX_BATCH_SIZE = int(os.getenv("KAFKA_MAX_BATCH_SIZE", "500"))


def pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_tables() -> None:
    conn = pg_connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {POSTGRES_RAW_TABLE} (
                        fetched_at       TIMESTAMP        NOT NULL,
                        source           VARCHAR(50)      NOT NULL,
                        city             VARCHAR(100)     NOT NULL,
                        latitude         DOUBLE PRECISION NOT NULL,
                        longitude        DOUBLE PRECISION NOT NULL,
                        temperature_c    DOUBLE PRECISION NOT NULL,
                        wind_speed_kmh   DOUBLE PRECISION NOT NULL,
                        humidity_pct     INTEGER          NOT NULL,
                        precipitation_mm DOUBLE PRECISION NOT NULL,
                        weather_code     INTEGER          NOT NULL
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {POSTGRES_AGG_TABLE} (
                        hour_bucket            TIMESTAMP        NOT NULL,
                        city                   VARCHAR(100)     NOT NULL,
                        avg_temperature_c      DOUBLE PRECISION,
                        max_temperature_c      DOUBLE PRECISION,
                        min_temperature_c      DOUBLE PRECISION,
                        avg_wind_speed_kmh     DOUBLE PRECISION,
                        avg_humidity_pct       DOUBLE PRECISION,
                        total_precipitation_mm DOUBLE PRECISION,
                        record_count           INTEGER,
                        PRIMARY KEY (hour_bucket, city)
                    )
                    """
                )
        LOGGER.info(
            "Tables '%s' and '%s' are ready.", POSTGRES_RAW_TABLE, POSTGRES_AGG_TABLE
        )
    finally:
        conn.close()


def _verify_pg_connection() -> None:
    conn = pg_connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {POSTGRES_RAW_TABLE}")
                count = cur.fetchone()[0]
        LOGGER.info(
            "PostgreSQL OK - '%s' has %d existing row(s).", POSTGRES_RAW_TABLE, count
        )
    finally:
        conn.close()


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
        max_poll_records=MAX_BATCH_SIZE,
    )


def _parse_timestamp(raw_value: Any) -> Optional[datetime]:
    if raw_value is None:
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _coerce_record(payload: Dict[str, Any]) -> Optional[Tuple]:
    fetched_at = _parse_timestamp(payload.get("fetched_at"))
    try:
        row = (
            fetched_at,
            str(payload.get("source")),
            str(payload.get("city")),
            float(payload.get("latitude")),
            float(payload.get("longitude")),
            float(payload.get("temperature_c")),
            float(payload.get("wind_speed_kmh")),
            int(payload.get("humidity_pct")),
            float(payload.get("precipitation_mm")),
            int(payload.get("weather_code")),
        )
    except (TypeError, ValueError):
        return None

    if (
        row[0] is None
        or not row[2]
        or not (-80.0 <= row[5] <= 60.0)
        or not (0 <= row[7] <= 100)
        or row[6] < 0.0
        or row[8] < 0.0
    ):
        return None

    return row


def _insert_raw_rows(rows: Sequence[Tuple]) -> None:
    if not rows:
        return
    conn = pg_connect()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {POSTGRES_RAW_TABLE} (
                        fetched_at, source, city, latitude, longitude,
                        temperature_c, wind_speed_kmh, humidity_pct,
                        precipitation_mm, weather_code
                    ) VALUES %s
                    """,
                    rows,
                )
    finally:
        conn.close()


def _hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _upsert_hourly_agg(affected_hours: Iterable[datetime]) -> None:
    affected_hours = list({hour for hour in affected_hours})
    if not affected_hours:
        return

    upsert_sql = f"""
        INSERT INTO {POSTGRES_AGG_TABLE} (
            hour_bucket, city,
            avg_temperature_c, max_temperature_c, min_temperature_c,
            avg_wind_speed_kmh, avg_humidity_pct, total_precipitation_mm,
            record_count
        )
        SELECT
            DATE_TRUNC('hour', fetched_at)           AS hour_bucket,
            city,
            ROUND(AVG(temperature_c)::numeric,    2) AS avg_temperature_c,
            ROUND(MAX(temperature_c)::numeric,    2) AS max_temperature_c,
            ROUND(MIN(temperature_c)::numeric,    2) AS min_temperature_c,
            ROUND(AVG(wind_speed_kmh)::numeric,   2) AS avg_wind_speed_kmh,
            ROUND(AVG(humidity_pct)::numeric,     1) AS avg_humidity_pct,
            ROUND(SUM(precipitation_mm)::numeric, 2) AS total_precipitation_mm,
            COUNT(*)                                 AS record_count
        FROM {POSTGRES_RAW_TABLE}
        WHERE DATE_TRUNC('hour', fetched_at) = ANY(%s)
        GROUP BY DATE_TRUNC('hour', fetched_at), city
        ON CONFLICT (hour_bucket, city) DO UPDATE SET
            avg_temperature_c      = EXCLUDED.avg_temperature_c,
            max_temperature_c      = EXCLUDED.max_temperature_c,
            min_temperature_c      = EXCLUDED.min_temperature_c,
            avg_wind_speed_kmh     = EXCLUDED.avg_wind_speed_kmh,
            avg_humidity_pct       = EXCLUDED.avg_humidity_pct,
            total_precipitation_mm = EXCLUDED.total_precipitation_mm,
            record_count           = EXCLUDED.record_count
    """

    conn = pg_connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(upsert_sql, (affected_hours,))
        LOGGER.info(
            "Upserted hourly agg for %d hour bucket(s).", len(affected_hours)
        )
    finally:
        conn.close()


def _process_messages(messages: Sequence[Any]) -> None:
    total_received = len(messages)
    if total_received == 0:
        return

    valid_rows: List[Tuple] = []
    dropped_payload: Optional[Dict[str, Any]] = None

    for msg in messages:
        payload = msg.value
        row = _coerce_record(payload)
        if row is None:
            if dropped_payload is None:
                dropped_payload = payload
            continue
        valid_rows.append(row)

    valid_count = len(valid_rows)
    dropped = total_received - valid_count

    LOGGER.info(
        "Batch received=%d valid=%d dropped_by_quality=%d",
        total_received,
        valid_count,
        dropped,
    )

    if dropped_payload is not None:
        LOGGER.warning("Sample dropped record: %s", dropped_payload)

    if valid_count == 0:
        LOGGER.warning("All records failed quality checks, nothing written.")
        return

    _insert_raw_rows(valid_rows)
    LOGGER.info(
        "%d row(s) written to '%s'.", valid_count, POSTGRES_RAW_TABLE
    )

    affected_hours = [_hour_bucket(row[0]) for row in valid_rows]
    _upsert_hourly_agg(affected_hours)


def start_consumer_loop() -> None:
    consumer = create_consumer()
    LOGGER.info(
        "Kafka consumer connected | bootstrap=%s topic=%s group_id=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
    )

    try:
        while True:
            polled = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_BATCH_SIZE)
            batch = []
            for records in polled.values():
                batch.extend(records)

            if not batch:
                LOGGER.info("No messages received in the last %d ms.", POLL_TIMEOUT_MS)
                time.sleep(1)
                continue

            _process_messages(batch)
    finally:
        consumer.close()


def shutdown_handler(signum: int, _frame: Any) -> None:
    LOGGER.info("Signal %s received - shutting down.", signum)
    raise KeyboardInterrupt


def main() -> None:
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        ensure_tables()
        _verify_pg_connection()
        start_consumer_loop()
    except KeyboardInterrupt:
        LOGGER.info("Consumer stopped by user.")
    except KafkaError as exc:
        LOGGER.exception("Kafka consumer failed: %s", exc)
        sys.exit(1)
    except Exception as exc:
        LOGGER.exception("Consumer failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
