import json
import logging
import os
import threading
from datetime import datetime
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taxi-api")

DATABASE_URL = os.environ["DATABASE_URL"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "taxi-processed")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "taxi-api-consumer")

app = FastAPI(title="Taxi Fleet API")
# TODO:we only listen to processed data right now, we also need to fetch the side topics
# TODO: this has a lot of funktions wich are not used right now maybe we could remove them

@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


INSERT_SQL = """
INSERT INTO taxi_speed (
    taxi_id, event_timestamp, longitude, latitude, speed,
    average_speed, total_distance, is_speeding, is_out_of_area,
    last_moved, is_parking, ingested_at
) VALUES (
    %(taxi_id)s, %(timestamp)s, %(longitude)s, %(latitude)s, %(speed)s,
    %(averageSpeed)s, %(totalDistance)s, %(isSpeeding)s, %(isOutOfArea)s,
    %(lastMoved)s, %(isParking)s, %(ingested_at)s
)
"""


def store_event(record: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, record)
        conn.commit()


def consume_loop() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    log.info("Kafka-Consumer gestartet fuer Topic '%s'", KAFKA_TOPIC)
    for message in consumer:
        try:
            store_event(message.value)
        except Exception:
            log.exception("Konnte Event nicht speichern: %s", message.value)


def reset_database() -> None:
    """Clear stored taxi data on startup.

    The provider always replays the same T-Drive dataset from the beginning, so
    without a reset each run stacks duplicate rows on top of the previous run's
    data — including rows whose wall-clock received_at is "in the future"
    relative to the new run, which corrupts time-ordered queries. Truncating on
    startup gives every run a clean slate while keeping full history *within* a
    run (path replay, history endpoints still work).

    Controlled by RESET_DB_ON_STARTUP (default "true"); set to "false" to keep
    data across restarts.
    """
    if os.environ.get("RESET_DB_ON_STARTUP", "true").lower() != "true":
        log.info("RESET_DB_ON_STARTUP disabled — keeping existing taxi data")
        return
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE taxi_speed;")
            conn.commit()
        log.info("Cleared taxi_speed table for a fresh run")
    except Exception:
        log.exception("Could not reset taxi_speed table on startup")


@app.on_event("startup")
def start_consumer_thread() -> None:
    # Wipe stale data from previous runs BEFORE the consumer starts writing.
    reset_database()
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()

@app.get("/taxis/{taxi_id}/locations")
def get_last_locations(taxi_id: int, limit: int = Query(5, le=1000)):
    """Letzte `limit` Standorte eines Taxis, sortiert nach event_timestamp
    (neuester zuerst)."""
    sql = """
        SELECT taxi_id, event_timestamp, longitude, latitude, speed,
               average_speed, total_distance, is_speeding, is_out_of_area,
               is_parking
        FROM taxi_speed
        WHERE taxi_id = %s
        ORDER BY event_timestamp DESC
        LIMIT %s
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (taxi_id, limit))
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="Keine Standorte fuer dieses Taxi gefunden")
        return rows
    
@app.get("/taxis/{taxi_id}/times")
def get_last_location_timed(
    taxi_id: int,
    time_interval: int = Query(5, ge=1, le=1000),
    number: int = Query(20, ge=1, le=1000),
):
    """Return up to `number` evenly distributed samples from the last `time_interval` minutes."""

    sql = """
        SELECT taxi_id, event_timestamp, longitude, latitude, speed,
               average_speed, total_distance, is_speeding,
               is_out_of_area, is_parking
        FROM taxi_speed
        WHERE taxi_id = %s
          AND event_timestamp >= (
              SELECT MAX(event_timestamp) - (%s || ' minutes')::interval
              FROM taxi_speed
              WHERE taxi_id = %s
          )
        ORDER BY event_timestamp ASC
    """

    with get_conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(sql, (taxi_id, time_interval, taxi_id))
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="Keine Standorte fuer dieses Taxi gefunden",
        )

    if len(rows) <= number:
        return rows

    if number == 1:
        return [rows[-1]]

    last_index = len(rows) - 1

    # Evenly spaced indices including first and last
    indices = []
    for i in range(number):
        idx = round(i * last_index / (number - 1))
        if not indices or idx != indices[-1]:
            indices.append(idx)

    # Fill gaps if rounding produced duplicates
    current = 0
    while len(indices) < number:
        while current in indices:
            current += 1
        indices.append(current)

    indices.sort()

    return [rows[i] for i in indices]

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/taxis")
def list_latest_positions():
    """Letzte bekannte Position pro Taxi."""
    sql = """
        SELECT DISTINCT ON (taxi_id) *
        FROM taxi_speed
        ORDER BY taxi_id, received_at DESC
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


@app.get("/taxis/{taxi_id}")
def get_latest_position(taxi_id: int):
    sql = """
        SELECT * FROM taxi_speed
        WHERE taxi_id = %s
        ORDER BY received_at DESC
        LIMIT 1
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (taxi_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Taxi nicht gefunden")
        return row


@app.get("/taxis/{taxi_id}/history")
def get_history(taxi_id: int, limit: int = Query(100, le=1000)):
    sql = """
        SELECT * FROM taxi_speed
        WHERE taxi_id = %s
        ORDER BY received_at DESC
        LIMIT %s
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (taxi_id, limit))
        return cur.fetchall()


@app.get("/taxis/speeding")
def list_speeding():
    sql = """
        SELECT DISTINCT ON (taxi_id) * FROM taxi_speed
        WHERE is_speeding
        ORDER BY taxi_id, received_at DESC
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


@app.get("/taxis/out-of-area")
def list_out_of_area():
    sql = """
        SELECT DISTINCT ON (taxi_id) * FROM taxi_speed
        WHERE is_out_of_area
        ORDER BY taxi_id, received_at DESC
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


@app.get("/stats/latency")
def latency_stats():
    """Durchschnittliche/max. End-to-End Pipeline-Latenz der letzten 5 Minuten,
    basierend auf dem ingested_at-Feld aus TaxiSpeed."""
    sql = """
        SELECT
            AVG(EXTRACT(EPOCH FROM received_at) * 1000 - ingested_at) AS avg_latency_ms,
            MAX(EXTRACT(EPOCH FROM received_at) * 1000 - ingested_at) AS max_latency_ms,
            COUNT(*) AS sample_size
        FROM taxi_speed
        WHERE received_at > now() - interval '5 minutes'
    """
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchone()