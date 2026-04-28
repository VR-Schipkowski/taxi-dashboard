"""
Taxi Data Provider – Kafka Producer (streaming / memory-efficient)
------------------------------------------------------------------
Instead of loading all records into RAM, we open every .txt file as a
lazy iterator and merge them on-the-fly with a min-heap (heapq.merge).
Memory usage stays O(number_of_files) regardless of total data size.

Data format per line:  taxiId,timestamp,longitude,latitude
Example:               1,2008-02-02 15:36:08,116.51172,39.92123
"""

import os
import time
import json
import heapq
import argparse
import logging
from datetime import datetime
from itertools import groupby

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
END_TOKEN     = "__END__"
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# Per-file lazy iterator
# ---------------------------------------------------------------------------

def _file_records(path: str):
    """
    Generator: yields (datetime, taxiId, longitude, latitude) tuples from
    one taxi file, in the order they appear in the file.
    The file is assumed to be sorted by timestamp already (T-Drive files are).
    If a line is malformed it is skipped with a warning.
    """
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) != 4:
                log.warning("Skipping malformed line in %s: %r", path, line)
                continue
            taxi_id_str, ts_str, lon_str, lat_str = parts
            try:
                dt  = datetime.strptime(ts_str.strip(), TIMESTAMP_FMT)
                yield (dt, int(taxi_id_str), float(lon_str), float(lat_str))
            except ValueError as exc:
                log.warning("Parse error in %s (%s): %r", path, exc, line)


# ---------------------------------------------------------------------------
# Merged stream
# ---------------------------------------------------------------------------

def merged_stream(data_dir: str):
    """
    Opens all .txt files and merges their record streams by timestamp using
    a min-heap.  Peak RAM = one record per open file.

    Yields (datetime, taxiId, longitude, latitude) in ascending timestamp order.
    """
    txt_files = sorted(
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.endswith(".txt")
    )
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in {data_dir!r}")

    log.info("Opening %d taxi files for streaming ...", len(txt_files))

    # heapq.merge compares tuples lexicographically; datetime is the first
    # element so records are merged in timestamp order automatically.
    return heapq.merge(*(_file_records(p) for p in txt_files))


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

from kafka.errors import KafkaError


def make_producer(bootstrap_servers: str) -> KafkaProducer:
    log.info("Connecting to Kafka at %s...", bootstrap_servers)
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
                retries=5,
                retry_backoff_ms=300,
                request_timeout_ms=10000 # Erhöhtes Timeout für den Erstkontakt
            )
            log.info("Successfully connected to Kafka!")
            return producer
        except Exception:
            log.warning("Kafka not available yet, retrying in 2 seconds...")
            time.sleep(2)


def _send(producer: KafkaProducer, topic: str, taxi_id: int, payload: dict):
    producer.send(topic, key=taxi_id, value=payload).add_errback(
        lambda exc: log.error("Send failed: %s", exc)
    )


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def replay(
    producer: KafkaProducer,
    topic: str,
    data_dir: str,
    speed_factor: float,
):
    """
    Streams all taxi files merged by timestamp and publishes to Kafka.

    Timing:
        speed_factor > 1  -> faster than real-time  (e.g. 10 = 10x)
        speed_factor = 1  -> real-time
        speed_factor = 0  -> flood / benchmark mode (no sleep)
    """
    stream = merged_stream(data_dir)

    wall_start: float | None = None
    data_start: datetime | None = None
    total_sent = 0
    last_seen: dict[int, datetime] = {}   # taxiId -> last timestamp seen

    try:
        for batch_dt, batch_iter in groupby(stream, key=lambda r: r[0]):
            # --- timing ------------------------------------------------------
            if wall_start is None:
                wall_start = time.monotonic()
                data_start = batch_dt
            elif speed_factor > 0:
                data_elapsed = (batch_dt - data_start).total_seconds()
                wall_elapsed = time.monotonic() - wall_start
                sleep_for    = (data_elapsed / speed_factor) - wall_elapsed
                if sleep_for > 0:
                    time.sleep(sleep_for)

            # --- publish all records in this timestamp batch ------------------
            for dt, taxi_id, lon, lat in batch_iter:
                _send(producer, topic, taxi_id, {
                    "taxi_id":    taxi_id,
                    "timestamp": dt.strftime(TIMESTAMP_FMT),
                    "longitude": lon,
                    "latitude":  lat,
                })
                last_seen[taxi_id] = dt
                total_sent += 1

            producer.flush()

            if total_sent % 5_000 == 0:
                log.info("Published %d records ...", total_sent)

    finally:
        # Send END token for every taxi we saw
        for taxi_id in last_seen:
            _send(producer, topic, taxi_id, {
                "taxiId": taxi_id,
                "type":   END_TOKEN,
            })
        producer.flush()
        log.info("Done. Total records sent: %d | END tokens: %d",
                 total_sent, len(last_seen))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="T-Drive Taxi -> Kafka producer (streaming, low memory)."
    )
    p.add_argument("--data-dir",          "-d", required=True,
                   help="Directory containing the T-Drive .txt files.")
    p.add_argument("--bootstrap-servers", "-b", default="localhost:9092",
                   help="Kafka bootstrap servers (default: localhost:9092).")
    p.add_argument("--topic",             "-t", default="taxi-locations",
                   help="Kafka topic name (default: taxi-locations).")
    p.add_argument("--speed-factor",      "-s", type=float, default=1.0,
                   help="Replay speed multiplier. 1=real-time, 0=flood mode.")
    return p


def main():
    args = build_arg_parser().parse_args()

    log.info("=== Taxi Data Provider (streaming) ===")
    log.info("Data dir   : %s", args.data_dir)
    log.info("Brokers    : %s", args.bootstrap_servers)
    log.info("Topic      : %s", args.topic)
    log.info("Speed      : %sx", args.speed_factor)

    producer = make_producer(args.bootstrap_servers)
    try:
        replay(
            producer=producer,
            topic=args.topic,
            data_dir=args.data_dir,
            speed_factor=args.speed_factor,
        )
    finally:
        producer.close()
        log.info("Producer closed.")


if __name__ == "__main__":
    main()