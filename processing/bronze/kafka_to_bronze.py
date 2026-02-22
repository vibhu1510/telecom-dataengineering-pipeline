"""
Kafka → Bronze Layer Consumer
==============================
Reads raw events from Kafka topics and writes them to MinIO (Bronze layer)
as Parquet files. This is the landing zone — NO transformations applied,
data is stored exactly as received.

In production this could be replaced with:
  - Kafka Connect S3 Sink Connector (zero-code)
  - AWS Kinesis Firehose
  - Databricks Auto Loader

Runs continuously as a micro-batch consumer (flush every N records or T seconds).
"""

import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from confluent_kafka import Consumer, KafkaException, TopicPartition

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BRONZE_BUCKET = "telecom-bronze"

TOPICS = ["call_events", "data_session_events", "sms_events", "network_probe_events"]

# Flush to S3 when either threshold is hit
FLUSH_RECORD_THRESHOLD = 10_000   # records per topic
FLUSH_TIME_THRESHOLD_SEC = 300    # 5 minutes


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def flush_buffer_to_s3(topic: str, records: List[dict], s3_client, run_ts: datetime) -> str:
    """Write a batch of raw JSON records as Parquet to the Bronze layer."""
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Hive-style partitioning by date so Spark/Trino can use partition pruning
    s3_key = (
        f"events/{topic}/"
        f"year={run_ts.year}/month={run_ts.month:02d}/day={run_ts.day:02d}/"
        f"{topic}_{run_ts.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
    )

    local_path = f"/tmp/{uuid.uuid4().hex}.parquet"
    pq.write_table(table, local_path, compression="snappy")

    with open(local_path, "rb") as f:
        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=f,
            Metadata={
                "topic": topic,
                "record_count": str(len(records)),
                "flush_timestamp": run_ts.isoformat(),
            },
        )

    os.unlink(local_path)
    s3_path = f"s3://{BRONZE_BUCKET}/{s3_key}"
    print(f"  [{topic}] Flushed {len(records):,} records → {s3_path}")
    return s3_path


def run_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "bronze-layer-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,     # we commit manually after successful S3 write
        "max.poll.interval.ms": 600_000,
        "session.timeout.ms": 45_000,
    })

    consumer.subscribe(TOPICS)
    s3_client = get_s3_client()

    # In-memory buffers per topic
    buffers: Dict[str, List[dict]] = defaultdict(list)
    last_flush_time: Dict[str, float] = {t: time.time() for t in TOPICS}
    total_consumed = 0

    print(f"Bronze consumer started. Subscribing to: {TOPICS}")
    print(f"Flush thresholds: {FLUSH_RECORD_THRESHOLD:,} records OR {FLUSH_TIME_THRESHOLD_SEC}s")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Timeout — check if any buffers need time-based flush
                pass
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                topic = msg.topic()
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    # Add Kafka metadata to the raw record
                    record["_kafka_topic"] = topic
                    record["_kafka_partition"] = msg.partition()
                    record["_kafka_offset"] = msg.offset()
                    record["_kafka_timestamp"] = msg.timestamp()[1]
                    record["_bronze_ingested_at"] = datetime.now(timezone.utc).isoformat()
                    buffers[topic].append(record)
                    total_consumed += 1
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    print(f"  [WARN] Failed to parse message: {e}")

            # Check flush conditions for each topic
            now = time.time()
            for topic in list(buffers.keys()):
                buf = buffers[topic]
                time_since_flush = now - last_flush_time.get(topic, now)
                should_flush = (
                    len(buf) >= FLUSH_RECORD_THRESHOLD
                    or (len(buf) > 0 and time_since_flush >= FLUSH_TIME_THRESHOLD_SEC)
                )
                if should_flush:
                    flush_ts = datetime.now(timezone.utc)
                    flush_buffer_to_s3(topic, buf, s3_client, flush_ts)
                    buffers[topic] = []
                    last_flush_time[topic] = now
                    consumer.commit(asynchronous=False)

            if total_consumed % 50_000 == 0 and total_consumed > 0:
                print(f"  Total consumed: {total_consumed:,}")

    except KeyboardInterrupt:
        print("\nShutting down consumer, flushing remaining buffers...")
        for topic, buf in buffers.items():
            if buf:
                flush_buffer_to_s3(topic, buf, s3_client, datetime.now(timezone.utc))
        consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        print(f"Consumer closed. Total records consumed: {total_consumed:,}")


if __name__ == "__main__":
    run_consumer()
