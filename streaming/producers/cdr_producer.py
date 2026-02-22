"""
CDR (Call Detail Record) Kafka Producer
========================================
Simulates a telecom network continuously emitting call/data session events
from cell towers at high throughput. In production this would be replaced
by actual network probe equipment or a Kafka Connect source connector.

Topics produced:
  - call_events       : voice call start/end events
  - data_session_events: mobile data session events
  - sms_events        : SMS delivery events
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Generator

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

TOPICS = {
    "calls": "call_events",
    "data": "data_session_events",
    "sms": "sms_events",
}

# Simulated network geography — towers mapped to regions
TOWERS = [
    {"tower_id": "SEA-001", "city": "Seattle", "state": "WA", "lat": 47.6062, "lon": -122.3321, "region": "Northwest"},
    {"tower_id": "SEA-047", "city": "Seattle", "state": "WA", "lat": 47.6205, "lon": -122.3493, "region": "Northwest"},
    {"tower_id": "PDX-012", "city": "Portland", "state": "OR", "lat": 45.5051, "lon": -122.6750, "region": "Northwest"},
    {"tower_id": "LAX-088", "city": "Los Angeles", "state": "CA", "lat": 34.0522, "lon": -118.2437, "region": "Southwest"},
    {"tower_id": "SFO-033", "city": "San Francisco", "state": "CA", "lat": 37.7749, "lon": -122.4194, "region": "Southwest"},
    {"tower_id": "NYC-201", "city": "New York", "state": "NY", "lat": 40.7128, "lon": -74.0060, "region": "Northeast"},
    {"tower_id": "NYC-202", "city": "New York", "state": "NY", "lat": 40.7589, "lon": -73.9851, "region": "Northeast"},
    {"tower_id": "CHI-055", "city": "Chicago", "state": "IL", "lat": 41.8781, "lon": -87.6298, "region": "Midwest"},
    {"tower_id": "DAL-099", "city": "Dallas", "state": "TX", "lat": 32.7767, "lon": -96.7970, "region": "South"},
    {"tower_id": "MIA-077", "city": "Miami", "state": "FL", "lat": 25.7617, "lon": -80.1918, "region": "Southeast"},
    {"tower_id": "DEN-044", "city": "Denver", "state": "CO", "lat": 39.7392, "lon": -104.9903, "region": "Mountain"},
    {"tower_id": "PHX-066", "city": "Phoenix", "state": "AZ", "lat": 33.4484, "lon": -112.0740, "region": "Southwest"},
    {"tower_id": "BOS-031", "city": "Boston", "state": "MA", "lat": 42.3601, "lon": -71.0589, "region": "Northeast"},
    {"tower_id": "ATL-082", "city": "Atlanta", "state": "GA", "lat": 33.7490, "lon": -84.3880, "region": "Southeast"},
]

PLANS = ["Basic", "Standard", "Premium", "Unlimited", "Business"]
CALL_RESULTS = {
    "0": "SUCCESS",
    "17": "BUSY",
    "21": "CALL_REJECTED",
    "31": "NETWORK_FAILURE",
    "38": "NETWORK_OUT_OF_ORDER",
    "41": "TEMPORARY_FAILURE",
}
CONTENT_TYPES = ["streaming_video", "social_media", "web_browsing", "voip", "gaming", "file_download"]


def random_msisdn() -> str:
    """Generate a realistic-looking MSISDN (phone number)."""
    area_codes = ["206", "425", "503", "213", "415", "212", "312", "214", "305", "720"]
    return f"+1{random.choice(area_codes)}{random.randint(1000000, 9999999)}"


def random_customer_id() -> str:
    return f"CUST-{random.randint(10000000, 99999999)}"


# ── Event generators ──────────────────────────────────────────────────────────

def generate_call_event() -> dict:
    tower = random.choice(TOWERS)
    result_code = random.choices(
        list(CALL_RESULTS.keys()),
        weights=[85, 5, 3, 3, 2, 2],  # SUCCESS is most common
    )[0]
    duration = random.randint(5, 3600) if result_code == "0" else 0
    ts = datetime.now(timezone.utc)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "CALL",
        "caller_msisdn": random_msisdn(),
        "called_msisdn": random_msisdn(),
        "caller_customer_id": random_customer_id(),
        "tower_id": tower["tower_id"],
        "tower_city": tower["city"],
        "tower_state": tower["state"],
        "tower_lat": tower["lat"],
        "tower_lon": tower["lon"],
        "call_start_timestamp": ts.isoformat(),
        "call_end_timestamp": None if result_code != "0" else ts.isoformat(),
        "duration_seconds": duration,
        "call_result_code": result_code,
        "call_result_desc": CALL_RESULTS[result_code],
        "plan_type": random.choice(PLANS),
        "roaming": random.random() < 0.08,  # 8% of calls are roaming
        "revenue_usd": round(duration * 0.02, 4) if result_code == "0" else 0.0,
        "ingested_at": ts.isoformat(),
    }


def generate_data_session_event() -> dict:
    tower = random.choice(TOWERS)
    duration = random.randint(30, 7200)
    bytes_dl = random.randint(1024, 500_000_000)
    bytes_ul = random.randint(512, 50_000_000)
    ts = datetime.now(timezone.utc)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "DATA_SESSION",
        "subscriber_msisdn": random_msisdn(),
        "customer_id": random_customer_id(),
        "tower_id": tower["tower_id"],
        "tower_city": tower["city"],
        "tower_state": tower["state"],
        "session_start_timestamp": ts.isoformat(),
        "session_duration_seconds": duration,
        "bytes_downloaded": bytes_dl,
        "bytes_uploaded": bytes_ul,
        "total_bytes": bytes_dl + bytes_ul,
        "content_type": random.choice(CONTENT_TYPES),
        "radio_technology": random.choice(["4G_LTE", "5G_NR", "5G_NR", "5G_MMWAVE"]),  # 5G weighted
        "signal_strength_dbm": random.randint(-110, -50),
        "packet_loss_pct": round(random.uniform(0, 5), 2),
        "latency_ms": random.randint(5, 200),
        "plan_type": random.choice(PLANS),
        "roaming": random.random() < 0.05,
        "revenue_usd": round((bytes_dl + bytes_ul) / 1_000_000 * 0.001, 6),
        "ingested_at": ts.isoformat(),
    }


def generate_sms_event() -> dict:
    tower = random.choice(TOWERS)
    ts = datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "SMS",
        "sender_msisdn": random_msisdn(),
        "receiver_msisdn": random_msisdn(),
        "sender_customer_id": random_customer_id(),
        "tower_id": tower["tower_id"],
        "sms_type": random.choice(["P2P", "A2P"]),  # person-to-person vs application-to-person
        "delivery_status": random.choices(["DELIVERED", "FAILED", "PENDING"], weights=[92, 5, 3])[0],
        "message_size_bytes": random.randint(10, 160),
        "timestamp": ts.isoformat(),
        "ingested_at": ts.isoformat(),
    }


# ── Producer ──────────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")


def run_producer(events_per_second: int = 500, duration_seconds: int = 3600):
    """
    Continuously produce telecom events to Kafka.

    Args:
        events_per_second: Target throughput (approximate).
        duration_seconds: How long to run. Set to 0 for infinite.
    """
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 10,          # small batching window for throughput
        "batch.num.messages": 1000,
        "compression.type": "snappy",
        "acks": "1",
    })

    generators = [
        (TOPICS["calls"], generate_call_event, 0.30),      # 30% calls
        (TOPICS["data"], generate_data_session_event, 0.60),  # 60% data
        (TOPICS["sms"], generate_sms_event, 0.10),         # 10% SMS
    ]

    sleep_interval = 1.0 / events_per_second
    start_time = time.time()
    total_produced = 0

    print(f"Starting CDR producer: {events_per_second} events/sec target")
    print(f"Topics: {list(TOPICS.values())}")

    try:
        while True:
            if duration_seconds > 0 and (time.time() - start_time) > duration_seconds:
                break

            # Pick event type by weight
            r = random.random()
            cumulative = 0.0
            topic, generator, _ = generators[-1]
            for t, g, weight in generators:
                cumulative += weight
                if r < cumulative:
                    topic, generator = t, g
                    break

            event = generator()
            key = event.get("caller_msisdn") or event.get("subscriber_msisdn") or event.get("sender_msisdn", "")

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )

            total_produced += 1
            if total_produced % 10_000 == 0:
                producer.poll(0)
                elapsed = time.time() - start_time
                print(f"  Produced {total_produced:,} events in {elapsed:.1f}s "
                      f"({total_produced / elapsed:.0f} events/sec)")

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush(timeout=10)
        print(f"Total events produced: {total_produced:,}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Telecom CDR Kafka Producer")
    parser.add_argument("--rate", type=int, default=100, help="Events per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration seconds (0=infinite)")
    args = parser.parse_args()
    run_producer(events_per_second=args.rate, duration_seconds=args.duration)
