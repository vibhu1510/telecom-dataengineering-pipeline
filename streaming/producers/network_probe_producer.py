"""
Network Probe Event Producer
=============================
Emits tower-level KPI metrics (signal strength, load, handoffs, failures)
to Kafka. Used by the real-time network ops dashboard and anomaly detection.

Topic: network_probe_events
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "network_probe_events"

TOWERS = [
    {"tower_id": "SEA-001", "region": "Northwest", "capacity_erlangs": 500},
    {"tower_id": "SEA-047", "region": "Northwest", "capacity_erlangs": 350},
    {"tower_id": "PDX-012", "region": "Northwest", "capacity_erlangs": 420},
    {"tower_id": "LAX-088", "region": "Southwest", "capacity_erlangs": 800},
    {"tower_id": "SFO-033", "region": "Southwest", "capacity_erlangs": 600},
    {"tower_id": "NYC-201", "region": "Northeast", "capacity_erlangs": 1200},
    {"tower_id": "NYC-202", "region": "Northeast", "capacity_erlangs": 1100},
    {"tower_id": "CHI-055", "region": "Midwest", "capacity_erlangs": 700},
    {"tower_id": "DAL-099", "region": "South", "capacity_erlangs": 650},
    {"tower_id": "MIA-077", "region": "Southeast", "capacity_erlangs": 550},
]

# Tower state — simulate gradual degradation for some towers
_tower_state = {t["tower_id"]: {"load_pct": random.uniform(20, 60)} for t in TOWERS}


def _evolve_tower_state():
    """Slowly drift tower load to simulate real-world conditions."""
    for tower_id, state in _tower_state.items():
        delta = random.gauss(0, 2)
        state["load_pct"] = max(5, min(100, state["load_pct"] + delta))

        # Occasionally inject a spike (simulates a sports event, outage, etc.)
        if random.random() < 0.001:
            state["load_pct"] = min(100, state["load_pct"] + random.uniform(20, 40))
            print(f"  [SPIKE] Tower {tower_id} load spike: {state['load_pct']:.1f}%")


def generate_probe_event(tower: dict) -> dict:
    tower_id = tower["tower_id"]
    load_pct = _tower_state[tower_id]["load_pct"]
    capacity = tower["capacity_erlangs"]
    ts = datetime.now(timezone.utc)

    # Signal quality degrades under high load
    noise_factor = 1 + (load_pct / 100) * 0.3

    return {
        "probe_id": str(uuid.uuid4()),
        "tower_id": tower_id,
        "region": tower["region"],
        "measurement_timestamp": ts.isoformat(),
        "load_pct": round(load_pct, 2),
        "active_connections": int(capacity * load_pct / 100),
        "capacity_erlangs": capacity,
        "avg_signal_strength_dbm": round(random.gauss(-75, 5) * noise_factor, 2),
        "avg_sinr_db": round(random.gauss(15, 3) / noise_factor, 2),   # signal-to-interference ratio
        "call_setup_success_rate_pct": round(max(90, 99.5 - load_pct * 0.05), 2),
        "call_drop_rate_pct": round(max(0, 0.1 + load_pct * 0.01), 3),
        "handoff_success_rate_pct": round(max(95, 99.0 - load_pct * 0.03), 2),
        "avg_throughput_mbps": round(max(1, random.gauss(150, 20) / noise_factor), 1),
        "packet_loss_pct": round(max(0, random.gauss(0.1, 0.05) * noise_factor), 3),
        "avg_latency_ms": round(random.gauss(20, 5) * noise_factor, 1),
        "power_output_watts": round(random.uniform(40, 60), 1),
        "temperature_celsius": round(random.gauss(45, 5) + load_pct * 0.2, 1),
        "software_version": "NOS-24.3.1",
        "alert_threshold_breached": load_pct > 85,
        "ingested_at": ts.isoformat(),
    }


def run_producer(interval_seconds: float = 1.0):
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 5,
        "compression.type": "snappy",
    })

    print(f"Network probe producer started — emitting every {interval_seconds}s per tower")

    try:
        while True:
            _evolve_tower_state()
            for tower in TOWERS:
                event = generate_probe_event(tower)
                producer.produce(
                    topic=TOPIC,
                    key=tower["tower_id"].encode(),
                    value=json.dumps(event).encode(),
                )
            producer.poll(0)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Shutting down network probe producer...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    run_producer(interval_seconds=1.0)
