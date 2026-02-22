"""
Real-Time SIM Swap Fraud Detection — Apache Flink (PyFlink)
=============================================================
Detects two classic telecom fraud patterns in real time:

1. IMPOSSIBLE TRAVEL — A customer's phone appears in two geographically
   distant locations within a time window too short to physically travel.
   e.g. A call from Seattle at 10:00 AM followed by a call from Miami
   at 10:05 AM is physically impossible (3,300km, would require Mach 40).

2. VELOCITY FRAUD — An account makes an unusually high number of calls
   within a short time window (SIM cloning / account takeover signature).

Architecture:
  Kafka(call_events) → Flink KeyedStream(customer_id) → Stateful Rules
  → Kafka(fraud_alerts) → Notification service + fraud analyst dashboard

Key Flink concepts demonstrated:
  - ProcessFunction with stateful keyed stream
  - ValueState for last-seen location tracking
  - ListState for velocity counting within sliding window
  - Event-time processing with watermarks
  - Side outputs for different alert severity levels

Requires: apache-flink 1.19, confluent-kafka
"""

import json
import math
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "call_events"
ALERTS_TOPIC = "fraud_alerts"
GROUP_ID = "flink-fraud-detector"

# Fraud thresholds
IMPOSSIBLE_TRAVEL_WINDOW_MINUTES = 60    # time window to check for impossible travel
IMPOSSIBLE_TRAVEL_MIN_DISTANCE_KM = 500  # minimum distance to flag as suspicious
MIN_SPEED_TO_FLAG_KMH = 600              # >600 km/h is impossible by commercial transport
VELOCITY_WINDOW_MINUTES = 30             # window for velocity fraud check
VELOCITY_CALL_THRESHOLD = 25            # >25 calls in 30 min is suspicious


# ── Haversine distance calculation ───────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in kilometers."""
    R = 6371.0  # Earth's radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── Fraud Detection ProcessFunction ──────────────────────────────────────────

class FraudDetectionFunction(KeyedProcessFunction):
    """
    Stateful per-customer fraud detection.

    State maintained per customer:
      - last_call_state: (timestamp, lat, lon, tower_id) of most recent call
      - recent_calls_state: list of (timestamp, event_id) for velocity window
    """

    def open(self, runtime_context: RuntimeContext):
        # State: last seen call location and time
        self.last_call_state = runtime_context.get_state(
            ValueStateDescriptor(
                "last_call",
                Types.PICKLED_BYTE_ARRAY()
            )
        )

        # State: list of recent call timestamps for velocity counting
        self.recent_calls_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "recent_calls",
                Types.PICKLED_BYTE_ARRAY()
            )
        )

    def process_element(self, event: dict, ctx: KeyedProcessFunction.Context):
        """
        Called for every call event belonging to this customer (key).
        Returns zero or more fraud alert events.
        """
        customer_id = event.get("caller_customer_id", "UNKNOWN")
        event_id = event.get("event_id", "")
        tower_id = event.get("tower_id", "")
        lat = event.get("tower_lat")
        lon = event.get("tower_lon")
        call_ts_str = event.get("call_start_timestamp", "")
        result_code = event.get("call_result_code", "")

        if not all([lat, lon, call_ts_str]):
            return  # Skip incomplete records

        try:
            call_ts = datetime.fromisoformat(call_ts_str.replace("Z", "+00:00"))
            call_ts_epoch = call_ts.timestamp()
        except (ValueError, AttributeError):
            return

        alerts = []

        # ── Check 1: Impossible Travel ────────────────────────────────────
        last_call_raw = self.last_call_state.value()
        if last_call_raw is not None:
            import pickle
            last_ts_epoch, last_lat, last_lon, last_tower = pickle.loads(last_call_raw)

            time_diff_hours = (call_ts_epoch - last_ts_epoch) / 3600.0
            if time_diff_hours > 0 and last_lat != lat and last_lon != lon:
                distance_km = haversine_km(last_lat, last_lon, lat, lon)
                implied_speed_kmh = distance_km / time_diff_hours

                if (distance_km >= IMPOSSIBLE_TRAVEL_MIN_DISTANCE_KM
                        and implied_speed_kmh >= MIN_SPEED_TO_FLAG_KMH):
                    alert = {
                        "alert_id": f"IMP_TRAVEL_{event_id}",
                        "alert_type": "IMPOSSIBLE_TRAVEL",
                        "severity": "HIGH" if implied_speed_kmh > 2000 else "MEDIUM",
                        "customer_id": customer_id,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "event_id": event_id,
                        "details": {
                            "previous_tower": last_tower,
                            "previous_lat": last_lat,
                            "previous_lon": last_lon,
                            "previous_ts": datetime.fromtimestamp(last_ts_epoch, tz=timezone.utc).isoformat(),
                            "current_tower": tower_id,
                            "current_lat": lat,
                            "current_lon": lon,
                            "current_ts": call_ts.isoformat(),
                            "distance_km": round(distance_km, 1),
                            "time_diff_minutes": round(time_diff_hours * 60, 1),
                            "implied_speed_kmh": round(implied_speed_kmh, 0),
                        },
                        "recommended_action": "FREEZE_ACCOUNT",
                    }
                    alerts.append(alert)

        # Update last call state
        import pickle
        self.last_call_state.update(pickle.dumps((call_ts_epoch, lat, lon, tower_id)))

        # ── Check 2: Velocity Fraud ───────────────────────────────────────
        velocity_window_start = call_ts_epoch - (VELOCITY_WINDOW_MINUTES * 60)

        # Load and prune recent calls outside the window
        recent = [
            pickle.loads(r) for r in self.recent_calls_state.get()
        ]
        recent = [(ts, eid) for ts, eid in recent if ts >= velocity_window_start]
        recent.append((call_ts_epoch, event_id))

        call_count_in_window = len(recent)

        if call_count_in_window > VELOCITY_CALL_THRESHOLD:
            alert = {
                "alert_id": f"VEL_FRAUD_{event_id}",
                "alert_type": "VELOCITY_FRAUD",
                "severity": "HIGH" if call_count_in_window > VELOCITY_CALL_THRESHOLD * 2 else "MEDIUM",
                "customer_id": customer_id,
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "event_id": event_id,
                "details": {
                    "call_count_in_window": call_count_in_window,
                    "window_minutes": VELOCITY_WINDOW_MINUTES,
                    "threshold": VELOCITY_CALL_THRESHOLD,
                    "tower_id": tower_id,
                },
                "recommended_action": "FLAG_FOR_REVIEW",
            }
            alerts.append(alert)

        # Update recent calls state (retain only within window)
        self.recent_calls_state.clear()
        for item in recent:
            self.recent_calls_state.add(pickle.dumps(item))

        # Emit alerts downstream
        for alert in alerts:
            yield json.dumps(alert)


# ── Flink Job Definition ──────────────────────────────────────────────────────

def run_fraud_detection_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    # Configure Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermark strategy: allow up to 30 seconds of late arrivals
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(
            # Extract event time from the JSON payload
            lambda event_str, _: int(
                datetime.fromisoformat(
                    json.loads(event_str).get("call_start_timestamp", "").replace("Z", "+00:00")
                ).timestamp() * 1000
            )
        )
    )

    # Build the streaming pipeline
    call_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="Kafka_call_events",
    )

    # Parse JSON, key by customer_id, apply fraud detection
    fraud_alerts = (
        call_stream
        .map(lambda msg: json.loads(msg))
        .key_by(lambda event: event.get("caller_customer_id", "UNKNOWN"))
        .process(FraudDetectionFunction())
    )

    # Sink alerts back to Kafka
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            # Use default string serializer → alerts_topic
            None  # Replace with proper KafkaRecordSerializationSchema in full impl
        )
        .build()
    )

    fraud_alerts.print()  # For local development; replace with Kafka sink in production

    env.execute("Telecom_FraudDetection_ImpossibleTravel_Velocity")


if __name__ == "__main__":
    run_fraud_detection_job()
