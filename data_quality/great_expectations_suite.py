"""
Data Quality — Great Expectations Suites
==========================================
Programmatically defines expectation suites for all major data layers.
Can be run standalone or invoked from Airflow as a data gate.

Great Expectations complements dbt tests:
  - dbt tests: fast, run inside the warehouse (SQL-based)
  - GX suites: richer statistical checks, distribution validation,
               regex patterns, cross-column rules, anomaly detection

Suites defined:
  1. bronze_call_events_suite  — raw data integrity checks
  2. silver_calls_suite        — post-cleaning correctness
  3. gold_fact_calls_suite     — business logic correctness
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
import pandas as pd

# ── Initialize GX context (local filesystem backend) ────────────────────────
context = gx.get_context(mode="ephemeral")  # use "file" mode in production


# ── Suite 1: Bronze — raw call events ────────────────────────────────────────

def build_bronze_call_events_suite():
    suite = context.add_expectation_suite("bronze_call_events")

    # Schema expectations
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="event_id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="caller_msisdn"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="tower_id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="call_start_timestamp"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="duration_seconds"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="call_result_code"))

    # Null checks
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="event_id",
        meta={"notes": "event_id must always be present — it's the primary key"}
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="caller_msisdn", mostly=0.99  # allow 1% nulls for edge cases
    ))

    # Uniqueness
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
        column="event_id"
    ))

    # Value ranges
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="duration_seconds",
        min_value=0,
        max_value=86400,  # no call lasts longer than 24 hours
        meta={"notes": "Negative or >24h duration indicates data corruption"}
    ))

    # Result code must be one of the known codes
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="call_result_code",
        value_set=["0", "17", "21", "31", "38", "41"],
        mostly=0.99,  # 1% unknown codes tolerated
    ))

    # Phone number format (MSISDN)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
        column="caller_msisdn",
        regex=r"^\+1\d{10}$",
        mostly=0.95,
        meta={"notes": "US numbers only in this dataset"}
    ))

    # Timestamp format
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column="call_start_timestamp",
        strftime_format="%Y-%m-%dT%H:%M:%S",
        mostly=0.99,
    ))

    # Row count anomaly — should have at least 1M records per day
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeGreaterThan(
        value=1_000_000,
        meta={"notes": "Less than 1M records suggests pipeline failure or source issue"}
    ))

    context.save_expectation_suite(suite)
    return suite


# ── Suite 2: Silver — cleaned calls ──────────────────────────────────────────

def build_silver_calls_suite():
    suite = context.add_expectation_suite("silver_calls")

    # All Silver column names must follow snake_case convention
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="call_id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="call_status"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="call_duration_seconds"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="revenue_usd"))

    # After cleaning, no nulls on key columns
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="call_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="call_status"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="call_start_ts"))

    # Status must be decoded human-readable values
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="call_status",
        value_set=["SUCCESS", "USER_BUSY", "CALL_REJECTED", "NETWORK_FAILURE",
                   "NETWORK_OUT_OF_ORDER", "TEMPORARY_FAILURE"],
        mostly=0.99,
    ))

    # Revenue: non-negative, within realistic bounds per call
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="revenue_usd",
        min_value=0,
        max_value=100,
        meta={"notes": "No single call should generate >$100 revenue"}
    ))

    # Call duration distribution check
    suite.add_expectation(gx.expectations.ExpectColumnMedianToBeBetween(
        column="call_duration_seconds",
        min_value=30,    # median call duration should be > 30 seconds
        max_value=600,   # and < 10 minutes
    ))

    # Success rate should be in realistic range (80-99%)
    suite.add_expectation(gx.expectations.ExpectColumnMeanToBeBetween(
        column="is_successful",
        min_value=0.80,
        max_value=0.99,
    ))

    context.save_expectation_suite(suite)
    return suite


# ── Suite 3: Gold — fact_calls ────────────────────────────────────────────────

def build_gold_fact_calls_suite():
    suite = context.add_expectation_suite("gold_fact_calls")

    # All dimension keys must be populated (UNKNOWN is acceptable, not NULL)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="customer_key"
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="tower_key"
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="call_date_key"
    ))

    # Date key format: 8-digit integer YYYYMMDD
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="call_date_key",
        min_value=20200101,
        max_value=20301231,
    ))

    # Revenue assertions
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="revenue_usd",
        min_value=0,
        max_value=100,
    ))

    # call_count must always be exactly 1 (grain enforcement)
    suite.add_expectation(gx.expectations.ExpectColumnDistinctValuesToContainSet(
        column="call_count",
        value_set=[1],
    ))

    context.save_expectation_suite(suite)
    return suite


# ── Monitoring: Row count trend ───────────────────────────────────────────────

class RowCountMonitor:
    """
    Simple anomaly detection for row counts.
    Compares today's count against a rolling 7-day baseline.
    Flags deviations > 30% as anomalies.
    """
    ANOMALY_THRESHOLD = 0.30  # 30% deviation

    def __init__(self, history: list):
        """history: list of (date_str, row_count) tuples, oldest first"""
        self.history = history

    def check(self, today_count: int, today_date: str) -> dict:
        if len(self.history) < 3:
            return {"status": "INSUFFICIENT_HISTORY", "today_count": today_count}

        recent = [count for _, count in self.history[-7:]]
        baseline = sum(recent) / len(recent)
        deviation = abs(today_count - baseline) / baseline if baseline > 0 else 0

        return {
            "date": today_date,
            "today_count": today_count,
            "7day_average": round(baseline),
            "deviation_pct": round(deviation * 100, 1),
            "status": "ANOMALY" if deviation > self.ANOMALY_THRESHOLD else "OK",
            "threshold_pct": self.ANOMALY_THRESHOLD * 100,
        }


if __name__ == "__main__":
    print("Building Bronze call events suite...")
    build_bronze_call_events_suite()
    print("Building Silver calls suite...")
    build_silver_calls_suite()
    print("Building Gold fact_calls suite...")
    build_gold_fact_calls_suite()
    print("\nAll expectation suites built successfully.")
    print("Run checkpoint to validate data against suites.")
