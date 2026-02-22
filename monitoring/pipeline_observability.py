"""
Pipeline Observability — Custom Monitoring
==========================================
Implements the data observability pillars without a commercial tool:
  1. Freshness   — was data updated recently?
  2. Volume      — are row counts within expected range?
  3. Distribution — have column value distributions shifted unexpectedly?
  4. Schema      — have column names/types changed unexpectedly?
  5. Lineage     — trace data from source to consumption

In production this would integrate with Monte Carlo Data, Metaplane,
or a custom Grafana/Prometheus stack. Here we implement the core logic
to demonstrate the concepts.
"""

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from enum import Enum


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class ObservabilityCheck:
    check_name: str
    table_name: str
    severity: Severity
    status: str          # "PASSED" or "FAILED"
    value: float
    threshold: float
    details: str
    checked_at: str = ""

    def __post_init__(self):
        if not self.checked_at:
            self.checked_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        d = asdict(self)
        d["severity"] = self.severity.value
        return d


# ── 1. Freshness Monitor ──────────────────────────────────────────────────────

class FreshnessMonitor:
    """
    Checks that tables were updated within expected SLA windows.
    Different tables have different freshness expectations:
      - silver_network_probes: updated every 30 minutes
      - silver_calls: updated daily
      - fact_calls: updated daily
    """

    FRESHNESS_SLAS = {
        "silver_network_probes": timedelta(minutes=35),
        "silver_calls": timedelta(hours=3),
        "silver_customers": timedelta(hours=26),
        "fact_calls": timedelta(hours=4),
        "agg_tower_hourly_utilization": timedelta(minutes=35),
        "agg_customer_monthly_summary": timedelta(hours=26),
    }

    def check(self, table_name: str, last_updated: datetime) -> ObservabilityCheck:
        sla = self.FRESHNESS_SLAS.get(table_name, timedelta(hours=24))
        now = datetime.now(timezone.utc)
        age = now - last_updated
        passed = age <= sla

        return ObservabilityCheck(
            check_name="freshness",
            table_name=table_name,
            severity=Severity.CRITICAL if not passed else Severity.INFO,
            status="PASSED" if passed else "FAILED",
            value=age.total_seconds() / 60,    # age in minutes
            threshold=sla.total_seconds() / 60,
            details=(
                f"Table is {age.total_seconds()/60:.1f} min old "
                f"(SLA: {sla.total_seconds()/60:.0f} min)"
            ),
        )


# ── 2. Volume Monitor ─────────────────────────────────────────────────────────

class VolumeMonitor:
    """
    Detects anomalous row counts by comparing against a rolling baseline.
    Uses Z-score method: flag if today's count is more than N standard
    deviations from the recent mean.
    """

    Z_SCORE_THRESHOLD = 3.0  # flag if >3 std deviations from mean

    def check(
        self,
        table_name: str,
        today_count: int,
        historical_counts: List[int],  # last 14 days, oldest first
    ) -> ObservabilityCheck:
        if len(historical_counts) < 5:
            return ObservabilityCheck(
                check_name="volume",
                table_name=table_name,
                severity=Severity.INFO,
                status="PASSED",
                value=today_count,
                threshold=0,
                details="Insufficient history for anomaly detection",
            )

        import statistics
        mean = statistics.mean(historical_counts)
        stdev = statistics.stdev(historical_counts) if len(historical_counts) > 1 else 1
        z_score = abs(today_count - mean) / stdev if stdev > 0 else 0

        pct_change = (today_count - mean) / mean * 100 if mean > 0 else 0
        passed = z_score <= self.Z_SCORE_THRESHOLD

        return ObservabilityCheck(
            check_name="volume",
            table_name=table_name,
            severity=Severity.WARNING if not passed else Severity.INFO,
            status="PASSED" if passed else "FAILED",
            value=round(z_score, 2),
            threshold=self.Z_SCORE_THRESHOLD,
            details=(
                f"Today: {today_count:,} rows | "
                f"14-day avg: {mean:,.0f} | "
                f"Change: {pct_change:+.1f}% | "
                f"Z-score: {z_score:.2f}"
            ),
        )


# ── 3. Distribution Monitor ───────────────────────────────────────────────────

class DistributionMonitor:
    """
    Detects when column value distributions shift unexpectedly.
    Compares null rates, mean values, and cardinality changes.
    """

    def check_null_rate(
        self,
        table_name: str,
        column_name: str,
        current_null_rate: float,  # 0.0 to 1.0
        baseline_null_rate: float,
        tolerance: float = 0.05,   # flag if null rate changes by >5 percentage points
    ) -> ObservabilityCheck:
        delta = abs(current_null_rate - baseline_null_rate)
        passed = delta <= tolerance

        return ObservabilityCheck(
            check_name="null_rate",
            table_name=f"{table_name}.{column_name}",
            severity=Severity.WARNING if not passed else Severity.INFO,
            status="PASSED" if passed else "FAILED",
            value=round(current_null_rate * 100, 2),
            threshold=round((baseline_null_rate + tolerance) * 100, 2),
            details=(
                f"Current null rate: {current_null_rate*100:.2f}% | "
                f"Baseline: {baseline_null_rate*100:.2f}% | "
                f"Delta: {delta*100:.2f}pp (threshold: {tolerance*100:.0f}pp)"
            ),
        )

    def check_cardinality(
        self,
        table_name: str,
        column_name: str,
        current_distinct_count: int,
        baseline_distinct_count: int,
        tolerance_pct: float = 0.20,  # 20% cardinality change is suspicious
    ) -> ObservabilityCheck:
        if baseline_distinct_count == 0:
            return ObservabilityCheck(
                check_name="cardinality", table_name=f"{table_name}.{column_name}",
                severity=Severity.INFO, status="PASSED", value=current_distinct_count,
                threshold=0, details="No baseline available"
            )

        pct_change = abs(current_distinct_count - baseline_distinct_count) / baseline_distinct_count
        passed = pct_change <= tolerance_pct

        return ObservabilityCheck(
            check_name="cardinality",
            table_name=f"{table_name}.{column_name}",
            severity=Severity.WARNING if not passed else Severity.INFO,
            status="PASSED" if passed else "FAILED",
            value=current_distinct_count,
            threshold=baseline_distinct_count * (1 + tolerance_pct),
            details=(
                f"Current distinct values: {current_distinct_count:,} | "
                f"Baseline: {baseline_distinct_count:,} | "
                f"Change: {pct_change*100:.1f}% (threshold: {tolerance_pct*100:.0f}%)"
            ),
        )


# ── 4. Schema Monitor ─────────────────────────────────────────────────────────

class SchemaMonitor:
    """
    Detects unexpected schema changes: added/removed columns, type changes.
    Reads schema from a stored baseline and compares against current schema.
    """

    def check(
        self,
        table_name: str,
        current_schema: Dict[str, str],    # {column_name: data_type}
        baseline_schema: Dict[str, str],
    ) -> List[ObservabilityCheck]:
        checks = []

        # Check for removed columns (breaking change!)
        removed = set(baseline_schema) - set(current_schema)
        for col in removed:
            checks.append(ObservabilityCheck(
                check_name="schema_removed_column",
                table_name=table_name,
                severity=Severity.CRITICAL,
                status="FAILED",
                value=0,
                threshold=0,
                details=f"Column '{col}' ({baseline_schema[col]}) was REMOVED — BREAKING CHANGE",
            ))

        # Check for added columns (usually OK, but worth noting)
        added = set(current_schema) - set(baseline_schema)
        for col in added:
            checks.append(ObservabilityCheck(
                check_name="schema_added_column",
                table_name=table_name,
                severity=Severity.INFO,
                status="PASSED",
                value=1,
                threshold=0,
                details=f"New column '{col}' ({current_schema[col]}) was added",
            ))

        # Check for type changes (potentially breaking)
        for col in set(current_schema) & set(baseline_schema):
            if current_schema[col] != baseline_schema[col]:
                checks.append(ObservabilityCheck(
                    check_name="schema_type_change",
                    table_name=table_name,
                    severity=Severity.WARNING,
                    status="FAILED",
                    value=0,
                    threshold=0,
                    details=(
                        f"Column '{col}' type changed: "
                        f"{baseline_schema[col]} → {current_schema[col]}"
                    ),
                ))

        if not checks:
            checks.append(ObservabilityCheck(
                check_name="schema",
                table_name=table_name,
                severity=Severity.INFO,
                status="PASSED",
                value=len(current_schema),
                threshold=len(baseline_schema),
                details=f"Schema unchanged ({len(current_schema)} columns)",
            ))

        return checks


# ── 5. Lineage Tracker ────────────────────────────────────────────────────────

class LineageTracker:
    """
    Simple in-memory lineage graph.
    In production this would be OpenLineage events to Marquez or DataHub.
    """

    # Static lineage for our pipeline
    LINEAGE = {
        "agg_tower_hourly_utilization": ["fact_calls", "silver_network_probes"],
        "fact_calls": ["silver_calls", "dim_customers", "dim_towers", "dim_date"],
        "agg_customer_monthly_summary": ["fact_calls", "dim_customers"],
        "silver_calls": ["bronze_call_events"],
        "silver_customers": ["iceberg.bronze.crm_customers"],
        "silver_network_probes": ["iceberg.bronze.network_probe_events"],
        "bronze_call_events": ["kafka://call_events"],
        "iceberg.bronze.crm_customers": ["crm_batch_ingestor"],
        "iceberg.bronze.network_probe_events": ["kafka://network_probe_events"],
    }

    def get_upstream(self, table_name: str, depth: int = 3) -> dict:
        """Return the full upstream lineage tree up to `depth` hops."""
        if depth == 0 or table_name not in self.LINEAGE:
            return {"table": table_name, "upstream": []}

        return {
            "table": table_name,
            "upstream": [
                self.get_upstream(parent, depth - 1)
                for parent in self.LINEAGE[table_name]
            ],
        }

    def impact_analysis(self, changed_table: str) -> List[str]:
        """What downstream tables are impacted if `changed_table` changes?"""
        impacted = []
        for table, parents in self.LINEAGE.items():
            if changed_table in parents:
                impacted.append(table)
                impacted.extend(self.impact_analysis(table))
        return list(set(impacted))


# ── Observability Report ──────────────────────────────────────────────────────

class ObservabilityReport:
    """Aggregates all checks and generates a summary report."""

    def __init__(self):
        self.checks: List[ObservabilityCheck] = []

    def add(self, check_or_checks):
        if isinstance(check_or_checks, list):
            self.checks.extend(check_or_checks)
        else:
            self.checks.append(check_or_checks)

    def summary(self) -> dict:
        total = len(self.checks)
        failed = [c for c in self.checks if c.status == "FAILED"]
        critical = [c for c in failed if c.severity == Severity.CRITICAL]

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_checks": total,
            "passed": total - len(failed),
            "failed": len(failed),
            "critical_failures": len(critical),
            "pipeline_status": "BLOCKED" if critical else ("DEGRADED" if failed else "HEALTHY"),
            "checks": [c.to_dict() for c in self.checks],
        }

    def print_report(self):
        summary = self.summary()
        print(f"\n{'='*60}")
        print(f"Pipeline Observability Report — {summary['generated_at']}")
        print(f"{'='*60}")
        print(f"Status: {summary['pipeline_status']}")
        print(f"Checks: {summary['passed']}/{summary['total_checks']} passed")
        if summary["failed"] > 0:
            print(f"\nFailed checks:")
            for c in [ch for ch in self.checks if ch.status == "FAILED"]:
                print(f"  [{c.severity.value}] {c.table_name}.{c.check_name}: {c.details}")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    # Demo: run all monitors with synthetic data
    report = ObservabilityReport()

    freshness = FreshnessMonitor()
    report.add(freshness.check(
        "silver_calls",
        last_updated=datetime.now(timezone.utc) - timedelta(hours=2)
    ))
    report.add(freshness.check(
        "silver_network_probes",
        last_updated=datetime.now(timezone.utc) - timedelta(minutes=28)
    ))

    volume = VolumeMonitor()
    report.add(volume.check(
        "fact_calls",
        today_count=3_500_000,
        historical_counts=[3_200_000, 3_400_000, 3_300_000, 3_600_000,
                           3_100_000, 3_450_000, 3_250_000, 3_380_000],
    ))
    # Simulate anomaly — today's count dropped 60%
    report.add(volume.check(
        "silver_network_probes",
        today_count=120_000,
        historical_counts=[850_000, 900_000, 870_000, 920_000, 880_000],
    ))

    dist = DistributionMonitor()
    report.add(dist.check_null_rate("silver_calls", "tower_id", 0.002, 0.001))

    schema = SchemaMonitor()
    report.add(schema.check(
        "silver_calls",
        current_schema={"call_id": "VARCHAR", "call_status": "VARCHAR", "revenue_usd": "DECIMAL"},
        baseline_schema={"call_id": "VARCHAR", "call_status": "VARCHAR", "revenue_usd": "DECIMAL"},
    ))

    lineage = LineageTracker()
    upstream = lineage.get_upstream("agg_tower_hourly_utilization")
    print("Lineage for agg_tower_hourly_utilization:")
    print(json.dumps(upstream, indent=2))
    impact = lineage.impact_analysis("silver_calls")
    print(f"\nImpact of silver_calls change: {impact}")

    report.print_report()
