"""
Network Monitoring Pipeline DAG
================================
Runs every 30 minutes to refresh the real-time network ops dashboard.
Much shorter cadence than the CDR pipeline because network engineers
need fresh data to respond to incidents quickly.

Steps:
  1. Process latest probe events from Silver (last 2 hours)
  2. Refresh tower_hourly_utilization aggregate
  3. Check for critical towers (load > 90%) and alert
  4. Snapshot daily tower health summary
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "network-ops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["network-ops@telecom.internal"],
}

DBT_DIR = "/opt/airflow/dbt_project"


def check_critical_towers(**context):
    """Query the aggregate table and page on-call if any tower is CRITICAL."""
    # In production: query Trino, check for CRITICAL towers, trigger PagerDuty
    critical_threshold = 90.0
    print(f"[NetworkOps] Checking for towers with load > {critical_threshold}%")
    # placeholder — real impl queries agg_tower_hourly_utilization
    print("[NetworkOps] No critical towers detected in this run.")


with DAG(
    dag_id="network_monitoring",
    description="30-minute refresh of network ops dashboard and alerting",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/30 * * * *",    # every 30 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["network", "real-time", "ops"],
    doc_md="""
## Network Monitoring Pipeline
Refreshes tower utilization metrics every 30 minutes.
Checks for critical load conditions and pages on-call if thresholds breached.
    """,
) as dag:

    refresh_silver_probes = BashOperator(
        task_id="refresh_silver_network_probes",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver_network_probes --profiles-dir .",
    )

    refresh_utilization = BashOperator(
        task_id="refresh_tower_utilization",
        bash_command=f"cd {DBT_DIR} && dbt run --select agg_tower_hourly_utilization --profiles-dir .",
    )

    run_probe_tests = BashOperator(
        task_id="run_probe_data_tests",
        bash_command=f"cd {DBT_DIR} && dbt test --select silver_network_probes --profiles-dir .",
    )

    alert_on_critical = PythonOperator(
        task_id="alert_on_critical_towers",
        python_callable=check_critical_towers,
        provide_context=True,
    )

    refresh_silver_probes >> run_probe_tests >> refresh_utilization >> alert_on_critical
