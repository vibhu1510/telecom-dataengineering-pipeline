"""
CDR Pipeline Orchestration DAG
================================
Orchestrates the full nightly CDR (Call Detail Record) pipeline:

  1. Validate data freshness in Kafka (was data actually produced today?)
  2. Compact Bronze Iceberg tables (merge small files from streaming consumer)
  3. CRM batch ingestion (pull customer updates from CRM system)
  4. dbt Silver transforms (clean, type-cast, deduplicate)
  5. dbt Gold models (dimensional modeling, aggregations)
  6. dbt data quality tests (fail-fast before dashboards refresh)
  7. Alert on-call if any step fails (Slack/PagerDuty)

Schedule: Daily at 1:00 AM UTC (after streaming consumer has a full day's data)

Key Airflow patterns demonstrated:
  - TaskGroup for logical grouping
  - SparkSubmitOperator for Spark jobs
  - BashOperator for dbt CLI
  - BranchPythonOperator for conditional logic
  - SLA miss callbacks
  - Cross-task communication via XCom
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ── DAG defaults ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-eng-alerts@telecom.internal"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=4),  # Pipeline must complete by 5 AM
}

SPARK_CONN_ID = "spark_default"
DBT_DIR = "/opt/airflow/dbt_project"
MINIO_ENDPOINT = "http://minio:9000"

# ── Helper functions ──────────────────────────────────────────────────────────

def check_bronze_freshness(**context) -> str:
    """
    Check that Kafka/Bronze data for yesterday's date exists and is non-empty.
    Returns the next task_id (for branching).
    """
    import boto3
    from botocore.client import Config
    from datetime import date, timedelta

    yesterday = date.today() - timedelta(days=1)
    prefix = (
        f"events/call_events/"
        f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
    )

    response = s3.list_objects_v2(Bucket="telecom-bronze", Prefix=prefix)
    file_count = response.get("KeyCount", 0)

    if file_count == 0:
        print(f"[WARN] No Bronze files found for {yesterday} at prefix {prefix}")
        context["ti"].xcom_push(key="bronze_file_count", value=0)
        return "handle_missing_bronze_data"

    print(f"[OK] Found {file_count} Bronze files for {yesterday}")
    context["ti"].xcom_push(key="bronze_file_count", value=file_count)
    return "bronze_compaction"


def send_pipeline_failure_alert(context):
    """Called when any task fails. Would integrate with PagerDuty/Slack in prod."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"]

    message = (
        f"[PIPELINE FAILURE]\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution: {execution_date}\n"
        f"Log: {task_instance.log_url}"
    )
    # In production: slack_webhook.send(message) or pagerduty.create_incident(message)
    print(f"ALERT: {message}")


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="cdr_pipeline",
    description="Nightly CDR pipeline: Bronze compaction → Silver → Gold → DQ tests",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",   # 1:00 AM UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,               # no concurrent runs of this pipeline
    tags=["cdr", "production", "medallion"],
    on_failure_callback=send_pipeline_failure_alert,
    doc_md="""
## CDR Pipeline
End-to-end pipeline for Call Detail Records.
- **Bronze**: Compacts streaming Parquet files into Iceberg tables
- **Silver**: Cleans, types, deduplicates (dbt incremental models)
- **Gold**: Dimensional model, aggregations for dashboards (dbt)
- **Tests**: Data quality gates before dashboard refresh
    """,
) as dag:

    # ── Step 0: Freshness check ───────────────────────────────────────────────
    check_freshness = BranchPythonOperator(
        task_id="check_bronze_freshness",
        python_callable=check_bronze_freshness,
        provide_context=True,
    )

    handle_missing_data = BashOperator(
        task_id="handle_missing_bronze_data",
        bash_command="""
            echo "No Bronze data found for yesterday. Sending alert and aborting."
            # slack-notify "Bronze data missing for {{ ds }} — pipeline aborted"
            exit 1
        """,
    )

    # ── Step 1: Bronze compaction (4 topics run in parallel) ─────────────────
    with TaskGroup(group_id="bronze_compaction") as bronze_compaction:
        for topic in ["call_events", "data_session_events", "sms_events", "network_probe_events"]:
            SparkSubmitOperator(
                task_id=f"compact_{topic}",
                conn_id=SPARK_CONN_ID,
                application="/opt/spark/jobs/bronze/bronze_compaction.py",
                application_args=["--date", "{{ ds }}", "--topic", topic],
                conf={
                    "spark.master": "spark://spark-master:7077",
                    "spark.executor.memory": "2g",
                    "spark.executor.cores": "2",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                },
                packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4",
                retries=1,
            )

    # ── Step 2: CRM batch ingestion ───────────────────────────────────────────
    ingest_crm = BashOperator(
        task_id="ingest_crm_customers",
        bash_command="python /opt/airflow/processing/../ingestion/batch/crm_batch_ingestor.py --batch-size 10000",
        env={"MINIO_ENDPOINT": MINIO_ENDPOINT, "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin"},
    )

    # ── Step 3: dbt Silver transforms ─────────────────────────────────────────
    with TaskGroup(group_id="silver_transforms") as silver_transforms:
        dbt_silver_calls = BashOperator(
            task_id="dbt_silver_calls",
            bash_command=f"cd {DBT_DIR} && dbt run --select silver_calls --profiles-dir .",
        )
        dbt_silver_customers = BashOperator(
            task_id="dbt_silver_customers",
            bash_command=f"cd {DBT_DIR} && dbt run --select silver_customers --profiles-dir .",
        )
        dbt_silver_probes = BashOperator(
            task_id="dbt_silver_network_probes",
            bash_command=f"cd {DBT_DIR} && dbt run --select silver_network_probes --profiles-dir .",
        )

    # ── Step 4: dbt Gold models ───────────────────────────────────────────────
    with TaskGroup(group_id="gold_models") as gold_models:
        dbt_dims = BashOperator(
            task_id="dbt_gold_dimensions",
            bash_command=f"cd {DBT_DIR} && dbt run --select tag:dimension --profiles-dir .",
        )
        dbt_fact_calls = BashOperator(
            task_id="dbt_fact_calls",
            bash_command=f"cd {DBT_DIR} && dbt run --select fact_calls --profiles-dir .",
        )
        dbt_aggregates = BashOperator(
            task_id="dbt_gold_aggregates",
            bash_command=f"cd {DBT_DIR} && dbt run --select tag:aggregate --profiles-dir .",
        )
        dbt_dims >> dbt_fact_calls >> dbt_aggregates

    # ── Step 5: Data quality gate ─────────────────────────────────────────────
    dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"""
            cd {DBT_DIR} &&
            dbt test --select gold --profiles-dir . --store-failures --target prod
        """,
    )

    # ── Step 6: Notify dashboard refresh ─────────────────────────────────────
    notify_success = BashOperator(
        task_id="notify_pipeline_success",
        bash_command="""
            echo "CDR pipeline complete for {{ ds }}."
            echo "Gold tables refreshed. Dashboards ready."
            # superset-cli refresh-dashboards --tag cdr
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    check_freshness >> [bronze_compaction, handle_missing_data]
    [bronze_compaction, ingest_crm] >> silver_transforms
    silver_transforms >> gold_models >> dbt_tests >> notify_success
