# Telecom Data Engineering Platform

A production-grade, end-to-end data engineering pipeline modeled after the architecture
used by large telecom carriers processing billions of events per day. Built to demonstrate
the complete modern data stack from streaming ingestion to executive dashboards.

---

## What This Simulates

A large telecom carrier with ~120 million customers faces these data challenges daily:

- **Billions of CDRs** (Call Detail Records) from cell towers every day
- **Real-time fraud detection** — identify SIM cloning within seconds
- **Network ops dashboards** — which tower is overloaded right now?
- **Customer churn prediction** — who is likely to cancel next month?
- **Revenue reporting** — what was call revenue by region last quarter?

This project builds the complete data infrastructure to answer all of these questions.

---

## Architecture Overview

```
                          SOURCES
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │  Cell Tower  │  │  CRM System  │  │  Billing DB  │
    │  (CDR/Probes)│  │  (Customers) │  │  (Invoices)  │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           │ streaming        │ batch            │ batch
           ▼                  ▼                  ▼
    ┌──────────────────────────────────────────────────┐
    │              Apache Kafka (KRaft)                │
    │   call_events │ data_session_events │ sms_events │
    │   network_probe_events │ fraud_alerts            │
    └──────────────────────┬───────────────────────────┘
                           │
           ┌───────────────┼──────────────────┐
           │               │                  │
           ▼               ▼                  ▼
    ┌─────────────┐  ┌───────────┐   ┌──────────────┐
    │  BRONZE     │  │  Flink    │   │   Fraud      │
    │  (MinIO/S3) │  │  Fraud    │──▶│   Alerts     │
    │  Raw Parquet│  │  Detection│   │   (Kafka)    │
    └──────┬──────┘  └───────────┘   └──────────────┘
           │
           ▼ dbt + Spark
    ┌─────────────┐
    │  SILVER     │
    │  Iceberg    │   Cleansed, typed, deduped, conformed
    │  Tables     │
    └──────┬──────┘
           │
           ▼ dbt
    ┌─────────────┐
    │  GOLD       │
    │  Star Schema│   fact_calls, dim_customers, dim_towers,
    │  + Aggregates│  agg_tower_hourly_utilization, ...
    └──────┬──────┘
           │
           ▼
    ┌──────────────────────────────────────┐
    │           SERVING LAYER              │
    │  Trino (SQL)  │  Superset (BI)       │
    │  Feature Store │ Reverse ETL         │
    └──────────────────────────────────────┘
                    ▲
    ┌───────────────┴──────────────────────┐
    │    Apache Airflow (Orchestration)    │
    │  CDR Pipeline DAG │ Network Ops DAG  │
    └──────────────────────────────────────┘
```

---

## Stack

| Layer | Tool | Why |
|---|---|---|
| Event streaming | Apache Kafka (KRaft, no ZooKeeper) | Industry standard; durable ordered log |
| Object storage | MinIO (S3-compatible) | Local S3 replacement; same API as AWS S3 |
| Table format | Apache Iceberg | ACID transactions, time travel, partition evolution |
| Batch processing | Apache Spark 3.5 | Distributed compute for terabyte-scale transforms |
| Stream processing | Apache Flink 1.19 | Stateful, low-latency event-time processing |
| Transformations | dbt-core + dbt-trino | SQL-based transforms with lineage, docs, tests |
| Query engine | Trino | Fast SQL over Iceberg on MinIO; no data movement |
| Orchestration | Apache Airflow 2.9 | DAG-based scheduling, retries, alerting |
| Data quality | Great Expectations + dbt tests | Multi-layer DQ: schema, freshness, volume, distribution |
| Dashboards | Apache Superset | Open-source BI connected to Trino |
| Infrastructure | Docker Compose | One-command local deployment |

---

## Project Structure

```
.
├── docker-compose.yml              # Full local stack (Kafka, Spark, Flink, Airflow, Trino, Superset)
├── requirements.txt
│
├── streaming/
│   ├── producers/
│   │   ├── cdr_producer.py         # Simulates network equipment emitting CDRs to Kafka
│   │   └── network_probe_producer.py  # Simulates tower KPI metrics
│   └── flink_jobs/
│       └── fraud_detection.py      # Stateful Flink job: impossible travel + velocity fraud
│
├── ingestion/
│   └── batch/
│       └── crm_batch_ingestor.py   # Incremental watermark-based CRM ingestor → Bronze
│
├── processing/
│   ├── bronze/
│   │   ├── kafka_to_bronze.py      # Kafka consumer → Parquet files on MinIO
│   │   └── bronze_compaction.py    # Spark: compact small files → Iceberg tables
│   └── silver/                     # (dbt handles Silver/Gold transforms)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── seeds/
│   │   └── towers_seed.csv         # Tower reference data (static dimension)
│   ├── macros/
│   │   └── generate_surrogate_key.sql
│   ├── models/
│   │   ├── bronze/                 # Views over raw Iceberg tables
│   │   │   └── bronze_call_events.sql
│   │   ├── silver/                 # Incremental: clean, type, deduplicate
│   │   │   ├── silver_calls.sql
│   │   │   ├── silver_customers.sql
│   │   │   ├── silver_network_probes.sql
│   │   │   └── schema.yml          # Column-level tests
│   │   └── gold/
│   │       ├── dimensions/
│   │       │   ├── dim_customers.sql   # SCD Type 2 with PII masking
│   │       │   ├── dim_towers.sql
│   │       │   └── dim_date.sql        # Date spine with fiscal calendar
│   │       ├── facts/
│   │       │   ├── fact_calls.sql      # Central call fact table (star schema)
│   │       │   └── schema.yml
│   │       └── aggregates/
│   │           ├── agg_tower_hourly_utilization.sql
│   │           └── agg_customer_monthly_summary.sql
│   └── tests/
│       ├── assert_no_negative_revenue.sql
│       ├── assert_tower_load_in_range.sql
│       └── assert_row_count_anomaly.sql    # Anomaly detection via SQL
│
├── orchestration/
│   └── dags/
│       ├── cdr_pipeline_dag.py     # Nightly: Bronze → Silver → Gold → DQ tests
│       └── network_monitoring_dag.py  # Every 30 min: probe refresh + alerting
│
├── data_quality/
│   └── great_expectations_suite.py  # GX suites: schema, volume, distribution, freshness
│
├── monitoring/
│   └── pipeline_observability.py   # Custom: freshness, volume, distribution, schema, lineage
│
└── infrastructure/
    ├── docker/
    │   ├── postgres-init.sql
    │   └── trino/etc/              # Trino catalog config (Iceberg + PostgreSQL)
    └── terraform/                  # (Add IaC for cloud deployment)
```

---

## Key Design Decisions (Interview-Ready)

### Why Medallion Architecture?
Raw data lands in Bronze untouched — if a transformation bug is found, you reprocess from Bronze without re-ingesting from the source (which may be transient or expensive to re-query). Silver makes data trustworthy and conformed. Gold encodes business meaning and is shaped for consumption.

### Why Apache Iceberg over plain Parquet?
Plain Parquet on S3 has no ACID guarantees — a failed Spark job leaves partial data with no rollback. Iceberg adds an atomic metadata swap: either the whole write commits or nothing changes. It also enables time travel (`SELECT * FROM cdrs FOR SYSTEM_TIME AS OF '2026-01-01'`), row-level deletes for GDPR compliance, and partition evolution without rewriting data.

### Why dbt for transformations?
dbt lets engineers write `SELECT` statements instead of procedural ETL code. It handles dependency ordering automatically, generates documentation from code, runs data quality tests, and tracks lineage. The `{{ ref() }}` macro creates explicit dependencies that dbt resolves into a DAG — the same artifact that powers lineage visualizations.

### Why Flink over Spark Streaming for fraud detection?
Flink is purpose-built for stateful streaming with exactly-once guarantees. Its `KeyedProcessFunction` with `ValueState` lets us maintain per-customer state (last call location) across millions of events without loading state into application memory each time. Spark Structured Streaming can do this but with higher latency and less expressive state management.

### Why incremental dbt models?
Running a full refresh of Silver CDRs daily would require re-processing the entire history. Incremental models use `WHERE _bronze_ingested_at >= DATE_ADD('day', -3, CURRENT_DATE)` to only process new/recent records. The `-3 day` lookback handles late-arriving events and ensures idempotent reruns (running twice produces the same result).

### Surrogate keys over natural keys in Gold
`dim_customers` uses a surrogate key (MD5 hash of `customer_id`) as the primary key. If a customer's natural key ever changes (rare but happens during system migrations), the surrogate key ensures all historical fact records still join correctly. This is fundamental to Kimball dimensional modeling.

---

## Running Locally

### Prerequisites
- Docker Desktop (8GB RAM recommended)
- Docker Compose v2

### Start the stack

```bash
docker compose up -d

# Check all services are healthy
docker compose ps
```

### Services

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8082 | — |
| Flink UI | http://localhost:8083 | — |
| Trino UI | http://localhost:8084 | — |
| Airflow | http://localhost:8085 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |

### Produce synthetic CDR data

```bash
# Install dependencies
pip install -r requirements.txt

# Start generating CDR events to Kafka (100 events/sec)
python streaming/producers/cdr_producer.py --rate 100

# Start network probe events (1 reading per tower per second)
python streaming/producers/network_probe_producer.py
```

### Run Bronze consumer (Kafka → MinIO)

```bash
python processing/bronze/kafka_to_bronze.py
```

### Run Bronze compaction (Spark)

```bash
python processing/bronze/bronze_compaction.py \
  --date $(date -d "yesterday" +%Y-%m-%d) \
  --topic call_events
```

### Run dbt pipeline

```bash
cd dbt_project

# Seed reference data
dbt seed

# Run all models in dependency order
dbt run

# Run data quality tests
dbt test

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

### Trigger Airflow DAG manually

```bash
# Via Airflow CLI inside the container
docker compose exec airflow-scheduler \
  airflow dags trigger cdr_pipeline --conf '{"date": "2026-02-21"}'
```

### Run fraud detection job (Flink)

```bash
# Submit to local Flink cluster
docker compose exec flink-jobmanager \
  flink run -py /opt/flink/jobs/fraud_detection.py
```

---

## Data Quality Gates

The pipeline will not refresh dashboards if any of these checks fail:

| Check | Type | Threshold |
|---|---|---|
| `fact_calls.call_id` uniqueness | dbt generic | 0 duplicates |
| `fact_calls.revenue_usd >= 0` | dbt singular | 0 violations |
| Tower load in [0, 100]% | dbt singular | 0 violations |
| Daily row count anomaly | dbt singular | Not <50% of 7-day avg |
| Bronze freshness | GX / custom | Data < 3 hours old |
| Schema unchanged | Custom monitor | No removed columns |

---

## Fraud Detection Rules

The Flink streaming job detects two patterns in real time:

**Impossible Travel**
If customer A makes a call from Seattle and then a call from Miami 5 minutes later
(3,300 km apart), the implied speed (~40,000 km/h) is physically impossible —
this indicates SIM cloning or account takeover. Alert fires within seconds.

**Velocity Fraud**
If an account makes >25 calls within any 30-minute window, this is far outside normal
usage and suggests SIM cloning where multiple devices share the account.

Both alerts are written to the `fraud_alerts` Kafka topic and routed to the fraud
analyst dashboard and an automated account freeze workflow.

---

## Data Lineage Example

```
agg_tower_hourly_utilization
  ├── fact_calls
  │   ├── silver_calls
  │   │   └── bronze_call_events (view)
  │   │       └── iceberg.bronze.call_events
  │   │           └── Kafka: call_events
  │   │               └── Cell tower network probes
  │   ├── dim_customers
  │   │   └── silver_customers
  │   │       └── iceberg.bronze.crm_customers
  │   │           └── crm_batch_ingestor.py (nightly)
  │   ├── dim_towers
  │   │   └── towers_seed.csv
  │   └── dim_date (computed)
  └── silver_network_probes
      └── iceberg.bronze.network_probe_events
          └── Kafka: network_probe_events
              └── network_probe_producer.py
```

When a number looks wrong in a dashboard, you trace this graph backward to find the root cause.

---

## Skills Demonstrated

- **Distributed stream processing**: Kafka producers/consumers, Flink stateful streaming, event-time windowing, watermarks
- **Batch ETL**: Incremental watermark pattern, CDC simulation, Parquet/Iceberg writes
- **Dimensional modeling**: Star schema, SCD Type 2, surrogate keys, date spine
- **dbt**: Incremental models, generic/singular tests, macros, seeds, schema YML
- **Spark**: PySpark DataFrames, Iceberg integration, file compaction, partitioning strategy
- **Data quality**: Great Expectations suites, custom observability (freshness, volume, distribution, schema, lineage)
- **Orchestration**: Airflow DAGs with TaskGroups, branching, SLA callbacks, cross-task XCom
- **Infrastructure**: Docker Compose multi-service stack, MinIO (S3), Trino, Superset
- **Security/Governance**: PII masking in Gold layer, column-level access patterns, GDPR-ready row-level deletes via Iceberg
