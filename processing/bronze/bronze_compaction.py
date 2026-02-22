"""
Bronze Layer Compaction Job (PySpark)
=======================================
Runs nightly (via Airflow) to compact many small Parquet files produced
by the streaming consumer into optimally-sized files (~256MB each) and
registers the result as Apache Iceberg tables.

Small files are a major performance problem in data lakes:
  - Each file requires a metadata operation to open
  - Query planners take longer to enumerate thousands of tiny files
  - Object storage charges per-request (AWS S3 API calls)

This job:
  1. Reads all raw Parquet files for a given date from Bronze bucket
  2. Coalesces them into target file sizes
  3. Writes as Iceberg tables (enabling ACID, time travel, schema evolution)
  4. Registers file statistics in the Iceberg catalog

Usage:
  spark-submit bronze_compaction.py --date 2026-02-21 --topic call_events
"""

import argparse
import os
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Spark Session ────────────────────────────────────────────────────────────

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://rest-catalog:8181")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://telecom-bronze/iceberg/")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Target file size for compaction (~256MB in bytes)
        .config("spark.sql.files.maxRecordsPerFile", 0)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


TARGET_FILE_SIZE_BYTES = 256 * 1024 * 1024  # 256MB

TOPIC_SCHEMAS = {
    "call_events": {
        "partition_cols": ["year", "month", "day"],
        "sort_cols": ["call_start_timestamp", "tower_id"],
        "primary_key": "event_id",
    },
    "data_session_events": {
        "partition_cols": ["year", "month", "day"],
        "sort_cols": ["session_start_timestamp", "tower_id"],
        "primary_key": "event_id",
    },
    "sms_events": {
        "partition_cols": ["year", "month", "day"],
        "sort_cols": ["timestamp", "tower_id"],
        "primary_key": "event_id",
    },
    "network_probe_events": {
        "partition_cols": ["year", "month", "day"],
        "sort_cols": ["measurement_timestamp", "tower_id"],
        "primary_key": "probe_id",
    },
}


def compact_topic(spark: SparkSession, topic: str, run_date: date):
    schema_config = TOPIC_SCHEMAS.get(topic)
    if not schema_config:
        raise ValueError(f"Unknown topic: {topic}")

    year, month, day = run_date.year, run_date.month, run_date.day

    # Read all raw Parquet files for this date partition
    raw_path = (
        f"s3a://telecom-bronze/events/{topic}/"
        f"year={year}/month={month:02d}/day={day:02d}/"
    )

    print(f"Reading raw files from: {raw_path}")
    df = spark.read.parquet(raw_path)

    initial_count = df.count()
    print(f"  Records read: {initial_count:,}")

    # Add partition columns if not present
    if "year" not in df.columns:
        df = df.withColumn("year", F.lit(year).cast("int"))
        df = df.withColumn("month", F.lit(month).cast("int"))
        df = df.withColumn("day", F.lit(day).cast("int"))

    # Deduplicate by primary key (idempotent re-runs)
    pk = schema_config["primary_key"]
    df = df.dropDuplicates([pk])
    dedup_count = df.count()
    print(f"  After dedup: {dedup_count:,} (removed {initial_count - dedup_count:,} duplicates)")

    # Sort for better compression and query performance
    sort_cols = schema_config["sort_cols"]
    df = df.sortWithinPartitions(*[c for c in sort_cols if c in df.columns])

    # Estimate optimal number of output files
    # Each executor can output multiple files; we hint with repartition
    avg_row_size_bytes = 500  # estimate for telecom CDR records
    target_rows_per_file = TARGET_FILE_SIZE_BYTES // avg_row_size_bytes
    num_partitions = max(1, dedup_count // target_rows_per_file)
    print(f"  Target partitions: {num_partitions} (~{target_rows_per_file:,} rows/file)")

    df = df.repartition(num_partitions)

    # Write as Iceberg table (creates or appends to existing)
    iceberg_table = f"iceberg.bronze.{topic.replace('-', '_')}"
    partition_cols = schema_config["partition_cols"]

    print(f"  Writing to Iceberg table: {iceberg_table}")
    (
        df.writeTo(iceberg_table)
        .partitionedBy(*[c for c in partition_cols if c in df.columns])
        .tableProperty("write.target-file-size-bytes", str(TARGET_FILE_SIZE_BYTES))
        .tableProperty("write.metadata.compression-codec", "gzip")
        .tableProperty("history.expire.max-snapshot-age-ms", str(7 * 24 * 60 * 60 * 1000))  # 7 days
        .createOrReplace()
    )

    print(f"  Compaction complete for {topic} / {run_date}")

    # Run Iceberg maintenance operations
    spark.sql(f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{iceberg_table}',
            strategy => 'sort',
            sort_order => 'zorder({", ".join(sort_cols[:2])})',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'min-file-size-bytes', '{TARGET_FILE_SIZE_BYTES // 4}'
            )
        )
    """)

    # Expire old snapshots (keep 7 days)
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
    spark.sql(f"""
        CALL iceberg.system.expire_snapshots(
            table => '{iceberg_table}',
            older_than => TIMESTAMP '{datetime.now(timezone.utc) - timedelta(days=7)}'
        )
    """)

    return dedup_count


def main():
    parser = argparse.ArgumentParser(description="Bronze Compaction Job")
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)),
                        help="Date to compact (YYYY-MM-DD)")
    parser.add_argument("--topic", required=True, choices=list(TOPIC_SCHEMAS.keys()),
                        help="Kafka topic / event type to compact")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date)
    print(f"Bronze Compaction Job")
    print(f"  Topic: {args.topic}")
    print(f"  Date : {run_date}")

    spark = create_spark_session(f"bronze_compaction_{args.topic}_{run_date}")

    try:
        record_count = compact_topic(spark, args.topic, run_date)
        print(f"\nJob complete. Final record count: {record_count:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
