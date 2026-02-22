"""
CRM Batch Ingestor (Incremental CDC Pattern)
=============================================
Simulates pulling customer/account data from an operational CRM system
into the Bronze layer using the timestamp-watermark incremental pattern.

In production this would connect to Salesforce via API or Fivetran.
Here we generate synthetic data and write Parquet files to MinIO (S3-compatible).

Output: s3://telecom-bronze/crm/customers/year=YYYY/month=MM/day=DD/
"""

import argparse
import hashlib
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BRONZE_BUCKET = "telecom-bronze"
WATERMARK_FILE = "/tmp/crm_watermark.json"

PLANS = ["Basic_30", "Standard_60", "Premium_80", "Unlimited_100", "BusinessPro_150"]
STATES = ["WA", "OR", "CA", "TX", "NY", "FL", "IL", "CO", "AZ", "GA", "MA", "NV", "OH", "NC", "MI"]
CHURN_RISK_LEVELS = ["LOW", "MEDIUM", "HIGH"]
ACQUISITION_CHANNELS = ["Online", "Store", "Referral", "B2B", "Promotion"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def load_watermark() -> datetime:
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            ts = json.load(f)["last_run"]
            return datetime.fromisoformat(ts)
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def save_watermark(ts: datetime):
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_run": ts.isoformat()}, f)


def generate_customer_records(n: int, updated_after: datetime) -> pd.DataFrame:
    """Generate synthetic CRM customer records updated after the watermark."""
    now = datetime.now(timezone.utc)
    records = []

    for i in range(n):
        customer_id = f"CUST-{random.randint(10000000, 99999999)}"
        created = now - timedelta(days=random.randint(1, 1825))
        updated = updated_after + timedelta(seconds=random.randint(0, 86400))
        plan = random.choice(PLANS)
        plan_price = float(plan.split("_")[1])

        records.append({
            "customer_id": customer_id,
            "account_number": f"ACC{random.randint(100000000, 999999999)}",
            "first_name": f"FirstName{i}",
            "last_name": f"LastName{i}",
            "email": f"user{i}@example.com",
            "phone_number": f"+1{random.randint(2000000000, 9999999999)}",
            "billing_address_street": f"{random.randint(100, 9999)} Main St",
            "billing_address_city": random.choice(["Seattle", "Portland", "Los Angeles", "New York", "Chicago"]),
            "billing_address_state": random.choice(STATES),
            "billing_address_zip": f"{random.randint(10000, 99999)}",
            "plan_code": plan,
            "plan_monthly_fee_usd": plan_price,
            "contract_start_date": created.date().isoformat(),
            "contract_end_date": None if random.random() < 0.7 else (created + timedelta(days=365)).date().isoformat(),
            "is_active": random.random() < 0.92,
            "number_of_lines": random.randint(1, 6),
            "device_type": random.choice(["iPhone", "Samsung Galaxy", "Google Pixel", "OnePlus", "Motorola"]),
            "device_imei": f"{random.randint(100000000000000, 999999999999999)}",
            "acquisition_channel": random.choice(ACQUISITION_CHANNELS),
            "lifetime_value_usd": round(plan_price * random.uniform(12, 60), 2),
            "churn_risk": random.choices(CHURN_RISK_LEVELS, weights=[60, 25, 15])[0],
            "nps_score": random.randint(-100, 100),
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
            # Metadata added by ingestion layer
            "_source_system": "crm_salesforce",
            "_ingested_at": now.isoformat(),
            "_record_hash": hashlib.md5(customer_id.encode()).hexdigest(),
        })

    return pd.DataFrame(records)


def write_to_bronze(df: pd.DataFrame, run_date: datetime, s3_client) -> str:
    """Write DataFrame as Parquet to Bronze layer with Hive-style partitioning."""
    table = pa.Table.from_pandas(df)
    date_str = run_date.strftime("%Y/%m/%d")
    s3_key = f"crm/customers/year={run_date.year}/month={run_date.month:02d}/day={run_date.day:02d}/customers_{run_date.strftime('%Y%m%d_%H%M%S')}.parquet"

    # Write to a local buffer first, then upload
    local_path = f"/tmp/customers_{run_date.strftime('%Y%m%d_%H%M%S')}.parquet"
    pq.write_table(table, local_path, compression="snappy")

    with open(local_path, "rb") as f:
        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=f,
            Metadata={
                "source_system": "crm_salesforce",
                "ingestion_type": "incremental",
                "record_count": str(len(df)),
            },
        )

    os.unlink(local_path)
    s3_path = f"s3://{BRONZE_BUCKET}/{s3_key}"
    print(f"  Written {len(df):,} records → {s3_path}")
    return s3_path


def run_ingestion(batch_size: int = 5000, dry_run: bool = False):
    """
    Main ingestion function:
    1. Load watermark (last successful run time)
    2. Pull records updated after watermark
    3. Write to Bronze as Parquet
    4. Advance watermark
    """
    watermark = load_watermark()
    now = datetime.now(timezone.utc)
    print(f"CRM Batch Ingestor starting")
    print(f"  Watermark: {watermark.isoformat()}")
    print(f"  Run time : {now.isoformat()}")
    print(f"  Batch size: {batch_size:,}")

    df = generate_customer_records(n=batch_size, updated_after=watermark)
    print(f"  Generated {len(df):,} customer records")

    if dry_run:
        print(f"[DRY RUN] Would write {len(df):,} records to Bronze")
        print(df.head(3).to_string())
        return

    s3 = get_s3_client()
    s3_path = write_to_bronze(df, now, s3)

    save_watermark(now)
    print(f"  Watermark advanced to {now.isoformat()}")
    print(f"Ingestion complete. Output: {s3_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CRM Batch Ingestor")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_ingestion(batch_size=args.batch_size, dry_run=args.dry_run)
