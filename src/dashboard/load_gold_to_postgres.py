import io
import os

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv("/opt/.env")

# ── Config ───────────────────────────────────────────────────────
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET      = os.getenv("S3_BUCKET")

GOLD_TABLES = [
    "daily_revenue_by_city",
    "top_products",
    "order_status_summary",
    "payment_method_analysis",
    "city_performance_ranking",
    "amount_category_summary"
]


def read_parquet_from_s3(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        print(f"   ⚠️  No files found at s3://{bucket}/{prefix}")
        return None
    dfs = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".parquet"):
            obj_data = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj_data["Body"].read()))
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else None


def main():
    print("🚀 Loading Gold tables from AWS S3 → PostgreSQL...")
    print(f"   S3 Bucket : {S3_BUCKET}\n")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@localhost:5432/ecommerce_pipeline"
    )

    total_loaded = 0
    for table in GOLD_TABLES:
        print(f"📦 Loading {table}...")
        try:
            df = read_parquet_from_s3(s3, S3_BUCKET, f"gold/{table}")
            if df is None:
                print(f"   ⚠️  No data found for {table}")
                continue
            count = len(df)
            df.to_sql(table, engine, if_exists="replace", index=False)
            print(f"   ✅ {table}: {count:,} rows loaded")
            total_loaded += 1
        except Exception as e:
            print(f"   ❌ {table} failed: {e}")

    print(f"\n✅ Done! {total_loaded}/{len(GOLD_TABLES)} tables loaded")


if __name__ == "__main__":
    main()