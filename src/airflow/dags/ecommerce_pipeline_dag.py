from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

# ── Default Arguments ────────────────────────────────────────────
default_args = {
    "owner": "deepak",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────────────
dag = DAG(
    dag_id="ecommerce_data_pipeline",
    description="Daily e-commerce pipeline: Bronze check → Silver → Gold → Quality",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["ecommerce", "delta-lake", "pyspark"],
)


# ── Task Functions ───────────────────────────────────────────────
def check_bronze_health(**context):
    import os
    from datetime import datetime, timedelta
    print("🔍 Checking Bronze layer health...")
    bronze_path = "/opt/airflow/data/bronze/orders"
    if not os.path.exists(bronze_path):
        raise Exception("❌ Bronze table missing! Is the streaming job running?")
    print("✅ Bronze table exists")
    recent_files = []
    cutoff = datetime.now() - timedelta(hours=24)
    for root, dirs, files in os.walk(bronze_path):
        for file in files:
            if file.endswith(".parquet"):
                filepath = os.path.join(root, file)
                modified = datetime.fromtimestamp(os.path.getmtime(filepath))
                if modified > cutoff:
                    recent_files.append(filepath)
    if not recent_files:
        raise Exception("❌ No recent Bronze data! Streaming job may be down.")
    print(f"✅ Found {len(recent_files)} recent Bronze files")
    print("✅ Bronze health check passed — proceeding to Silver")


def run_silver_transformation(**context):
    import subprocess
    import sys
    print("🚀 Starting Silver transformation...")
    result = subprocess.run(
        [sys.executable, "/opt/airflow/src/silver/silver_transform.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Silver job failed:\n{result.stderr}")
    print("✅ Silver transformation complete!")


def run_gold_aggregations(**context):
    import subprocess
    import sys
    print("🚀 Starting Gold aggregations...")
    result = subprocess.run(
        [sys.executable, "/opt/airflow/src/gold/gold_aggregations.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Gold job failed:\n{result.stderr}")
    print("✅ Gold aggregations complete!")

def upload_to_s3(**context):
    """Upload Bronze, Silver, Gold Delta tables to AWS S3."""
    import os

    import boto3
    from dotenv import load_dotenv

    load_dotenv("/opt/airflow/.env")

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")
    S3_BUCKET      = os.getenv("S3_BUCKET")

    print(f"🚀 Starting S3 upload to bucket: {S3_BUCKET}")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    def upload_folder(local_path, s3_prefix):
        uploaded = 0
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative   = os.path.relpath(local_file, local_path)
                s3_key     = f"{s3_prefix}/{relative}".replace("\\", "/")
                s3.upload_file(local_file, S3_BUCKET, s3_key)
                uploaded += 1
        return uploaded

    # Upload all three layers
    layers = {
        "bronze/orders" : "/opt/airflow/data/bronze/orders",
        "silver/orders" : "/opt/airflow/data/silver/orders",
        "gold"          : "/opt/airflow/data/gold",
    }

    total = 0
    for s3_prefix, local_path in layers.items():
        if os.path.exists(local_path):
            count = upload_folder(local_path, s3_prefix)
            print(f"   ✅ {s3_prefix}: {count} files uploaded")
            total += count
        else:
            print(f"   ⚠️  {s3_prefix}: local path not found, skipping")

    print(f"\n✅ S3 upload complete! {total} files uploaded to s3://{S3_BUCKET}/")

def run_data_quality_check(**context):
    import os
    print("🔍 Running data quality checks...")
    checks_passed = []
    checks_failed = []
    silver_path = "/opt/airflow/data/silver/orders"
    if os.path.exists(silver_path):
        checks_passed.append("✅ Silver table exists")
    else:
        checks_failed.append("❌ Silver table missing")
    gold_tables = [
        "daily_revenue_by_city", "top_products",
        "order_status_summary", "payment_method_analysis",
        "city_performance_ranking", "amount_category_summary"
    ]
    for table in gold_tables:
        path = f"/opt/airflow/data/gold/{table}"
        if os.path.exists(path):
            checks_passed.append(f"✅ Gold table: {table}")
        else:
            checks_failed.append(f"❌ Gold table missing: {table}")
    print("\n📋 Quality Check Results:")
    for check in checks_passed:
        print(f"   {check}")
    for check in checks_failed:
        print(f"   {check}")
    if checks_failed:
        raise Exception(f"Quality checks failed: {checks_failed}")
    print(f"\n✅ All {len(checks_passed)} quality checks passed!")


def pipeline_success_notification(**context):
    execution_date = context["execution_date"]
    print(f"""
    ╔══════════════════════════════════════════╗
    ║     PIPELINE COMPLETED SUCCESSFULLY      ║
    ╠══════════════════════════════════════════╣
    ║  DAG     : ecommerce_data_pipeline       ║
    ║  Date    : {execution_date.date()}               ║
    ║  Status  : ✅ All tasks passed           ║
    ╚══════════════════════════════════════════╝
    """)


# ── Tasks ────────────────────────────────────────────────────────
start = EmptyOperator(task_id="pipeline_start", dag=dag)

bronze_health_task = PythonOperator(
    task_id="check_bronze_health",
    python_callable=check_bronze_health,
    dag=dag,
)

silver_task = PythonOperator(
    task_id="run_silver_transformation",
    python_callable=run_silver_transformation,
    dag=dag,
)

gold_task = PythonOperator(
    task_id="run_gold_aggregations",
    python_callable=run_gold_aggregations,
    dag=dag,
)

s3_upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

quality_task = PythonOperator(
    task_id="run_data_quality_checks",
    python_callable=run_data_quality_check,
    dag=dag,
)

success_task = PythonOperator(
    task_id="pipeline_success_notification",
    python_callable=pipeline_success_notification,
    dag=dag,
)

end = EmptyOperator(task_id="pipeline_end", dag=dag)

# ── Dependencies ─────────────────────────────────────────────────
start >> bronze_health_task >> silver_task >> gold_task >> s3_upload_task >> quality_task >> success_task >> end