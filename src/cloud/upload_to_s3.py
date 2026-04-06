import os

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION      = os.getenv("AWS_REGION")
S3_BUCKET       = os.getenv("S3_BUCKET")

def upload_folder_to_s3(local_path, s3_prefix):
    """Upload entire local folder to S3."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    uploaded = 0
    skipped = 0

    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)

            # Build S3 key — relative path from local_path
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

            try:
                s3.upload_file(local_file, S3_BUCKET, s3_key)
                print(f"   ✅ Uploaded: {s3_key}")
                uploaded += 1
            except Exception as e:
                print(f"   ❌ Failed: {file} → {e}")
                skipped += 1

    return uploaded, skipped


def main():
    print("🚀 Uploading local Delta tables to AWS S3...")
    print(f"   Bucket: {S3_BUCKET}\n")

    # Upload Bronze
    print("📦 Uploading Bronze layer...")
    bronze_local = os.path.abspath("data/bronze/orders")
    u, s = upload_folder_to_s3(bronze_local, "bronze/orders")
    print(f"   Bronze: {u} files uploaded, {s} skipped\n")

    # Upload Silver
    print("📦 Uploading Silver layer...")
    silver_local = os.path.abspath("data/silver/orders")
    u, s = upload_folder_to_s3(silver_local, "silver/orders")
    print(f"   Silver: {u} files uploaded, {s} skipped\n")

    # Upload Gold tables
    gold_tables = [
        "daily_revenue_by_city",
        "top_products",
        "order_status_summary",
        "payment_method_analysis",
        "city_performance_ranking",
        "amount_category_summary"
    ]

    print("📦 Uploading Gold tables...")
    for table in gold_tables:
        gold_local = os.path.abspath(f"data/gold/{table}")
        u, s = upload_folder_to_s3(gold_local, f"gold/{table}")
        print(f"   {table}: {u} files uploaded")

    print("\n🎉 All data uploaded to S3!")
    print(f"   Check: https://s3.console.aws.amazon.com/s3/buckets/{S3_BUCKET}")


if __name__ == "__main__":
    main()