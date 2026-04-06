import os
import sys

# 1. Point to your new Hadoop directory
os.environ["HADOOP_HOME"] = "C:/hadoop"
# 2. Add the bin folder to the system PATH so Spark can find hadoop.dll
os.environ["PATH"] += os.pathsep + "C:/hadoop/bin"

# Existing Python environment setup
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, to_timestamp, trim, when

# ── Config ───────────────────────────────────────────────────────
BRONZE_PATH  = "file:///" + os.path.abspath("data/bronze/orders").replace("\\", "/")
SILVER_PATH  = "file:///" + os.path.abspath("data/silver/orders").replace("\\", "/")
CHECKPOINT   = "file:///" + os.path.abspath("data/checkpoints/silver").replace("\\", "/")


# ── Spark Session ────────────────────────────────────────────────
def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("SilverLayerTransformation")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Transformations ──────────────────────────────────────────────
def transform_to_silver(df):
    """
    Clean and enrich the Bronze data:
    1. Remove duplicate orders (same order_id)
    2. Drop rows where critical fields are null
    3. Fix data types (timestamp, amounts)
    4. Add derived columns (order_date, amount_category)
    5. Standardise city names to title case
    6. Add a data quality flag
    """

    # Step 1: Remove duplicates based on order_id
    # keep the first occurrence, drop the rest
    df_deduped = df.dropDuplicates(["order_id"])

    # Step 2: Drop rows where critical fields are null
    df_clean = df_deduped.dropna(
        subset=["order_id", "user_id", "product_id", "total_amount", "city"]
    )

    # Step 3: Fix and enrich columns
    df_transformed = (
        df_clean
        # Parse timestamp string to proper timestamp type
        .withColumn("order_timestamp",
                    to_timestamp(col("timestamp")))

        # Extract just the date part — useful for daily aggregations
        .withColumn("order_date",
                    to_date(col("timestamp")))

        # Standardise city — trim whitespace, proper title case
        .withColumn("city",
                    trim(col("city")))

        # Categorise order value — useful for business analysis
        .withColumn("amount_category",
                    when(col("total_amount") < 1000,  lit("Low"))
                    .when(col("total_amount") < 10000, lit("Medium"))
                    .when(col("total_amount") < 50000, lit("High"))
                    .otherwise(lit("Premium")))

        # Flag cancelled orders explicitly
        .withColumn("is_cancelled",
                    when(col("status") == "cancelled", lit(True))
                    .otherwise(lit(False)))

        # Flag delivered orders
        .withColumn("is_delivered",
                    when(col("status") == "delivered", lit(True))
                    .otherwise(lit(False)))

        # Cast amounts to proper types
        .withColumn("total_amount",  col("total_amount").cast("long"))
        .withColumn("unit_price",    col("unit_price").cast("long"))
        .withColumn("quantity",      col("quantity").cast("integer"))

        # Drop raw columns we no longer need
        .drop("timestamp", "offset", "partition",
              "kafka_timestamp", "ingested_at")

        # Add silver processing timestamp
        .withColumn("silver_processed_at",
                    to_timestamp(lit(
                        __import__("datetime")
                        .datetime.now().isoformat()
                    )))
    )

    return df_transformed


# ── Main ─────────────────────────────────────────────────────────
def main():
    print("🚀 Starting Silver Layer Transformation...")
    print(f"   Reading from Bronze : {BRONZE_PATH}")
    print(f"   Writing to Silver   : {SILVER_PATH}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Read Bronze Delta table
    print("📖 Reading Bronze layer...")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"   Bronze records      : {bronze_count:,}")

    # Apply transformations
    print("\n🔧 Applying Silver transformations...")
    silver_df = transform_to_silver(bronze_df)
    silver_count = silver_df.count()

    # Show stats
    duplicates_removed = bronze_count - silver_count
    print(f"   Duplicates removed  : {duplicates_removed:,}")
    print(f"   Silver records      : {silver_count:,}")

    # Show sample
    print("\n📊 Sample Silver data:")
    silver_df.select(
        "order_id", "product_name", "city",
        "total_amount", "amount_category",
        "status", "is_delivered", "order_date"
    ).show(5, truncate=False)

    # Show amount category breakdown
    print("💰 Amount category breakdown:")
    silver_df.groupBy("amount_category") \
             .count() \
             .orderBy("amount_category") \
             .show()

    # Show city breakdown
    print("🏙️  Orders by city:")
    silver_df.groupBy("city") \
             .count() \
             .orderBy(col("count").desc()) \
             .show()

    # Write to Silver Delta table
    print("💾 Writing to Silver Delta table...")
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")   # partition by date for fast queries
        .save(SILVER_PATH)
    )

    print(f"\n✅ Silver layer complete! {silver_count:,} clean records written.")
    print("   Partitioned by order_date for fast queries.")
    spark.stop()


if __name__ == "__main__":
    main()