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
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max,
    min,
    rank,
    round,
    sum,
)
from pyspark.sql.window import Window

# ── Config ───────────────────────────────────────────────────────
SILVER_PATH = "file:///" + os.path.abspath("data/silver/orders").replace("\\", "/")
GOLD_PATH   = "file:///" + os.path.abspath("data/gold").replace("\\", "/")


# ── Spark Session ────────────────────────────────────────────────
def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("GoldLayerAggregations")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_gold(df, path, description):
    """Write a Gold Delta table and print confirmation."""
    full_path = f"{GOLD_PATH}/{path}"
    df.write.format("delta").mode("overwrite").save(full_path)
    print(f"   ✅ {description} → saved to gold/{path}")


# ── Gold Table 1: Daily Revenue by City ─────────────────────────
def daily_revenue_by_city(df):
    print("\n📊 Gold Table 1: Daily Revenue by City")
    result = (
        df.filter(col("is_cancelled") == False)
        .groupBy("order_date", "city")
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_id").alias("unique_customers")
        )
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .orderBy(col("order_date").desc(), col("total_revenue").desc())
    )
    result.show(10, truncate=False)
    write_gold(result, "daily_revenue_by_city", "Daily Revenue by City")
    return result


# ── Gold Table 2: Top Products ───────────────────────────────────
def top_products(df):
    print("\n📊 Gold Table 2: Top Products by Revenue")
    result = (
        df.filter(col("is_cancelled") == False)
        .groupBy("product_id", "product_name")
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("city").alias("cities_reached")
        )
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .orderBy(col("total_revenue").desc())
    )
    result.show(10, truncate=False)
    write_gold(result, "top_products", "Top Products")
    return result


# ── Gold Table 3: Order Status Summary ──────────────────────────
def order_status_summary(df):
    print("\n📊 Gold Table 3: Order Status Summary")
    total = df.count()
    result = (
        df.groupBy("status")
        .agg(
            count("order_id").alias("order_count"),
            sum("total_amount").alias("total_value"),
            avg("total_amount").alias("avg_value")
        )
        .withColumn("avg_value", round(col("avg_value"), 2))
        .withColumn("percentage",
                    round((col("order_count") / total) * 100, 2))
        .orderBy(col("order_count").desc())
    )
    result.show(truncate=False)
    write_gold(result, "order_status_summary", "Order Status Summary")
    return result


# ── Gold Table 4: Payment Method Analysis ───────────────────────
def payment_method_analysis(df):
    print("\n📊 Gold Table 4: Payment Method Analysis")
    result = (
        df.filter(col("is_cancelled") == False)
        .groupBy("payment_method")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_id").alias("unique_users")
        )
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .orderBy(col("total_revenue").desc())
    )
    result.show(truncate=False)
    write_gold(result, "payment_method_analysis", "Payment Method Analysis")
    return result


# ── Gold Table 5: City Performance Ranking ──────────────────────
def city_performance_ranking(df):
    print("\n📊 Gold Table 5: City Performance Ranking")

    # Window function — rank cities by revenue
    window = Window.orderBy(col("total_revenue").desc())

    result = (
        df.filter(col("is_cancelled") == False)
        .groupBy("city")
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_id").alias("unique_customers"),
            sum(col("is_delivered").cast("int")).alias("delivered_orders")
        )
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .withColumn("delivery_rate",
                    round(
                        col("delivered_orders") / col("total_orders") * 100
                    , 2))
        .withColumn("revenue_rank", rank().over(window))
        .orderBy("revenue_rank")
    )
    result.show(truncate=False)
    write_gold(result, "city_performance_ranking", "City Performance Ranking")
    return result


# ── Gold Table 6: Amount Category Summary ───────────────────────
def amount_category_summary(df):
    print("\n📊 Gold Table 6: Amount Category Summary")
    result = (
        df.groupBy("amount_category")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            min("total_amount").alias("min_order_value"),
            max("total_amount").alias("max_order_value")
        )
        .withColumn("avg_order_value", round(col("avg_order_value"), 2))
        .orderBy(col("total_revenue").desc())
    )
    result.show(truncate=False)
    write_gold(result, "amount_category_summary", "Amount Category Summary")
    return result


# ── Main ─────────────────────────────────────────────────────────
def main():
    print("🚀 Starting Gold Layer Aggregations...")
    print(f"   Reading from Silver : {SILVER_PATH}")
    print(f"   Writing Gold to     : {GOLD_PATH}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Read Silver Delta table
    print("📖 Reading Silver layer...")
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    silver_df.cache()  # cache since we run multiple aggregations
    total = silver_df.count()
    print(f"   Silver records loaded: {total:,}")

    # Run all Gold aggregations
    daily_revenue_by_city(silver_df)
    top_products(silver_df)
    order_status_summary(silver_df)
    payment_method_analysis(silver_df)
    city_performance_ranking(silver_df)
    amount_category_summary(silver_df)

    print("\n" + "="*50)
    print("🏆 GOLD LAYER COMPLETE!")
    print("="*50)
    print("   6 Gold tables written:")
    print("   1. daily_revenue_by_city")
    print("   2. top_products")
    print("   3. order_status_summary")
    print("   4. payment_method_analysis")
    print("   5. city_performance_ranking")
    print("   6. amount_category_summary")
    print("\n   These tables power your Metabase dashboard!")

    silver_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()