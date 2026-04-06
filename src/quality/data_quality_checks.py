import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ── Config ───────────────────────────────────────────────────────
BRONZE_PATH = "file:///" + os.path.abspath("data/bronze/orders").replace("\\", "/")
SILVER_PATH = "file:///" + os.path.abspath("data/silver/orders").replace("\\", "/")
GOLD_PATH   = "file:///" + os.path.abspath("data/gold").replace("\\", "/")


# ── Spark Session ────────────────────────────────────────────────
def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("DataQualityChecks")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Quality Check Engine ─────────────────────────────────────────
class DataQualityChecker:
    def __init__(self):
        self.passed = []
        self.failed = []
        self.warnings = []

    def check(self, name, condition, value, expected, layer):
        """Run a single quality check and record result."""
        if condition:
            self.passed.append({
                "layer": layer,
                "check": name,
                "value": value,
                "status": "✅ PASSED"
            })
        else:
            self.failed.append({
                "layer": layer,
                "check": name,
                "value": value,
                "expected": expected,
                "status": "❌ FAILED"
            })

    def warn(self, name, message, layer):
        self.warnings.append({
            "layer": layer,
            "check": name,
            "message": message,
            "status": "⚠️  WARNING"
        })

    def print_report(self):
        """Print a formatted quality report."""
        total = len(self.passed) + len(self.failed)
        print("\n" + "="*60)
        print("       DATA QUALITY REPORT")
        print("="*60)

        print(f"\n{'Layer':<10} {'Check':<35} {'Status'}")
        print("-"*60)

        for r in self.passed:
            print(f"{r['layer']:<10} {r['check']:<35} {r['status']}")

        for r in self.failed:
            print(f"{r['layer']:<10} {r['check']:<35} {r['status']}")
            print(f"           Expected: {r['expected']}")
            print(f"           Got     : {r['value']}")

        for r in self.warnings:
            print(f"{r['layer']:<10} {r['check']:<35} {r['status']}")
            print(f"           {r['message']}")

        print("-"*60)
        print("\n📊 Summary:")
        print(f"   Total checks : {total}")
        print(f"   ✅ Passed    : {len(self.passed)}")
        print(f"   ❌ Failed    : {len(self.failed)}")
        print(f"   ⚠️  Warnings  : {len(self.warnings)}")
        print("="*60)

        if self.failed:
            raise Exception(
                f"❌ {len(self.failed)} quality check(s) failed! "
                f"See report above."
            )
        print("\n🏆 All quality checks passed!")
        return True


# ── Bronze Checks ────────────────────────────────────────────────
def check_bronze_quality(spark, checker):
    print("\n🔍 Checking Bronze layer quality...")
    df = spark.read.format("delta").load(BRONZE_PATH)
    total = df.count()

    # Check 1: Bronze has data
    checker.check(
        name="Bronze has records",
        condition=total > 0,
        value=f"{total:,} records",
        expected="> 0 records",
        layer="BRONZE"
    )

    # Check 2: No null order_ids
    null_order_ids = df.filter(col("order_id").isNull()).count()
    checker.check(
        name="order_id has no nulls",
        condition=null_order_ids == 0,
        value=f"{null_order_ids} nulls found",
        expected="0 nulls",
        layer="BRONZE"
    )

    # Check 3: No null user_ids
    null_user_ids = df.filter(col("user_id").isNull()).count()
    checker.check(
        name="user_id has no nulls",
        condition=null_user_ids == 0,
        value=f"{null_user_ids} nulls found",
        expected="0 nulls",
        layer="BRONZE"
    )

    # Check 4: Kafka offset is always positive
    negative_offsets = df.filter(col("offset") < 0).count()
    checker.check(
        name="Kafka offset >= 0",
        condition=negative_offsets == 0,
        value=f"{negative_offsets} negative offsets",
        expected="0 negative offsets",
        layer="BRONZE"
    )

    # Check 5: All 10 cities present
    city_count = df.select("city").distinct().count()
    checker.check(
        name="All 10 cities present",
        condition=city_count >= 10,
        value=f"{city_count} distinct cities",
        expected=">= 10 cities",
        layer="BRONZE"
    )

    print(f"   Bronze records checked: {total:,}")


# ── Silver Checks ────────────────────────────────────────────────
def check_silver_quality(spark, checker):
    print("\n🔍 Checking Silver layer quality...")
    df = spark.read.format("delta").load(SILVER_PATH)
    total = df.count()

    # Check 1: Silver has data
    checker.check(
        name="Silver has records",
        condition=total > 0,
        value=f"{total:,} records",
        expected="> 0 records",
        layer="SILVER"
    )

    # Check 2: No duplicate order_ids
    unique_orders = df.select("order_id").distinct().count()
    checker.check(
        name="order_id is unique",
        condition=unique_orders == total,
        value=f"{total - unique_orders} duplicates found",
        expected="0 duplicates",
        layer="SILVER"
    )

    # Check 3: total_amount always positive
    negative_amounts = df.filter(col("total_amount") <= 0).count()
    checker.check(
        name="total_amount > 0",
        condition=negative_amounts == 0,
        value=f"{negative_amounts} invalid amounts",
        expected="0 negative amounts",
        layer="SILVER"
    )

    # Check 4: quantity always positive
    bad_qty = df.filter(col("quantity") <= 0).count()
    checker.check(
        name="quantity > 0",
        condition=bad_qty == 0,
        value=f"{bad_qty} invalid quantities",
        expected="0 invalid quantities",
        layer="SILVER"
    )

    # Check 5: Valid status values only
    valid_statuses = ["placed", "confirmed", "shipped", "delivered", "cancelled"]
    invalid_status = df.filter(
        ~col("status").isin(valid_statuses)
    ).count()
    checker.check(
        name="status values are valid",
        condition=invalid_status == 0,
        value=f"{invalid_status} invalid statuses",
        expected="0 invalid statuses",
        layer="SILVER"
    )

    # Check 6: Valid payment methods
    valid_payments = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]
    invalid_payment = df.filter(
        ~col("payment_method").isin(valid_payments)
    ).count()
    checker.check(
        name="payment_method is valid",
        condition=invalid_payment == 0,
        value=f"{invalid_payment} invalid methods",
        expected="0 invalid methods",
        layer="SILVER"
    )

    # Check 7: amount_category populated
    null_category = df.filter(col("amount_category").isNull()).count()
    checker.check(
        name="amount_category not null",
        condition=null_category == 0,
        value=f"{null_category} nulls",
        expected="0 nulls",
        layer="SILVER"
    )

    # Check 8: order_date populated
    null_dates = df.filter(col("order_date").isNull()).count()
    checker.check(
        name="order_date not null",
        condition=null_dates == 0,
        value=f"{null_dates} nulls",
        expected="0 nulls",
        layer="SILVER"
    )

    # Warning: cancellation rate
    cancelled = df.filter(col("is_cancelled") == True).count()
    cancel_rate = round((cancelled / total) * 100, 2)
    if cancel_rate > 30:
        checker.warn(
            name="High cancellation rate",
            message=f"Cancellation rate is {cancel_rate}% — investigate!",
            layer="SILVER"
        )

    print(f"   Silver records checked: {total:,}")


# ── Gold Checks ──────────────────────────────────────────────────
def check_gold_quality(spark, checker):
    print("\n🔍 Checking Gold layer quality...")

    gold_tables = [
        "daily_revenue_by_city",
        "top_products",
        "order_status_summary",
        "payment_method_analysis",
        "city_performance_ranking",
        "amount_category_summary"
    ]

    for table in gold_tables:
        path = f"{GOLD_PATH}/{table}"
        try:
            df = spark.read.format("delta").load(path)
            count_val = df.count()
            checker.check(
                name=f"{table} has data",
                condition=count_val > 0,
                value=f"{count_val} rows",
                expected="> 0 rows",
                layer="GOLD"
            )
        except Exception:
            checker.check(
                name=f"{table} exists",
                condition=False,
                value="Table missing or unreadable",
                expected="Table exists with data",
                layer="GOLD"
            )

    # Check revenue is always positive in daily_revenue_by_city
    revenue_df = spark.read.format("delta").load(
        f"{GOLD_PATH}/daily_revenue_by_city"
    )
    negative_revenue = revenue_df.filter(col("total_revenue") <= 0).count()
    checker.check(
        name="Gold revenue always positive",
        condition=negative_revenue == 0,
        value=f"{negative_revenue} negative revenue rows",
        expected="0 negative revenue",
        layer="GOLD"
    )

    # Check top_products has all 10 products
    products_df = spark.read.format("delta").load(
        f"{GOLD_PATH}/top_products"
    )
    product_count = products_df.count()
    checker.check(
        name="All 10 products in Gold",
        condition=product_count == 10,
        value=f"{product_count} products",
        expected="10 products",
        layer="GOLD"
    )

    print(f"   Gold tables checked: {len(gold_tables)}")


# ── Main ─────────────────────────────────────────────────────────
def main():
    print("🚀 Starting Data Quality Checks...")
    print("   Checking Bronze → Silver → Gold layers\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    checker = DataQualityChecker()

    check_bronze_quality(spark, checker)
    check_silver_quality(spark, checker)
    check_gold_quality(spark, checker)

    checker.print_report()
    spark.stop()


if __name__ == "__main__":
    main()