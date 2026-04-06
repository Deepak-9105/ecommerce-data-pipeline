import os
import sys

# 1. Point to your new Hadoop directory
os.environ["HADOOP_HOME"] = "C:/hadoop"
# 2. Add the bin folder to the system PATH so Spark can find hadoop.dll
os.environ["PATH"] += os.pathsep + "C:/hadoop/bin"

# Existing Python environment setup
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# ── Config ───────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC_NAME     = "orders_topic"
BRONZE_PATH     = "file:///" + os.path.abspath("data/bronze/orders").replace("\\", "/")
CHECKPOINT_PATH = "file:///" + os.path.abspath("data/checkpoints/bronze").replace("\\", "/")

# ── Schema of incoming JSON ──────────────────────────────────────
# This tells Spark exactly what fields to expect inside each Kafka message
ORDER_SCHEMA = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     LongType(),    True),
    StructField("total_amount",   LongType(),    True),
    StructField("city",           StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("customer_name",  StringType(),  True),
    StructField("timestamp",      StringType(),  True),
])


# ── Spark Session ────────────────────────────────────────────────
def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeLayerStreaming")

        # Kafka + Delta packages
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0"
        )
        
        # Required for Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Optional optimization
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions","-Dlog4j.configuration=file:log4j.properties")
        .getOrCreate()
    )

# ── Main Streaming Job ───────────────────────────────────────────
def main():
    print("🚀 Starting Bronze Layer Streaming Job...")
    print(f"   Reading from Kafka topic : {TOPIC_NAME}")
    print(f"   Writing Bronze Delta to  : {BRONZE_PATH}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Step 1: Read raw stream from Kafka
    # Each Kafka message has: key, value, topic, partition, offset, timestamp
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")  # read from beginning
        .load()
    )

    # Step 2: Parse the JSON value from each Kafka message
    # Kafka sends raw bytes — we decode to string then parse JSON
    parsed_stream = (
        raw_stream
        .select(
            col("value").cast("string").alias("raw_json"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select(
            from_json(col("raw_json"), ORDER_SCHEMA).alias("data"),
            col("offset"),
            col("partition"),
            col("kafka_timestamp"),
            current_timestamp().alias("ingested_at")  # when we received it
        )
        .select(
            "data.*",          # expand all order fields
            "offset",
            "partition",
            "kafka_timestamp",
            "ingested_at"
        )
    )

    # Step 3: Write to Delta Lake (Bronze layer)
    # trigger: every 10 seconds, process a new micro-batch
    query = (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )

    print("✅ Bronze streaming job started!")
    print("   Micro-batch runs every 10 seconds.")
    print("   Keep the producer running in another terminal.")
    print("   Press Ctrl+C to stop.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()