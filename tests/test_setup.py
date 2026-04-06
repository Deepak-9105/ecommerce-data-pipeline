import os
import sys

# Tell Spark to use the same Python that is running this script
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("SetupTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create a simple test DataFrame
data = [("order_001", "Deepak", 1500.0), ("order_002", "Priya", 2300.0)]
columns = ["order_id", "customer_name", "amount"]

df = spark.createDataFrame(data, columns)
df.show()

# Write as Delta table
df.write.format("delta").mode("overwrite").save("data/bronze/test_orders")
print("\n✅ Delta Lake write successful!")

# Read it back
df_read = spark.read.format("delta").load("data/bronze/test_orders")
df_read.show()
print("✅ Delta Lake read successful!")
print("✅ PySpark + Delta Lake setup is COMPLETE!")

spark.stop()