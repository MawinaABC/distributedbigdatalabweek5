import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lab7") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True
)

print("=== Original Data ===")
df.show()

clean_df = df.dropna()

print("=== Cleaned Data (nulls removed) ===")
clean_df.show()

spark.stop()