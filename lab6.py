import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder \
    .appName("lab6") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True
)

print("=== Data ===")
df.show()

print("=== Schema ===")
df.printSchema()

print("=== Null values per column ===")
null_counts = df.select([
    _sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])
null_counts.show()

spark.stop()