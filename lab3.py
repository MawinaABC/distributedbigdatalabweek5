import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("lab3") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", 23),
    (2, "Bob", 27),
    (3, "Charlie", 22),
    (4, "Diana", 25)
]

columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

print("=== Filter using column expressions ===")
df.filter(col("age") > 24).show()

print("=== Filter using SQL-style string condition ===")
df.filter("age > 24").show()

spark.stop()
