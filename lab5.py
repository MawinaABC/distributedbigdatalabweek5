import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("lab5") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", 23),
    (2, "Bob", 27),
    (3, "Alice", 22),
    (4, "Diana", 25),
    (5, "Bob", 30)
]

columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

print("=== Group by name and count ===")
df.groupBy("name").count().show()

print("=== Global average age ===")
df.select(avg("age").alias("average_age")).show()

spark.stop()