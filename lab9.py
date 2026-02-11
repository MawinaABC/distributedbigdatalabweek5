import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("lab9") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", 23),
    (2, "Bob", 17),
    (3, "Charlie", 35)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

filtered_df = df.filter(col("age") > 18)

print("Filter applied... but no action yet.")

print("Triggering action:")
filtered_df.show()