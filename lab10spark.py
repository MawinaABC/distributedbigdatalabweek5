import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("lab10") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Alice", 23),
    (2, "Bob", 17),
    (3, "Charlie", 35),
    (4, "David", 16),
    (5, "Eva", 29)
]

columns = ["id", "name", "age"]

start = time.time()

df = spark.createDataFrame(data, columns)

result = df.filter(col("age") > 18) \
           .agg(avg("age"))

result.show()

end = time.time()

print("Execution time:", end - start)

spark.stop()