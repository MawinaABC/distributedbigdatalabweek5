import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lab8") \
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

df.createOrReplaceTempView("people")

result = spark.sql("""
    SELECT name, age
    FROM people
    WHERE age > 23
""")

print("=== SQL Query Result ===")
result.show()

spark.stop()