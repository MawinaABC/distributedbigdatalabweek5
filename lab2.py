import os
import sys

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("lab2") \
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

total_count = df.count()
print("Total rows: ", total_count)

id_count = df.select("id").count()
name_count = df.select("name").count()
age_count = df.select("age").count()

print("Rows after selecting 'id':", id_count)
print("Rows after selecting 'name':", name_count)
print("Rows after selecting 'age':", age_count)

spark.stop()
