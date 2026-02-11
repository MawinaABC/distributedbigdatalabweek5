from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("lab0") \
    .master("local[*]") \
    .getOrCreate()

print(spark)

spark.stop()