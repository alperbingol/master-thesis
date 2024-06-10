import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()
print(spark.version)

