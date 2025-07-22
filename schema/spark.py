from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').getOrCreate()

df = spark.read.csv('uber_data.csv', header = True)
