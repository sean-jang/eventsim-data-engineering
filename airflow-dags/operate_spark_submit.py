from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f


conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).appName("Test").master("local[*]").getOrCreate()

df = spark.read.format("csv")\
          .option("header", "true")\
          .option("inferSchema", "true")\
          .csv("s3a://eventsim/eventsim/date_id=2023-12-04.csv")

# Convert timestamp to datetime
df = df.withColumn("date_id", f.to_date(f.from_unixtime(f.col("ts")/1000)))

# Put partitions on s3
df.write.partitionBy("date_id").mode("overwrite").csv("s3a://eventsim/silver")