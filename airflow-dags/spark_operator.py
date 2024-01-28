import os
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f


aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_endpoint_url = os.environ.get("AWS_ENDPOINT_URL")

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key)
conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
conf.set("spark.hadoop.fs.s3a.endpoint", aws_endpoint_url)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

sc = SparkContext(conf=conf)


spark = SparkSession(sc).builder.appName("bronze-to-silver").getOrCreate()

df = spark.read.format("csv")\
          .option("header", "true")\
          .option("inferSchema", "true")\
          .csv("s3a://eventsim/eventsim/date_id=2023-12-04.csv")

# convert timestamp to datetime
df = df.withColumn("date_id", f.to_date(f.from_unixtime(f.col("ts")/1000)))

df.write.partitionBy("date_id").mode("overwrite").csv("s3a://eventsim/silver")