from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime


if len(sys.argv) != 3:
    print("Usage: MyFirstQuery.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

# filein = sys.argv[1]
# fileout = sys.argv[2]

filein = "hdfs://cm:9000/uhadoop2023/proyects/lospergua/the-reddit-climate-change-dataset-comments.csv"
fileout = "---" 
spark = SparkSession.builder.appName("MyFirstQuery").getOrCreate() # Create a spark session

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .option("multiline", "true") \
  .load(filein)

df = spark.read.csv(filein, header=True, inferSchema=True)

df = df.withColumn("date", from_unixtime(df["created_utc"]))

# Obtener el promedio de sentiment score por subreddit
avg_sentiment = df.groupBy("subreddit.name").agg(avg("sentiment").alias("avg_sentiment"))
avg_sentiment.write.csv(fileout)