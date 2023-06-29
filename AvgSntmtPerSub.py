from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if len(sys.argv) != 3:
    print("Usage: AvgSntmtPerSub.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("AvgSntmtPerSub").getOrCreate() # Create a spark session

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .option("multiline", "true") \
  .load(filein)

df = df.withColumn("date", from_unixtime(df["created_utc"]))
df = df.filter("sentiment IS NOT NULL")

# Obtener el promedio de sentiment score por subreddit
avg_sentiment = df.groupBy("subreddit_name").agg(avg("sentiment").alias("avg_sentiment"))

# Ordenamos de manera ascendente
avg_sentiment = avg_sentiment.orderBy(asc("avg_sentiment"))

avg_sentiment.write.csv(fileout)