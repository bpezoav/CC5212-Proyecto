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

# Obtener el minimo valor de sentiment score por subreddit
min_sentiment = df.groupBy("subreddit_name").agg(min("sentiment").alias("min_sentiment"))

# Obtener el maximo valor de sentiment score por subreddit
max_sentiment = df.groupBy("subreddit_name").agg(max("sentiment").alias("max_sentiment"))

# Obtener el promedio de sentiment score por subreddit
avg_sentiment = df.groupBy("subreddit_name").agg(avg("sentiment").alias("avg_sentiment"))

# Obtener la mediana de sentiment score por subreddit
median_sentiment = df.groupBy("subreddit_name").agg(expr("percentile(sentiment, 0.5)").alias("median_sentiment"))

# Obtener la desviacion estandard de sentiment score por subreddit
std_sentiment = df.groupBy("subreddit_name").agg(stddev("sentiment").alias("std_sentiment"))

# Unimos los dataframes
min_sentiment = min_sentiment.join(max_sentiment, "subreddit_name")
min_sentiment = min_sentiment.join(avg_sentiment, "subreddit_name")
min_sentiment = min_sentiment.join(median_sentiment, "subreddit_name")
min_sentiment = min_sentiment.join(std_sentiment, "subreddit_name")

min_sentiment.write.csv(fileout)

spark.stop()