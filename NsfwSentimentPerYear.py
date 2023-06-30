from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if len(sys.argv) != 3:
    print("Usage: NsfwSentimentPerYear.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("NsfwSentimentPerYear").getOrCreate() # Create a spark session

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .option("multiline", "true") \
  .load(filein)

df = df.withColumn("date", from_unixtime(df["created_utc"]))
#filtramos los valores nulos de sentiment
df = df.filter("sentiment IS NOT NULL")

df = df.withColumn("year", year("date"))

all_sentiment_yearly = df.groupBy("year").agg(avg("sentiment").alias("avg_all_sentiment"))
nsfw_sentiment_yearly = df.groupBy("year", "subreddit_nsfw").agg(avg("sentiment").alias("avg_sentiment"))

pivot_df = nsfw_sentiment_yearly.groupBy("year").pivot("subreddit_nsfw").agg(first("avg_sentiment"))

joined_df = all_sentiment_yearly.join(pivot_df, on="year")

result_df = joined_df.withColumn("difference", col("true") - col("false"))

result_df = result_df.orderBy(desc("year"))

result_df.write.csv(fileout, header=True)
spark.stop()