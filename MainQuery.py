from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime


if len(sys.argv) != 3:
    print("Usage: MainQuery.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

# filein = sys.argv[1]
# fileout = sys.argv[2]

filein = "hdfs://cm:9000/uhadoop2023/proyects/lospergua/the-reddit-climate-change-dataset-comments.csv"
fileout = "---" 
spark = SparkSession.builder.appName("MainQuery").getOrCreate() # Create a spark session

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .option("multiline", "true") \
  .load(filein)

df = df.withColumn("date", from_unixtime(df["created_utc"]))

# Obtenemos los comentarios con sentimiento negativo. Consideramos sentimiento negativo como aquellas filas con df[@"sentiment"] < -0.25
denial_comments = df.filter(df["sentiment"] < -0.25)

# Hacemos un conteo por subrredit
denial_comments_subreddit = denial_comments.groupBy("subreddit_name").agg(count("*").alias("subrredit_count"))

# Hacemos un conteo de comentarios negativos agrupando por año y mes
denial_comments_month = denial_comments.groupBy(year("date").alias("year"), month("date").alias("month")).agg(count("*").alias("month_count"))
# Los ordenamos de forma descendente por conteo
denial_comments_month = denial_comments_month.orderBy(desc("month_count"))
denial_comments_subreddit = denial_comments_subreddit.orderBy(desc("subrredit_count"))

# Hacemos el fileout
denial_comments_subreddit.write.csv("/uhadoop/2023/lospergua/denial_comments_subreddit")
denial_comments_month.write.csv("/uhadoop/2023/lospergua/denial_comments_month")

