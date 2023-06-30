from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime

if len(sys.argv) != 3:
    print("Usage: DenialsPerDate.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("DenialsPerDate").getOrCreate() # Create a spark session

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .option("multiline", "true") \
  .load(filein)

# Lectura
df = df.withColumn("date", from_unixtime(df["created_utc"]))

# Obtenemos los comentarios con sentimiento negativo. Consideramos sentimiento negativo como aquellas filas con df[@"sentiment"] < -0.25
denial_comments = df.filter(df["sentiment"] < -0.25)

# Hacemos un conteo de comentarios negativos agrupando por año y mes
denial_comments_month = denial_comments.groupBy(year("date").alias("year"), month("date").alias("month")).agg(count("*").alias("month_count"))

# Los ordenamos de forma descendente por conteo
denial_comments_month = denial_comments_month.orderBy(desc("month_count"))

# Escribimos los dataframes como csv
denial_comments_month.write.csv(fileout)

spark.stop()