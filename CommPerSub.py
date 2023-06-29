from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 3:
    print("Usage: CommPerSub.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("CommPerSub").getOrCreate() # Create a spark session

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
df = df.withColumn("neutral_count", when((df["sentiment"] > -0.25) & (df["sentiment"] < 0.25), 1).otherwise(0))
df = df.withColumn("denial_count", when(df["sentiment"] < -0.25, 1).otherwise(0))
df = df.withColumn("positive_count", when(df["sentiment"] > 0.25, 1).otherwise(0))

# Hacemos un conteo por subrredit
df = df.groupBy("subreddit_name").agg(sum("neutral_count").alias("neutral_count"), sum("denial_count").alias("denial_count"), sum("positive_count").alias("positive_count"))

# Escribimos los dataframes como csv
df.write.csv(fileout)