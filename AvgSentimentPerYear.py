from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if len(sys.argv) != 3:
    print("Usage: AvgSentimentPerYear.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("AvgSentimentPerYear").getOrCreate() # Create a spark session

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

#agrupar por año 
df_grouped_by_year = df.groupBy(year("date").alias("year"))
#obtener el promedio de sentiment score por año
avg_sentiment = df_grouped_by_year.agg(avg("sentiment").alias("avg_sentiment"))
#ordenamos de manera descendente por año
avg_sentiment = avg_sentiment.orderBy(desc("year"))
#escribimos el archivo de salida
avg_sentiment.write.csv(fileout)

