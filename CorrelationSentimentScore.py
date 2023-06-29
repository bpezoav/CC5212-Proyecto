from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 3:
    print("Usage: CorrelationSentimentScore.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("CorrelationSentimentScore").getOrCreate() # Create a spark session

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .option("multiline", "true") \
  .load(filein)

df = df.withColumn("date", from_unixtime(df["created_utc"]))

# Coef de correlacion de Pearson entre el sentimiento y el score
pearson_correlation = df.stat.corr("sentiment", "score")

# Escribimos el valor en un archivo txt
with open(fileout, "w") as f:
    f.write(str(pearson_correlation))

spark.stop()