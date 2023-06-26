from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime


if len(sys.argv) != 3:
    print("Usage: MainRDD.py <filein> <fileout>", file=sys.stderr)
    sys.exit(-1)

filein = sys.argv[1]
fileout = sys.argv[2]

spark = SparkSession.builder.appName("MainRDD").getOrCreate() # Create a spark session

input = spark.read.text(filein).rdd.map(lambda r: r[0])

lines = input.map(lambda line: line.split(","))

# Obtenemos los comentarios con sentimiento negativo. Consideramos sentimiento negativo como aquellas filas con df[@"sentiment"] < -0.25
denial_comments = lines.filter(lambda line: float(line[8]) < -0.25)

# Hacemos un conteo por subrredit
denial_comments_subreddit = denial_comments.map(lambda line: (line[1], 1)).reduceByKey(lambda a, b: a + b)

# Hacemos un conteo de comentarios negativos agrupando por año y mes
denial_comments_month = denial_comments.map(lambda line: (datetime.datetime.fromtimestamp(int(line[5])).year, datetime.datetime.fromtimestamp(int(line[5])).month, 1)).reduceByKey(lambda a, b: a + b)

# Los ordenamos de forma descendente por conteo
denial_comments_month = denial_comments_month.sortBy(lambda line: line[2], ascending=False)
denial_comments_subreddit = denial_comments_subreddit.sortBy(lambda line: line[1], ascending=False)

# Hacemos el fileout
denial_comments_subreddit.saveAsTextFile(fileout)
denial_comments_month.saveAsTextFile(fileout)

# Detenemos el job
spark.stop()

