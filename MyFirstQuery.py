from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, count
import datetime

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: MyFirstQuery.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1]
    fileout = sys.argv[2]
    
    # in pyspark shell start with:
    #   filein = "hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-two.tsv"
    #   fileout = "hdfs://cm:9000/uhadoop2023/lospergua/series-avg-two-py/"
    # and continue line-by-line from here

    spark = SparkSession.builder.appName("MyFirstQuery").getOrCreate() # Create a spark session

    df = spark.read.csv(filein, header=True)

    df = df.withColumn("date", to_date(df["created_utc"]))

    count_by_date = df.groupBy("date").agg(count("*").alias("count"))

    # Mostrar los resultados
    count_by_date.show()