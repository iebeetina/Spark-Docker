from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType,
                                   IntegerType, TimestampType,
                                   FloatType, StructType)

spark = SparkSession.builder.appName('Basics').getOrCreate()
sc = spark.sparkContext

# core part of the script
data_schema = [StructField('LCLid', StringType(), True),
                StructField('stdorToU', StringType(), True),
                StructField('DateTime', TimestampType(), True),
                StructField('KWH_per_hh_per_30min', FloatType(), True),
                StructField('Acorn', StringType(), True),
                StructField('Acorn_grouped', StringType(), True)]

final_struc = StructType(fields=data_schema)

power = spark.read.csv('gs://cebd1261/data/power.csv', schema=final_struc, header=True)

power.show(50)

sc.stop()
