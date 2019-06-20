try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (StructField, StringType,
                                   IntegerType, TimestampType,
                                   DoubleType, StructType)

except Exception as e:
    print(e)


def write_hdfs():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("write_parquet_files_hdfs") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    data_schema = [StructField('LCLid', StringType(), True),
                   StructField('stdorToU', StringType(), True),
                   StructField('DateTime', TimestampType(), True),
                   StructField('KWH_per_hh_per_30min', DoubleType(), True),
                   StructField('Acorn', StringType(), True),
                   StructField('Acorn_grouped', StringType(), True)]

    final_struc = StructType(fields=data_schema)

    power = spark.read.csv('volume/data/power_1.csv', schema=final_struc, header=True)

    power = power.na.drop()

    power.show()

    hdfs = "hdfs://hadoop:8020/"
    power.write.format("parquet").mode("overwrite").save(hdfs + "data/power")
    # End the Spark Context
    sc.stop()


if __name__ == "__main__":
    write_hdfs()
