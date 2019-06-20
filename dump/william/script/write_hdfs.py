try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (StructField, StringType,
                                   IntegerType, TimestampType,
                                   FloatType, StructType)
    # import pyspark.sql.functions as f

except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def write_hdfs():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("write_parquet_files_hdfs") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # core part of the script
    data_schema = [StructField('LCLid', StringType(), True),
                   StructField('stdorToU', StringType(), True),
                   StructField('DateTime', TimestampType(), True),
                   StructField('KWH_per_hh_per_30min', FloatType(), True),
                   StructField('Acorn', StringType(), True),
                   StructField('Acorn_grouped', StringType(), True)]

    final_struc = StructType(fields=data_schema)

    power = spark.read.csv('volume/data/power_1.csv', schema=final_struc, header=True)

    power.show(50)


    ## https://stackoverflow.com/questions/40069264/how-can-i-save-an-rdd-into-hdfs-and-later-read-it-back
    hdfs = "hdfs://hadoop:8020/"
    ## Writing file in CSV format
    # power.write.format("com.databricks.spark.csv").mode("overwrite").save(hdfs + "user/me/count-df.csv")
    power.write.format("parquet").mode("overwrite").save(hdfs + "data/power")
    # End the Spark Context
    sc.stop()

if __name__ == "__main__":
    write_hdfs()