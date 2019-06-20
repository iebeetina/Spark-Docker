
try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (StructField, StringType,
                                   IntegerType, TimestampType,
                                   DoubleType, StructType)

except Exception as e:
    print(e)


def write_mongo():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("write_to_mongo") \
                .config("spark.mongodb.input.uri", "mongodb://root:example@mongo/power.data?authSource=admin") \
                .config("spark.mongodb.output.uri", "mongodb://root:example@mongo/power.data?authSource=admin") \
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

    power.count()

    power.show(50)

    power.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    # End the Spark Context
    sc.stop()


if __name__ == "__main__":
    write_mongo()
