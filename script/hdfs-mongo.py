try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession

except Exception as e:
    print(e)


def hdfs_mongo():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("write_to_mongo2") \
                .config("spark.mongodb.input.uri", "mongodb://root:example@mongo/power.data?authSource=admin") \
                .config("spark.mongodb.output.uri", "mongodb://root:example@mongo/power.data?authSource=admin") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    hdfs = "hdfs://hadoop:8020/"

    power = spark.read.parquet(hdfs + "data/power")

    power.count()

    power.show()

    power.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()


    # End the Spark Context
    sc.stop()


if __name__ == "__main__":
    hdfs_mongo()
