try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
#    from operator import add
except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def get_counts():
    spark = SparkSession \
        .builder \
        .appName("mongodb") \
        .master("spark://master:7077") \
        .config("spark.mongodb.input.uri", "mongodb://root:example@mongo/test.coll?authSource=admin") \
        .config("spark.mongodb.output.uri", "mongodb://root:example@mongo/test.coll?authSource=admin") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # core part of the script
    lines = spark.read.text("README.md")
    ## See https://stackoverflow.com/questions/48927271/count-number-of-words-in-a-spark-dataframe
    count = lines.withColumn('word', f.explode(f.split(f.col('value'), ' ')))\
        .groupBy('word')\
        .count()\
        .sort('count', ascending=False)

    # output results
    count.show()
    count.printSchema()

    ## https://docs.mongodb.com/spark-connector/master/python/write-to-mongodb/
    count.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    # End the Spark Context
    spark.stop()

if __name__ == "__main__":
    get_counts()
