try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
#    from operator import add
except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def get_counts():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("words count dataframe") \
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

    ## https://stackoverflow.com/questions/40069264/how-can-i-save-an-rdd-into-hdfs-and-later-read-it-back
    hdfs = "hdfs://hadoop:8020/"
    ## Writing file in CSV format
    count.write.format("com.databricks.spark.csv").mode("overwrite").save(hdfs + "user/me/count-df.csv")

    # End the Spark Context
    sc.stop()

if __name__ == "__main__":
    get_counts()
