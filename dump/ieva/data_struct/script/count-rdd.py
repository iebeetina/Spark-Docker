try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from operator import add
except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def get_counts():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("words count RDD") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # core part of the script
    lines = sc.textFile("README.md")
    words = lines.flatMap(lambda x: x.split(' '))
    pairs = words.map(lambda x: (x,1))
    count = pairs.reduceByKey(lambda x,y: x+y)

    # output results
    lines.saveAsTextFile("hdfs://hadoop:8020/user/me/lines")
    count.saveAsTextFile("hdfs://hadoop:8020/user/me/count-rdd")

    for x in count.collect():
        print(x)

    sc.stop()

if __name__ == "__main__":
    get_counts()
