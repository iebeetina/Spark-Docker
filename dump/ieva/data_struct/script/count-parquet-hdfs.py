try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
#    from operator import add
except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def get_age():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("json parquet") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # core part of the script
    
    peopleDF = spark.read.json("volume/people.json")
    ## See https://stackoverflow.com/questions/48927271/count-number-of-words-in-a-spark-dataframe
    peopleDF.write.parquet("volume/people.parquet")
    parquetFile = spark.read.parquet("volume/people.parquet")
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.show()

    ## https://stackoverflow.com/questions/40069264/how-can-i-save-an-rdd-into-hdfs-and-later-read-it-back
    hdfs = "hdfs://hadoop:8020/"
    
    ## Writing file in CSV format
    teenagers.write.format("com.databricks.spark.csv").mode("overwrite").save(hdfs + "user/me/teenagers.csv")

    # End the Spark Context
    sc.stop()

if __name__ == "__main__":
    get_age()
