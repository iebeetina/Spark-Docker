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
                .appName("json parquet hdfs") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # core part of the script
    # read the json file into a dataframe
    peopleDF = spark.read.json("volume/script/people.json")
    # write the dataframe as a parquet file
    peopleDF.write.parquet("volume/people.parquet")
    # read the parquet file
    parquetFile = spark.read.parquet("volume/people.parquet")
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    # show results
    teenagers.show()

    ## Writing results as a parquet file
    teenagers.write.parquet("volume/teenagers.parquet")

    # End the Spark Context
    sc.stop()

if __name__ == "__main__":
    get_age()
