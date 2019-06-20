try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    # import pyspark.sql.functions as f

except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def write_hdfs2():
    spark = SparkSession.builder \
                .master('spark://master:7077') \
                .appName("write_parquet_files_hdfs2") \
                .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    power = spark.read.csv('volume/data/power_1.csv', header=True)

    power.show()


    ## https://stackoverflow.com/questions/40069264/how-can-i-save-an-rdd-into-hdfs-and-later-read-it-back
    hdfs = "hdfs://hadoop:8020/"
    ## Writing file in CSV format
    # power.write.format("com.databricks.spark.csv").mode("overwrite").save(hdfs + "user/me/count-df.csv")
    power.write.format("parquet").mode("overwrite").save(hdfs + "data/power")
    # End the Spark Context
    sc.stop()

if __name__ == "__main__":
    write_hdfs2()