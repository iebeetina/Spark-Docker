# Running Spark scripts and saving to HDFS, MongoDB, Parquet

#### NOTE 
 * make sure you have enough Docker resources. Increase the memory and CPUs.
 * for this example we'll be running the swarm on just one node

## Running spark on a cluster

get a swarm up and running:

`docker swarm init`

you might have to use a specific hostname:

`docker swarm init --advertise-addr $(hostname -i)`

Create a [shared network](https://docs.docker.com/network/overlay/): `docker network create -d overlay --attachable spark-network`

This network allows the containers to communicate and can be added to any new containers outside of the swarm to communicate with the containers in the swarm.

## Deploy the Spark stack containing the Hadoop service

Take a look at the yml file and deploy the Spark Cluster from the [stack-spark-hdfs.yml](stack-spark-hdfs.yml) template:
```
docker stack deploy --compose-file=stack-spark-hdfs.yml spark
```
List the deployed services: `docker service ls`

see the stacks `docker stack ls`

see the services in the stack and what node they're on `docker stack ps spark`

see the containers deployed on node `docker ps`

Check http://localhost:8080/ to see that there is indeed now 2 Registered Spark Workers

see the nodes from the manager:
`docker node ls`

### Spark RDD script

Open, read, then run the following PySpark script: [script/count-rdd.py](script/count-rdd.py)

Note that this is running the py file using spark-submit from a bind mount in a one-off container that connects through the spark-network overlay

``` Shell
docker run -t --rm \
  -v "$(pwd)"/script:/script \
  --network=spark-network \
  mjhea0/spark:2.4.1 \
  bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    /script/count-rdd.py
```

From the py script you can see that the files are saved in this line
`count.saveAsTextFile("hdfs://hadoop:8020/user/me/count-rdd")`

If you want to look at the results in the terminal, open the master container with

``` Shell
docker exec -it mastercontainerID sh
hdfs dfs -cat hdfs://hadoop:8020/user/me/count-rdd/part-00000*
```

`hdfs dfs -cat` is a command that will cat (ie show) the files listed

https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html#cat

Example: This will cat file1 and file2
	
`hdfs dfs -cat hdfs://nn1.example.com/file1 hdfs://nn2.example.com/file2`


For a more automated way that will print directly without manually specifying the master container id you can use a filter to automatically find the master container.

``` Shell
docker exec -it $(docker ps --filter name=master --format "{{.ID}}") \
    hdfs dfs -cat hdfs://hadoop:8020/user/me/count-rdd/part-00000*
```

To browse the files on HDFS:
http://localhost:50070 -> "Utilities/Browse the file system"

References:
* https://hub.docker.com/r/harisekhon/hadoop/
* https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-common/FileSystemShell.html


### Spark Dataframe script

Do the same but with a dataframe script

``` Shell
docker run -t --rm \
  -v "$(pwd)"/script:/script \
  --network=spark-network \
  mjhea0/spark:2.4.1 \
  bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    /script/count-dataframe.py
```

Notice in the data frame script there is an overwrite mode so you can re-run this script and the result will be overwritten. If I tried to run the RDD file multiple times it would fail because overwrite is not specified.

To copy a file from hdfs to local you can use the `hdfs dfs -get` command while in the master container. The command below will copy all the files in the me/ folder located on hdfs to the local folder (in this case in the master container). You will be open the test/me/ folder to see the files copied to the local.

`docker exec -it containerid \
hdfs dfs -get hdfs://hadoop:8020/user/me/ testfolder`

To remove files from hdfs you can use the -rm command. -rm -r to remove the directory. Example: remove the me/ directory from the hdfs on the master container.

`docker exec -it $(docker ps --filter name=master --format "{{.ID}}") \
hdfs dfs -rm -r hdfs://hadoop:8020/user/me/`

## MONGODB

Deploy the mongodb stack from the [stack-spark-mongodb.yml](stack-spark-mongodb.yml) file. I started the stack fresh by removing the old stack and deploying a new one. You can also probably just use this stack to update the current one, too.

`docker stack deploy -c stack-spark-mongodb.yml spark`

Have a look at & execute the [script/mongodb.py](script/mongodb.py) script with spark submit

``` Shell
docker run -t --rm \
  -v "$(pwd)"/script:/script \
  --network=spark-network \
  mjhea0/spark:2.4.1 \
  bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 \
    /script/mongodb.py
```

- https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector

Then, open http://localhost:8181

The results are in TEST.COLL. You can see all the tuples generated.

## MongoDB in Jupyter Notebook

Create a volume that will be referenced by the Jupyter stack file [stack-jupyter.yml](stack-jupyter.yml)

`docker volume create jupyter-data`

Deploy Jupyter into your existing stack. This is important because you want Mongodb still running from before.

`docker stack deploy -c stack-jupyter.yml spark`

Open http://localhost:8888

Access the password token by reading the Jupiter log

`docker service logs spark_jupyter`

Using the interface, Upload, Examine & Execute [Read_from_MongoDB.ipynb](script/Read_from_MongoDB.ipynb)
 
**NOTE**: change the spark connector config in the first cell from 2.12 to **2.11** otherwise it won’t work!!!

#### What is the notebook script doing?

It is connecting the notebook to our mongoDB. In the first cell the input is specified and it is the familiar TEST.COLL where we can see our output in mongodb express.

The second cell reads the data from TEST.COLL into spark as a data frame.
Finally, the dataframe is displayed using the df.show() command.

## Parquet Files

Parquet is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data.

Use the same `spark-submit` approach from the the examples provided by
https://spark.apache.org/docs/latest/sql-data-sources-parquet.html . Take note that the `people.json` file is located in https://github.com/apache/spark/blob/master/examples/src/main/resources/

I put the [people.json](script/people.json) file in the `script` folder

add the jupyter-data volume from before to the yml file containing the master and workers so the spark workers can access it. I put it on the master, too,  just to be safe. **NOTE** the volume is put into the `/usr/spark-2.4.1/volume` path in the worker container. Our `json` file will be put here.

``` yaml
    volumes:
      - "jupyter-data:/usr/spark-2.4.1/volume"
    AND add this at the end (external necessary for existing volumes)
    volumes:
      jupyter-data:
        external: true
```

**ALTERNATIVELY**

Use the service update to add the volume to the workers

```
docker service update \
    --mount-add \
      type=volume,src=jupyter-data,target=/usr/spark-2.4.1/volume \
    spark_worker
```

Create the [py script](script/count-parquet.py) to read the json and save the results of the sql query as a parquet file. It would start something like this:

``` python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .master('spark://master:7077') \
    .appName("ages parquet") \
    .getOrCreate()
sc = spark.sparkContext
```

I could have also just put the script file directly into the volume with a busybox hack below.

`docker run --rm -v "$(pwd)"/script:/script -v jupyter-data:/volume busybox cp -r /script/ /volume`

Run the script from a one-off container. I'll use a bind mount to put the script into the container in the same volume location as jupyter-data. I specify that the one-off container uses the same jupyter-data volume. It's connected by the spark-network we created. Finally, we use spark submit to run the [count-parquet](script/count-parquet.py) file.

``` Shell
docker run -t --rm \
    -v "$(pwd)"/script:/usr/spark-2.4.1/volume/script \
    -v jupyter-data:/usr/spark-2.4.1/volume \
    --network=spark-network \
    mjhea0/spark:2.4.1 bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    /usr/spark-2.4.1/volume/script/count-parquet.py
```

The parquet files in teenagers.parquet are now available in the volume, as specified in the script. `people.json` was read in using a parquet format and then a transformation and an action were performed. The results were saved as teenagers.parquet.

### json to parquet then save to HDFS

Using a stack with a hadoop service, you can save the files from teenagers to hdfs. Run the [count-parquet-hdfs](script/count-parquet-hdfs.py) file. Make sure that there aren't already files saved under the same target name since there isn't an overwrite specified.

To browse the files on HDFS:
http://localhost:50070 -> "Utilities/Browse the file system"

#### Clean up

Don't forget to remove your running Spark stack:
``` shell
docker stack rm spark
```

### References
- https://docs.mongodb.com/spark-connector/master/python-api/
- https://stackoverflow.com/questions/48145215/unable-to-connect-to-mongo-from-pyspark
- https://docs.mongodb.com/spark-connector/master/python/write-to-mongodb/