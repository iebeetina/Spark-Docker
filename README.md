# CEBD 1261 Final Project

Prepared by:<br>Ieva Betina<br>William Acosta

## Table of Contents
* [Create the cluster using Docker Swarm](#createcluster)
* [Read and save the data (HDFS, Parquet files, MongoDB)](#read_save)
* [Load the data with Jupyter notebook from MongoDB](#jupyter)
* [Security precautions with environment variables](#env)
* [Analyze the data with Spark Dataframes](#analyze)
* [Spark with Google Cloud Platform](#gcp)
* [Future improvements](#improvements)
* [Conclusion](#conclusion)

## Summary
This project will demonstrate running a Spark Cluster with Docker Swarm on a single node and also on Google Cloud Platform. As part of the demonstration we will read a [London energy csv](data/power_1.csv) with Spark and write it to HDFS using Parquet files and then also to MongoDB. From there, we will use Jupyter notebook as a front end to analyze the data. On GCP, we will demonstrate reading the csv from a bucket and saving back to the bucket in the Parquet format. Jupyter notebook will again be used to analyze the data.

<a id='createcluster'></a>
## Create a Spark Cluster

#### NOTE 
 * make sure you have enough Docker resources. Increase the memory and CPUs.
 * for this project we'll be running the swarm on just one node. We could have also run these services with Docker compose since there is only one host.

Get a swarm up and running:

`docker swarm init`

For multi-node clusters, you might have to use a specific hostname:

`docker swarm init --advertise-addr $(hostname -i)`

Create a [shared overlay network](https://docs.docker.com/network/overlay/) named "spark-network":

`docker network create -d overlay --attachable spark-network`

This network allows the containers to communicate and can be added to any new containers outside of the swarm to communicate with the containers in the swarm.


### Create a volume for files
We use a named volume "london-power" for storing files. We will be referencing this volume in our stack.yml so that the services can connect to it.

`docker volume create london-power`

**ALTERNATIVELY:**

Don't create the volume since the yml file can create it for you at launch.

### Create a custom docker image
We modified the mjhea0/spark:2.4.1 image to already include our python scripts. We created a simple [Dockerfile](Dockerfile) with the following:
```
FROM mjhea0/spark:2.4.1
COPY script/ /usr/spark-2.4.1/script
```

Then we build the image. We added the file to docker hub so we included a user name and tags:

`docker build -t wacosta/spark-custom:2.4.1 .`

Push to docker hub:

`docker push wacosta/spark-custom:2.4.1`

The script files will now be located in the workers when the stack launches.


**Alternatives**

Getting a file directly from a website either with `ADD` or `RUN wget` then unzipping them with ` RUN unzip `would be a great way to automate updating of the files by using this custom Docker image when deploying our stack. Just exploring that it is possible for educational purposes. However, the documentation recommends against it and large files should be put directly in a volume :
```
FROM mjhea0/spark:2.4.1

RUN apt-get update

RUN apt-get install -y wget

RUN mkdir /mongo

USER root

RUN wget "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/3527bf39-d93e-4071-8451-df2ade1ea4f2/Power-Networks-LCL-June2015.zip" -P  /usr/spark-2.4.1/volume/

RUN unzip /usr/spark-2.4.1/volume/Power-Networks-LCL-June2015.zip
```

We can see that the file has unzipped and is inside the spark worker:

<img src="images/photo10.png" width="800">

Another interesting option is to add files to the image from the cloud using AWS s3 bucket or gcp bucket. It would be worth exploring.

### Prepare the stack yml file
The [stack-spark.yml](stack-spark.yml) file must include all the services necessary for this project. This includes:
* spark master
* spark worker (using custom image)
* hadoop
* jupyter notebook
* mongoDB

It is also necessary to stipulate the named volume "london-power" that some of the containers will be using as well as the overlay network "spark-network".


``` yaml
    volumes:
      - "london-power:/usr/spark-2.4.1/volume"
    # AND add this at the end (external necessary for existing volumes)
volumes:
  london-power:
    external: true
    
networks:
  default:
    external:
      name: spark-network
```

### Set your environment variables before deploying

The mongo and jupyter services make use of environment variables in the yml file. These will be discussed in the [environment variables](#env) section below.

```
export ME_CONFIG_MONGODB_ADMINUSERNAME=root
export ME_CONFIG_MONGODB_ADMINPASSWORD=example
export MONGO_INITDB_ROOT_USERNAME=root
export MONGO_INITDB_ROOT_PASSWORD=example
export JUPYTER_TOKEN='project'
```

### Deploy the stack

`docker stack deploy --compose-file=stack-spark.yml spark`

If necessary scale the workers up or down. The default we set is 3:

`docker service scale spark_worker=5`

### Populate the volume
Use the busybox hack to copy files to a volume

`docker run --rm -v "$(pwd)"/data:/data -v london-power:/volume busybox cp -r data/ /volume`

If you don't write scripts directly into the image, add them this way:

`docker run --rm -v "$(pwd)"/script:/script -v london-power:/volume busybox cp -r script/ /volume`

### Look inside the volume
Use the busybox hack to peak inside and see the files

`docker run -it --rm -v london-power:/vol busybox sh`

**NOTE:**

If using the busybox hack, the volume must be populated *after* the stack is deployed. Otherwise, permission in the jupyter notebook will be denied when trying to start a new notebook.

<a id='read_save'></a>
## Read and save the data (HDFS, Parquet files, MongoDB)
### Run the .py file with spark-submit to save csv to HDFS and MongoDB
The [csv-hdfs-mongo.py](script/csv-hdfs-mongo.py) reads the csv as a dataframe in Spark. Then it gives it a schema and the proper headers which are specified by us. It saves the dataframe to HDFS as a parquet file. The parquet file is then loaded as a dataframe and saved to MongoDB. This is all done using spark submit. The mongo input and output uri are specified with environment variables for security.

We wrote the files to HDFS *and* MongoDB for demonstration purposes. We would use one or the other for regular use. We've also included other reference scripts for the following:
- csv to MongoDB
- csv to hdfs
- hdfs to MongoDB

Note that the spark submit needs the MongoDB connector passed as a package as well as the input/output uri passed with a --conf flag.

```Shell
docker run -t --rm \
  -v london-power:/usr/spark-2.4.1/volume \
  --network=spark-network \
  wacosta/spark-custom:2.4.1 \
  bin/spark-submit \
    --master spark://master:7077 \
    --class endpoint \
    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 \
    --conf "spark.mongodb.input.uri=mongodb://"$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD"@mongo/power.data?authSource=admin" \
    --conf "spark.mongodb.output.uri=mongodb://"$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD"@mongo/power.data?authSource=admin" \
    /usr/spark-2.4.1/script/csv-hdfs-mongo.py
```
View the results in HDFS at http://localhost:50070

View the results in MongoDB at http://localhost:8181/

![hdfs](images/hdfs_big.png)

![mongodb](images/mongo_big.png)


### Remove files from HDFS
If desired, you can remove files from HDFS:

`docker exec -it $(docker ps --filter name=master --format "{{.ID}}") hdfs dfs -rm -r hdfs://hadoop:8020/data/power/`

For processing the full 11GB file we used 5 workers using 1 core each and 2gb of memory each. The results were much faster after scaling up for the simple process of reading, setting a schema and writing the file. Notice that before scaling up, the process took 40 minutes on 2 cores and 1gb memory each.

![before](images/slow_spark.png)

After scaling up the process was much faster: 13 min using 5 cores and 2GB of memory per worker

![after](images/fast_spark.png)

<a id='jupyter'></a>
## Load the data with Jupyter notebook from MongoDB

Use jupyter notebook to load the data from MongoDB and then run some queries.

Go to Jupyter at http://localhost:8888

Type in the password that was used as the environment variable, in this case "project".

**Alternatively**:

Grab the token by looking at the logs:

`docker service logs spark_jupyter`

Use the [startup_default_mongo.ipynb](notebooks/startup_default_mongo.ipynb) notebook to get started. Notice that in a notebook you must specify the input and output uri and the spark connector.

```
.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.4.0' ) \
.config("spark.mongodb.input.uri", "mongodb://" + login + "@mongo/power.data?authSource=admin") \
.config("spark.mongodb.output.uri", "mongodb://" + login + "@mongo/power.data?authSource=admin")
```

![jupyter](images/jupyter.png)

### Save results from Jupyter to MongoDB

### Save to default output uri database and collection
```
df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
```

### Save to a different db and coll
You must specify the output uri in the config before doing this. For a db:results and coll:data

```
.config("spark.mongodb.output.uri", "mongodb://" + login + "@mongo/results.data?authSource=admin")
```

Then write to that destination:

```
df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database",
    "results").option("collection", "data").save()
```

![results](images/results.png)

<a id='env'></a>
## Security precautions with environment variables
Set environment variables in your local shell before deploying the stack.

```
export ME_CONFIG_MONGODB_ADMINUSERNAME=root
export ME_CONFIG_MONGODB_ADMINPASSWORD=example
export MONGO_INITDB_ROOT_USERNAME=root
export MONGO_INITDB_ROOT_PASSWORD=example
export JUPYTER_TOKEN='project'
```

Include these in the stack yml file. The values will be added to each service where they are specified

```yaml
jupyter:
    image: jupyter/pyspark-notebook:f646d2b2a3af
    volumes:
      - london-power:/home/jovyan/work
    ports:
      - 8888:8888
    environment:
      - MONGO_INITDB_ROOT_USERNAME
      - MONGO_INITDB_ROOT_PASSWORD
      - JUPYTER_TOKEN

  mongo:
    image: mongo
    restart: always
    environment:
      - MONGO_INITDB_ROOT_USERNAME
      - MONGO_INITDB_ROOT_PASSWORD

  mongo-express:
    image: mongo-express
    restart: always
    ports:
      - 8181:8081
    environment:
      - ME_CONFIG_MONGODB_ADMINUSERNAME
      - ME_CONFIG_MONGODB_ADMINPASSWORD

```

We used them to run the spark-submit in order to specify the uri. This allowed us to avoid using credentials in the py file itself

```
--conf "spark.mongodb.input.uri=mongodb://"$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD"@mongo/power.data?authSource=admin" \
--conf "spark.mongodb.output.uri=mongodb://"$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD"@mongo/power.data?authSource=admin" \
    /usr/spark-2.4.1/script/csv-hdfs-mongo.py
```

We then also use them in the jupyter notebook to avoid obviously exposing the credentials. They can still be accessed but they're not written directly into the notebook at least.

```python
user = %env MONGO_INITDB_ROOT_USERNAME
password = %env MONGO_INITDB_ROOT_PASSWORD
login = user + ":" + password
```

```python
    .config("spark.mongodb.input.uri", "mongodb://" + login + "@mongo/power.data?authSource=admin") \
    .config("spark.mongodb.output.uri", "mongodb://" + login + "@mongo/power.data?authSource=admin") \
```

An alternative for the jupyter file would have been to set the uri in the spark stack so it wouldn't have to be referenced in a cell. This is like the spark-submit approach. Unfortunately, we weren't sure how to set the uri at runtime. We also would like to further investigate Docker Secrets.

The os.environ.get is also useful within python files. The magic %env that we used is better for notebooks.

```python
import os
user = os.environ.get('MONGO_USER')
password = os.environ.get('MONGO_PASS')
login = user + ":" + password
```

<a id='analyze'></a>
## Analyze the data with Spark Dataframes
We used Spark to connect to our MongoDB with all the data that we uploaded. For the full 11GB file however, we found it faster to read from HDFS so we used MongoDB to save the results instead. We used both pythonic syntax and SparkSQL syntax to demonstrate.

See [analysis_final.ipynb](notebooks/analysis_final.ipynb) for the specifics.

In summary, we read the file in from HDFS as a dataframe:

`df = spark.read.parquet('hdfs://hadoop:8020/data/power')`

We confirmed there were 167,926,914 records. We then set out to find the monthly consumption patterns of each Acorn group and plot them. On average, the energy consumption was highest in "affluent" households, followed by "comfortable" and lowest in "adversity" households.

![avg consumption](images/avg_consumption.png)

Furthermore, we wanted to see if the test group that received warnings about energy prices beforehand consumed less energy than those who did not. The results showed that they did.

![std_tou consumption](images/kwh_std_tou.png)

Finally, we saved the results to a new MongoDB collection:

 ```python
results.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database", \
 "power").option("collection", "results").save()
 ```

<a id='gcp'></a>
## Spark with Google Cloud Platform
### Running a spark submit on GCP
Create project called cebd1261
 
Create a cluster called `project` with 2 workers 

<img src="images/cluster.png" width="800">

Create and populate the bucket with `/data` and `/script` using `Regional Storage` as `Storage Class`.This class is optimal for our need as it is for storing frequently accessed data in the same region as `Google CloudProc` or `Google Compute Engine` instances that use it such as data analytics and is lower cost per GB than Multi-Regional Storage used in class tutorial. 

We add a label to our bucket for example 'project' so if we theoretically have many buckets they all are labeled for better access.

We aso can `Edit Permissions` to our bucket to add another user (like the other teammate for the project ) so they can see what is inside inside the bucket (it can be read-only permission as well) 

To work with storage through the command line we use `gsutil`

      - `gsutil ls gs://cebd1261/` to see what is inside the bucket
      - `gsutil ls -L` gives a lot information about the settings of the bucket 
      - `gsutil mb gs://cebd1261_project/`  to create a new bucket 
      - `gsutil mv gs://cebd1261/gcp.py gs://cebd1261_project/` to move a file from one bucket to another
      - ...


<img src="images/bucket1.png" width="800">


Create and submit a spark job by choosing `PySpark` as `Job type` and pointing to our bucket for PyScript to run


<img src="images/sparkjob1.png" width="400">

<img src="images/sparkjob2.png" width="800">

When Job output is complete we can see the results 

<img src="images/sparkjob3.png" width="800">


### BigQuery

BigQuery on GCP can process petabytes in minutes. To query data we pin the project associated with data we are using and create a table. Then populate the table from the Google Storage bucket where our data is stored:


<img src="images/creatingBigQuery.png" width="800">


It is possible to load data from Google Cloud bucket as well as from a local machine and other sources. Adding data is incredibly easy. We had some trouble with `nulls`. To solve this in `Advanced settings` in table creating pane click `ignore unrecognized characters` the database was created and nulls ignored.

<img src="images/big_query_data.png" width="800">

Running a query using a simple SQL language.The queries can be scheduled to run as needed. There are two types of pricing for running queries either flat rate or per query. After running each query the pane shows how many Kb the process took and that way it is easy to plan what pricing is better.

<img src="images/query.png" width="800">

### Jupyter notebooks in a Cloud Datalab Session 

`Cloud Datalab` runs on `Compute Engine` and connects to many cloud services running on a vm instance. It has built-in `Jupyter Notebooks` that supports Tensorflow and SkLearn and can analyze terabytes of data hence often used for Machine learning.

First open secure shell and run `gcloud components install datalab` >
then run `datalab create datalabvm` > choose zone (say us-east-b) > console will output the localhost port that `Datalab` is available at > choose `web preview` and change port accordingly if necessary 

We run a simple Machine Learning task predicting Acorn_grouped depending on KWH per half an hour

<img src="images/datalab.png" width="800">

We get a prediction Mean squared error of 0.39

<img src="images/prediction.png" width="800">


### Install and run a Jupyter notebook on a Cloud Dataproc cluster

Create a cluster and install the Jupyter component through the shell 

```
gcloud beta dataproc clusters create jupyter-cluster \
    --optional-components=ANACONDA,JUPYTER \
    --image-version=1.3 \
    --enable-component-gateway \
    --bucket cebd1261 \
    --project cebd1261-242823
    
```
Navigate to the `Cloud Dataproc Clusters` , then select our cluster to open the `Cluster details` form. Click the `Web Interfaces`

<img src="images/clstr.png" width="800">

We can open a PySpark notebook stored in a bucket and work with data also stored inside our bucket by pointing to it during read:

`spark.read.csv('gs://cebd1261/data/power.csv', schema=final_struc, header=True)`

<img src="images/jupyterNotebook.png" width="800">

<a id='improvements'></a>

## Some MLlib 

   In this Diploma we had opportunity to explore basics of SkLearn however we took this opportunity to learn a little bit about MLlib.During GCP part of this project we applied a quick Linear Regression  model in sklearn to our data in DataLab trying to predict KHW/hh used based on household code and the Acorn_gropuded column that measures social status of the household. Ideally we wanted to use also the column where it indicates wheather the people were advised about the peaks in electricity beforehand,time of the day etc however the sample dataset had only one category and hence useless as a feature. When we tried to upload the 11G dataset to Hadoop the spark-submit would interrupt and the WARN would print that not enough resources were allocated(even after changing amountof spark_workers to 3 and memory to 2g in spark-submit .yml file )We also tried DecisionTreeRegressor, RandomForestRegressor, GBTRegressor with poor results.

Mean Squared error with SkLearn was 0.27 and with MLlib 0.20 which surprised us since we expected same results for both.
In the future more models should be trained using more accurate feature engineering. 
Notebooks available in our repo.


## Future Improvements

* Make multi-node cluster
* Fully automated with a scheduler like airflow
* Machine Learning with Spark MLib
* Improve security
* Connect MongoDB to Dataproc

<a id='conclusion'></a>
## Conclusion
Through the project we refined our docker skills and explored various file types and storage options. We got to really see the power of distributed Spark jobs by running our script on many workers with an 11GB file. We also explored new territory on Google Cloud Platform, scratching the surface of running Spark in a cloud environment. We briefly explored the data with Spark dataframes. We look forward to using more advance Spark features like MLib.
