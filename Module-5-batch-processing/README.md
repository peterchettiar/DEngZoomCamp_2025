# Week 5 Notes

### Table of contents

- [Introduction to Batch Processing](#introduction-to-batch-processing)
  - [Batch versus Streaming](#batch-versus-streaming)
  - [Types of batch jobs](#types-of-batch-jobs)
  - [Orchestrating batch jobs](#orchestrating-batch-jobs)
  - [Advantages and disadvantages of batch jobs](#advantages-and-disadvantages-of-batch-jobs)
- [Introduction to Spark](#introduction-to-spark)
  - [How does Spark work?](#how-does-spark-work)
  - [Why and when do we use Spark?](#why-and-when-do-we-use-spark)
- [Installing Spark on Linux](#installing-spark-on-linux)
- [First Look at Spark/PySpark](#first-look-at-sparkpyspark)
  - [Create a Spark Session](#create-a-spark-session)
  - [Reading CSV files](#reading-csv-files)
  - [Partitions](#partitions)
- [Spark DataFrames](#spark-dataframes)
  - [Actions vs transformations](#actions-vs-transformation)
  - [Functions and UDFs](#functions-and-udfs)
  - [Preparing Yellow and Green Taxi Data [OPTIONAL]](#preparing-yellow-and-green-taxi-data-optional)
  - [Spark SQL](#spark-sql)
- [Spark Internals](#spark-internals)
  - [Anatomy of a Spark Cluster](#anatomy-of-a-spark-cluster)
  - [GroupBy in Spark](#groupby-in-spark)
  - [Joins in Spark](#joins-in-spark)

# Introduction to Batch Processing

**Batch processing** is a computational technique in which a collection of data is amassed and then processed in a single operation, often without the need for real-time interaction (i.e. stream processing). This approach is a particularly effective method for handling large volumes of data, where tasks can be executed as a group during off-peak hours to optimmize system resources and throughput. End-of-day bank reconcilliation and payroll processing is an example of batch processing.

Basic principles - Here are some basic principles of the batch processing method:

![image](https://github.com/user-attachments/assets/70f0e8f2-a753-4aed-88eb-a31835fca5e4)

Here is an example flow of batch processing:
1. **Data collection:** Raw data is gathered from various sources such as databases, files, sensors or APIs. This data can be various types including text, numerical, or multimedia.
2. **Data preprocessing:** Raw data often requires cleaning, normalization, and transformation to make it suitable for analysis. This step involves removing duplicates, handling missing values, scaling numerical data, and encoding categorical variables.
3. **Batching data:** Data is divided into batches based on predefined criteria such as time intervals (e.g. daily, weekly), file sizes, or record counts. Each batch contains a subset of the overall data.
4. **Processing:** Each batch of data is processed using a specific algorithm or set of operations. This could involve computations, analyses, transformations, or model predictions depending on the task at hand. For example, in a batch image processing pipeline, this step might involve resizing, filtering, and feature extraction.
5. **Aggregation:** Results from each batch are aggregated or combined to derive meaningful insights or summaries. This could involve calculating statistics, generating reports, or visualizing trends across multiple batches.
6. **Storage or output:** The final results of the batch processing are typically stored in a database, data warehouse, or file system for future reference or further analysis. Alternatively, the results may be presented as reports, dashboards, or visualisations for consumption by stakeholders.
7. **Monitoring and iteration:** Batch processing systems are often monitored for performance, errors, or anomalies.

## Batch versus Streaming

The choice between batch and stream processing reflects a trade-off between timeliness and comprehensiveness. 

- **Batch processing handles data in large, discrete chunks**, known as batches, within scheduled windows. Batch processing is best suited for scenarios where the completeness of data is essential, like end-of-day reporting or inventory management.

  - Example: processing taxi trips each day

![image](https://github.com/user-attachments/assets/739fee80-368f-4a28-b214-d3c321baa0ec)

- **Stream processing tackles data as it arrives in real-time**, with no inherent delays. Stream processing excels when immediate insights are required, as seen in fraud detection systems or live dashboards.

  - Example: processing a taxi trip as soon as it's generated.

![image](https://github.com/user-attachments/assets/b14b34e7-4796-4fbb-b96c-af092721fff6)

> [!NOTE]
> While batch operations provide in-depth analysis of historical data, stream system react to immediate data inputs and events.

## Types of batch jobs

A ***batch job*** is a job (a unit of work) that will process data in batches. Batch jobs may be scheduled in many ways:
- Weekly
- Daily (very common)
- Hourly (very common)
- 3 times per hour
- Every 5 minutes

Batch jobs can be expressed in larger or even smaller granularities, so small that it can be seen as a form of micro-batch processing which is similar to stream processing. Batch jobs may also be carried out using different technologies:
- `Python Scripts`  - which can be run anywhere such as `kubernetes`, `AWS batch`, etc. ([data pipelines from week 1](https://github.com/peterchettiar/DEngZoomCamp_2025/tree/main/Module-1-docker-terraform#creating-a-custom-pipeline-with-docker))
- `SQL` - [dbt models from week 4](https://github.com/peterchettiar/DEngZoomCamp_2025/tree/main/Module-4-analytics-engineering#developing-with-dbt)
- Spark (Covered this week)

## Orchestrating batch jobs

> [!NOTE]
> Typically we use for orchestrating all the batch jobs is `Airflow`.

All of the following are batch jobs and are orchestrated using `airlfow`:

![image](https://github.com/user-attachments/assets/c21186c7-9fb2-4599-8f3a-9620ffa000e6)

## Advantages and disadvantages of batch jobs

| Advantages | Disadvantages |
|------------|---------------|
| **Easy to manage** : workflow tools to help us define all the steps | **Delay** : running jobs in regular intervals makes data unavailable until job is complete |
| **Retry** : able to parameterize the scripts to enable retries | |
| **Scale** : easy to add more cluster or get a bigger machine as needed by the workflow | |

# Introduction to Spark

[Apache Spark](https://spark.apache.org/) is an open-source, distributed computing framework designed for fast data processing. It is widely used for big data analytics, machine learning, and real-time data processing. Spark is known for its speed, scalability, and ease of use, making it a preferred choice for handling large-scale datasets.

As a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs.

> [!NOTE]
> Spark is actually written in Scala making it the native language that communicates with Spark. But there are wrappers available around Spark, one example is `PySpark` which is Spark wrapped in Python. Spark can be used for streaming as well as batch jobs but we will only cover the latter for now.

## How does Spark work?

Apache Spark processeses large-scale data across multiple machines using a cluster-based approach. It works by dividing tasks into smaller chunks and distributing them across multiple nodes for parallel execution. This makes Spark much faster than traditional batch processing frameworks like Hadoop MapReduce.

**Spark Architecture overview - Main components of Spark data processing engine**:
  - `Driver Program` - Controls the Spark application and coordinates the task
  - `Cluster Manager` - Allocates resources for execution (e.g. YARN, kubernetes, Mesos, or Standalone mode).
  - `Executors` - Workers that run tasks on different nodes in cluster.
  - `RDDs` (Resilient Distributed Datasets) - The fundamental data structure in Spark for distributed computing.

**Diagram of Spark Workflow**

![image](https://github.com/user-attachments/assets/d1e67396-9ee7-4317-a3f0-4b67fe3062e1)

> [!IMPORTANT]
> Apache Spark workflow: the Spark driver program works as a master and as an entry point for all the Spark jobs. The master submits jobs to the worker nodes. The cluster manager keeps the track of the nodes and the jobs distributed to them, several cluster managers are Yet Another Resource Negotiator (YARN), Kubernettes, mesos and standalone (in our case). The worker/slave nodes are the actual machines where the tasks are executed and they report back to the cluster manager

## Why and when do we use Spark?

If data is in data warehouse, it would be simpler to run jobs using `SQL` but more often than not, data will be in data lakes such as S3 or Google Cloud Storage and using SQL in those instance would not be feasible. Spark jobs using python make more sense, although these days making running SQL queries on data lakes are possible with the likes of `Hive`, `Presto/Athena` or even Spark as well. Preference would to always express your batch job as SQL.

![image](https://github.com/user-attachments/assets/717b6cc0-3c01-4c70-92cb-5497bb73c1d3)

Now, instead if our batch job included machine learning, the workflow would look like as follows:

![image](https://github.com/user-attachments/assets/37a7b1b2-9d21-4eec-b99d-64b8d868be94)

# Installing Spark on Linux

**Installation steps:**

1. Install java
    * Need specific version of JDK, either 8 or 11
    * Find the version you need on jdk.java.net/archive/ - use 11.02
    * Next create a folder on GCP vm called spark and run command `wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz` to download the installation file
    * Now we have to unpack the file - `tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz`
    * We can proceed to remove the downloaded file - `rm openjdk-11.0.2_linux-x64_bin.tar.gz`
    * Next we need to set an environment variable for java home (important step as spark will look for this variable so name has to be exactly JAVA_HOME as the path to java is used in its backend scripts of spark) - `JAVA_HOME="~/spark/jdk-11.0.2"` , you can verify this by running `echo $JAVA_HOME`
    * Now to set location of java executables to `PATH` so that it is available globally to all processes in the current shell session. We prepend the PATH  with our JAVA_HOME variable - `export PATH="${JAVA_HOME}/bin:${PATH}”`
    * Please be reminded that we you run this via the terminal its only available in the current bash session. If you want to add it permanently you need to run `nano ~/.bashrc` and insert the following commands at the end:
    ```bash
    export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
    export PATH="${JAVA_HOME}/bin:${PATH}”
    ```
    * After which you run `source ~/.bashrc`
    * Run `java —version` to verify installation

2. Install spark
    * So same steps for spark, go to the official download page - https://spark.apache.org/downloads.html 
    * Select as follows:

   ![Pasted Graphic](https://github.com/user-attachments/assets/8141c165-f690-4c05-a9d0-559d8848f8b1)

    * Make sure to click on the link in step 3 and copy the full link of the installer - https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz 	
    * Run command `wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz`
    * Now unpack the file - `tar xzfv spark-3.5.5-bin-hadoop3.tgz`
    * Remove the archive  - `rm  spark-3.5.5-bin-hadoop3.tgz`
    * Once done we can create `SPARK_HOME` environment variable which is the path to the spark archives, followed by setting a PATH variable to the executables while prepending the `SPARK_HOME` variable:
    ```bash
    export SPARK_HOME="${HOME}/spark/spark-3.5.5-bin-hadoop3"
    export PATH="${SPARK_HOME}/bin:${PATH}"
    ```
    * Now we have to test if spark works - run `spark-shell` and this should open a spark shell for us to test and run spark commands (press `CTRL + D` to close the session
    * Test commands are as follows:
        * `val data = 1 to 10000` - creating a range of numbers from 1 to 10,000. `data` is sequential collection (not yet distributed)
        * `val distData = sc.parallelize(data)` - basically converting our sequential collection into a resilient distributed dataset (`disData`) through  the process of partitioning across Spark’s cluster nodes which enables distributed processing.
        * `distData.filter(_<10).collect()` - Filters out elements less that 10 and brings back the filtered data back to the driver as an array.

3. Run Pyspark
    * Before running `pyspark`, we need to set `PYTHONPATH` environment variable to ensure that Python can locate the necessary spark libraries and communicate with Spark’s JVM (`Java Virtual Machine`) backend.
    * Spark includes a Python API (`pyspark`) that allows python users to interact with Spark’s core engines. However, python does not automatically know where Spark’s python modules are, hence we need to set `PYTHONPATH`:
    ```bash
    export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH”
    ```
    * Spark’s native language is Scala (JVM-based), while PySpark runs in python. To bridge this gap, Spark uses `Py4J`, a library that allows Python to communicate with JVM processes. By setting:    
    ```bash
    export PYTHONPATH=“${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH”
    ```
    * Without this, Python wouldn't be able to send commands to Spark’s Java engine, causing errors when running PySpark.
    * After setting the `PYTHONPATH` in the `.bashrc` file, we can either restart the terminal session or `source ~/.bashrc` for the settings to take effect
    * Now we can create a folder in home called `notebooks` and change directory into the folder and run the `jupyter notebook` command
>[!NOTE]
> Make sure that the port is forwarded locally if you’re using a virtual machine
> Also make sure that you are forwarding the correct port, default port for Jupyter is 8888 but sometimes it may already be in use and another port maybe assigned, so it is important to check the port number which is given at the end after running the `jupyter notebook` command. 

Now that we have setup `PySpark`, let’s test it out. Please take a look at [pyspark demo](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-5-batch-processing/code/3.1_test.ipynb) for an introduction to a spark session.

# First Look at Spark/PySpark
> **Reminder**: if you're running Spark and Jupyter Notebook on a remote machine, you will need to redirect ports 8888 for Jupyter Notebook and 4040 for the Spark UI.

## Create a Spark Session

We can use Spark with Python code by means of PySpark. We will be using Jupyter Notebooks for this lesson. We first need to import PySpark to our code:

```python
import pyspark
from pyspark.sql import SparkSession
```

We now need to instantiate a Spark session, an object that we use to interact with Spark which is our main entry point to spark.

```python

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

```

* `SparkSession` : is the class of the object that we instantiate. builder is the builder method.
* `master()`: sets the Spark master URL to connect to. The local string means that Spark will run on a local cluster. [*] means that Spark will run with as many CPU cores as possible.
* `appName()`: defines the name of our application/session. This will show in the Spark UI.
* `getOrCreate()`: will create the session or recover the object if it was previously created.

Once we've instantiated a session, we can access the Spark UI by browsing to `localhost:4040`. The UI will display all current jobs. Since we've just created the instance, there should be no jobs currently running.

## Reading CSV files

Similarlly to Pandas, Spark can read CSV files into dataframes, a tabular data structure. Unlike Pandas, Spark can handle much bigger datasets but it's unable to infer the datatypes of each column.

> **Note:** Spark dataframes use custom data types; we cannot use regular Python types.

For this example we will use the [High Volume For-Hire Vehicle Trip Records for January 2021](https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet) available from the [NYC TLC Trip Record Data webiste](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The file should be about 295MB in size.

Let's read the file and create a dataframe:
```python
df = spark.read \
    .option("header", "true") \
    .parquet(‘fhvhv_tripdata_2021-01.parquet')
```
* `read()` reads the file.
* `option()` contains options for the read method. In this case, we're specifying that the first line of the parquet file contains the column names.
* `parquet()` is for reading parquet files.


>[!IMPORTANT]
> It is worth point out that every time a cell in the spark session is executed, this would be reflected as a job visible on the Spark UI on `localhost:4040`. Do it give it a try, run a cell on your notebook and then refresh the page to see a new job included as a new line item.

You can see the contents of the dataframe with `df.show()` (only a few rows will be shown) or `df.head()`. You can also check the current schema with `df.schema`; you will notice that all values are strings. This is because unlike pandas, Spark does not infer the data types.

>[!NOTE]
> So far we had initialised our SparkSession and loaded the data based on the `.parquet` file we had downloaded from the Trip Record data website. But for the purpose of this exercise, it would be best to demonstrate using a `.csv` file, hence given the lack of `.csv` counterpart for January 2021 parquet file, I had basically queried this data from `bigquery` and downloaded the result as a `.csv` and had uploaded it into current directory as `output.csv`. Given the time constraint this seemed to be the best approach. I had use the following query to recreate the same data from the website:
```sql
SELECT
  *
FROM
  `ny-rides-peter-415106.nyc_tlc_data.fhvhvtaxi_trips`
WHERE
  EXTRACT(year
  FROM
    pickup_datetime) = 2021
  AND EXTRACT(month
  FROM
    pickup_datetime) = 1;

```

The need for this is to be able to create a schema using pandas so that this can be used to declare the datatypes of the fields when Spark is reading the `.parquet` file.

So after reading the `.csv` file using pandas library, we can proceed to convert the pandas dataframe into a spark dataframe and call the schema attribute to access the schema of the pandas dataframe that we saw previously but using Spark. Result should give you a schema that is different from what you saw previously using `.dtypes`.

```python
spark.createDataFrame(df_pandas).schema
```

The result should look something like this:
```txt
StructType([StructField('hvfhs_license_num', StringType(), True), StructField('dispatching_base_num', StringType(), True), StructField('originating_base_num', StringType(), True), StructField('request_datetime', StringType(), True), StructField('on_scene_datetime', StringType(), True), StructField('pickup_datetime', StringType(), True), StructField('dropoff_datetime', StringType(), True), StructField('PULocationID', LongType(), True), StructField('DOLocationID', LongType(), True), StructField('trip_miles', DoubleType(), True), StructField('trip_time', LongType(), True), StructField('base_passenger_fare', DoubleType(), True), StructField('tolls', DoubleType(), True), StructField('bcf', DoubleType(), True), StructField('sales_tax', DoubleType(), True), StructField('congestion_surcharge', DoubleType(), True), StructField('tips', DoubleType(), True), StructField('driver_pay', DoubleType(), True), StructField('shared_request_flag', StringType(), True), StructField('shared_match_flag', StringType(), True), StructField('access_a_ride_flag', StringType(), True), StructField('wav_request_flag', StringType(), True), StructField('wav_match_flag', StringType(), True)])
```

`StructType` comes from `scala` and we need to turn this into python code for declaring schema for our dataframe. Let's take this opportunity to also change the data types to more optimised data types than what was prescribed above.

> Note: Parquet stores INT64 as LongType in Spark, not IntegerType.

Now to amend the schema of our spark data frame that is loading the `.parquet` file.
```python
from pyspark.sql import types

# Lets amend the schema so that its more efficient than what was prescribed

schema = types.StructType(
    [
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True), 
        types.StructField('originating_base_num', types.StringType(), True), 
        types.StructField('request_datetime', types.TimestampType(), True), 
        types.StructField('on_scene_datetime', types.TimestampType(), True), 
        types.StructField('pickup_datetime', types.TimestampType(), True), 
        types.StructField('dropoff_datetime', types.TimestampType(), True), 
        types.StructField('PULocationID', types.LongType(), True), 
        types.StructField('DOLocationID', types.LongType(), True), 
        types.StructField('trip_miles', types.DoubleType(), True), 
        types.StructField('trip_time', types.LongType(), True), 
        types.StructField('base_passenger_fare', types.DoubleType(), True), 
        types.StructField('tolls', types.DoubleType(), True), 
        types.StructField('bcf', types.DoubleType(), True), 
        types.StructField('sales_tax', types.DoubleType(), True), 
        types.StructField('congestion_surcharge', types.DoubleType(), True), 
        types.StructField('tips', types.DoubleType(), True), 
        types.StructField('driver_pay', types.DoubleType(), True), 
        types.StructField('shared_request_flag', types.StringType(), True), 
        types.StructField('shared_match_flag', types.StringType(), True), 
        types.StructField('access_a_ride_flag', types.StringType(), True), 
        types.StructField('wav_request_flag', types.StringType(), True), 
        types.StructField('wav_match_flag', types.StringType(), True)
    ]
)

# now to read our parquet file into a spark dataframe again, but this time with a schema

df = spark.read.schema(schema).parquet("fhvhv_tripdata_2021-01.parquet")
df.head(5)
```

This code block should give us the desired output. Please take a look at the [notebook](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-5-batch-processing/code/04_pyspark.ipynb) for a better picture of what we have done so far!

## Partitions

A *Spark cluster* is composed of multiple *executors*. Each executor can process data independently in order to parallelize and speed up work.

In the previous example we read a single large parquet file. A file can only be read by a single executor, which means that the code we've written so far isn't parallelized and thus will only be run by a single executor rather than many at the same time.

In order to solve this issue, we can *split a file into multiple parts* so that each executor can take care of a part and have all executors working simultaneously. These splits are called **partitions**. Once the the executor finishes processing one partition it will move onto the next available partition until all partitions are complete.

![image](https://github.com/user-attachments/assets/33c71efe-c4f6-496a-a8ad-b5023c026082)

We will now partition the dataframe. This will create multiple files in parquet format.
```python
# create 24 partitions in our dataframe
df = df.repartition(24)
# parquetize and write to fhvhv/2021/01/ folder
df.write.parquet('fhvhv/2021/01/')
```

You may check the Spark UI at any time and see the progress of the current job, which is divided into stages which contain tasks. The tasks in a stage will not start until all tasks on the previous stage are finished.

When creating a dataframe, Spark creates as many partitions as CPU cores available by default, and each partition creates a task. Thus, assuming that the dataframe was initially partitioned into 6 partitions, the write.parquet() method will have 2 stages: the first with 6 tasks and the second one with 24 tasks.

Besides the 24 parquet files, you should also see a _SUCCESS file which should be empty. This file is created when the job finishes successfully.

Trying to write the files again will output an error because Spark will not write to a non-empty folder. You can force an overwrite with the mode argument:
```python
df.write.parquet('fhvhv/2021/01/', mode='overwrite')
```
The opposite of partitioning (joining multiple partitions into a single partition) is called *coalescing*.

# Spark DataFrames

As mentioned previously, Spark works with *DataFrames*.

We can create a dataframe from the parquet file directory we created in the previous section:
```python
df = spark.read.parquet('fhvhv/2021/01/')
```

Unlike CSV files, parquet files contain the schema of the dataset, so there is no need to specify a schema like we previously did when reading the CSV file. You can check the schema like this:
```python
df.printSchema()
```
>[!TIP]
> One of the reasons why parquet files are smaller than CSV files is because they store the data according to the datatypes, so integer values will take less space than long or string values.

There are many Pandas-like operations that we can do on Spark dataframes, such as:
* Column selection - returns a dataframe with only the specified columns.
```python
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')```
* Filtering by value - returns a dataframe whose records match the condition stated in the filter.
```python
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID').filter(df.hvfhs_license_num == 'HV0003')
```
* And many more. The official Spark documentation website contains a [quick guide for dataframes.](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)

## Actions vs transformations

In Apache Spark, operations on DataFrames are categorised into *transformations* and *actions*. Understanding the difference between these two is crucial for writing efficient Spark applications.

**Transformations**

Transformations are operations that create a new DataFrame from an existing one. They are lazy, meaning they are not executed immediately. Instead, Spark builds a logical plan (DAG, or Directed Acyclic Graph) of transformations, which is only executed when an action is called.

Characteristics of Transformations:
1. Lazy Evaluation: No computation happens until an action is triggered
2. Immutable: Transformations produce  a new DataFrame without modifying the original one
3. Examples:
    * `select(*cols)` - Selects specific columns from a DataFrame
    * `filter(condition)` – Filters rows based on a condition
    * `groupBy(*cols)` - Groups data by specified columns for aggregation
    * `orderBy(*cols, ascending=True)` - Sorts the DataFrame based on one or more columns 
    * `join(other, on, how=‘inner’` - Merges two DataFrames based on a common column using a specified join type
    * `withColumn(colName, colExpr)` - Adds or replaces a column using an expression
    * `drop(*cols)` - Removes specified columns from DataFrame
    * `union(otherDF)` - Combine two DataFrames with the same schema into one

**Actions**

Actions are operations that trigger the execution of the transformations and return a result to the driver program or write data to an external storage system. Actions are eager, meaning they force the computation of the logical plan.

Characteristics of Actions:
1. Eager Evaluation: 	Triggers the execution of the transformations.
2. Return Results: Actions return values (e.g. to driver) or write data to storage
3. Examples:
    * `count()` – Returns the number of rows in the DataFrame.
    * `collect()` – Retrieves all rows of the DataFrame as a list.
    * `show(n=20)` – Displays the first n rows of the DataFrame in a readable format.
    * `take(n)` – Returns the first n rows as a list (like collect() but limited).
    * `first()` – Returns the first row of the DataFrame.
    * `foreach(func)` – Applies a function to each row of the DataFrame (useful for side effects).
    * `write.save(path, format)` – Saves the DataFrame to a specified path in a given format (e.g., Parquet, CSV).

Actions vs Transformations in Apache Spark key difference summary:

| **Aspect**            | **Transformations**                          | **Actions**                              |
|------------------------|----------------------------------------------|------------------------------------------|
| **Execution**          | Lazy (no immediate execution)                | Eager (triggers execution)               |
| **Result**             | Returns a new DataFrame or RDD               | Returns a value or writes data           |
| **Examples**           | `select()`, `filter()`, `groupBy()`, `join()`| `count()`, `collect()`, `show()`, `save()`|
| **Purpose**            | Define the logical plan (DAG)                | Execute the logical plan and produce output |
| **Performance Impact** | No computation happens                       | Triggers computation and data processing |
| **Fault Tolerance**    | Part of the lineage (logical plan)           | Triggers execution of the lineage        |
| **Common Methods**     | `map()`, `flatMap()`, `filter()`, `union()`  | `collect()`, `take()`, `first()`, `foreach()` |

---

## Functions and UDFs

**Functions**

Besides the SQL and Pandas-like commands we've seen so far, Spark provides additional built-in functions that allow for more complex data manipulation. By convention, these functions are imported as follows:
```python
from pyspark.sql import functions as F
```
Here's an example of built-in function usage:
```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```
- `withColumn()` is a transformation that adds a new column to the dataframe.
  - NOTE: adding a new column with the same name as a previously existing column will overwrite the existing column!
- `select()` is another transformation that selects the stated columns.
- `F.to_date()` is a built-in Spark function that converts a timestamp to date format (year, month and day only, no hour and minute).
  
A list of built-in functions is available in the [official Spark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) page.

Besides these built-in functions, Spark allows us to create User Defined Functions (UDFs) with custom behavior for those instances where creating SQL queries for that behaviour becomes difficult both to manage and test.

**User-defined functions (UDFs)**

UDFs are regular functions which are then passed as parameters to a special builder. Let's create one:
```python
# A crazy function that changes values when they're divisible by 7 or 3
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'

# Creating the actual UDF
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
```

- `F.udf()` takes a function (`crazy_stuff()` in this example) as parameter as well as a `return_type` for the function (a string in our example).
- While `crazy_stuff()` is obviously non-sensical, UDFs are handy for things such as ML and other complex operations for which SQL isn't suitable or desirable. Python code is also easier to test than SQL.

We can then use our UDF in transformations just like built-in functions:
```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```
## Preparing Yellow and Green Taxi Data [OPTIONAL]

This is an optional section and you don’t have to go through it. The content covers the development of a `bash script` so as to be able to download the data that we need for subsequent sections into our local directory. Also, by taking this approach, we don’t have to run command line commands on our notebooks as well as being able to avoid certain schema inferring issues that we had seen how to tackle in the previous section. Again these issues don’t lie with `.parquet()` files and only occur in `.csv()` files, and since the data format from the NY Taxi page only has the `.parquet()` format, our version of the bash script will be more simplified than the one covered in the [video](https://www.youtube.com/watch?v=CI3P4tAtru4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=49). It would be recommended for beginners to go through the section, and if you decide to go with the `.csv` raw taxi files then execute the script that was prepared by the course instructor [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/code/download_data.sh).

Since we are trying to eventually compute some metrics similar to [week 4 dbt monthly zone revenue script](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-4-analytics-engineering/taxi_rides_ny/models/core/dm_monthly_zone_revenue.sql), we are going to download the full year data on a monthly basis for yellow and green trip taxi records for the year 2020 and 2021.

So let’s start out with a sample link for each taxi type from the [website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):
```url
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet
```
Right off the bat you can probably tell that the structure of both URLs are fairly similar with the exception of the taxi type and the year-month. We that being said, we can generalise the structure of the download URL by parameterising the variable as such:
```url
https://d37ci6vzurychx.cloudfront.net/trip-data/{TAXI_TYPE}_tripdata_{YEAR}-{MONTH}.parquet
``` 

With all that in mind we can proceed to construct our **download_data.sh** bash script:

1. First things first, we had identified the variables in the URL so it would be essential to start off with defining the parameters. These would be used as arguments when we run the script via command line, as such `$1` and `$2` represent the position of the input argument
```bash
TAXI_TYPE=$1
YEAR=$2
```

2. Next we create a variable called `URL_PREFIX` that contains the consistent part of the URL as follows:
```bash
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"
```

3. Since taxi type and year are given as input to the script, the only parameter that is left is the `MONTH` which we can be added to the URL through a for loop. Keep in mind that the months need to be zero-padded.  Also, `for` loops in bash operate in a code block that starts with a `do` command and ends with `done`.
```bash
for MONTH in {1..12}; do
# FMONTH is formatted month	
    FMONTH=`printf "%02d" ${MONTH}`
.
.
.
done
```

> [!NOTE]
> `printf` is a command in bash scripting to format and print output, similar to `echo` but with the formatting option.
> `"%02d" ` this format is ensuring that `MONTH` is zero-padded (0), is integer (%d) and has minimum width of 2 digits (2)

4. Now we continue to build inside the for loop - we need to define the URL of each month file for each taxi type, the local path in which the file is downloaded into, and finally the actual execution.
```bash
 # Construct the download URL
 URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

# Define local paths
LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

# print statement to show which file is being currently downloaded
echo "donwloading ${URL} to ${LOCAL_PATH}"

# Create destination directory if it does not exist - p flag parent directory
mkdir -p ${LOCAL_PREFIX}

# Download file into specified path using -O flag - actual execution 
wget ${URL} -O ${LOCAL_PATH}
```

As such the full script will be as follows:
```bash
# bash script for downloading NYC taxi trip data 

# Ensure the script exits immediately if any command fails
set -e

# setting some variables - script expects 2 arguments
TAXI_TYPE=$1
YEAR=$2

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Loop through each month (1..12) - "%02d" ensure that month number is zero-padded
for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    # Construct the download URL
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

    # Define local paths
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "donwloading ${URL} to ${LOCAL_PATH}"

    # Create destination directory if it does not exist - p flag parent directory
    mkdir -p ${LOCAL_PREFIX}
    # Download file into specified path using -O flag
    wget ${URL} -O ${LOCAL_PATH}

done
```

> [!IMPORTANT]
> `set -e` ensure that the script exits immediately at the first non-zero code if a command fails (e.g.. if there is a 404 error)
> Also, we need to make the bash script an executable if we want it to run via command line. Hence we need to run `chmod +x download_data.sh` before running the script.
> When running the script, run it like `./download_data.sh yellow 2020` with `./` at the start as without it terminal will interpret as a command rather that the location to the executable.

>[!TIP]
> There are a lot of files being downloaded into many different folder layers by the script and an easy way of seeing them all in a clear way to see if the script is operating as it should is via the command line tool called `tree`. If its not installed, do so by running the command `sude apt-get install tree`. After which you get the tree structure of the root folder (e.g. `tree data/`), you should see all the files in their respective folder.

>[!NOTE]
>  The approach taken in this section downloads the raw files from the NY taxi website in `.parquet` format. The advantage to this approach is that we do not have to define the schema or reformat it just as we would have done for a `.csv` file. But if you had followed the instructor in the course and had downloaded the `.csv` file instead, please find the link to the notebook for defining the schema [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/code/05_taxi_schema.ipynb).

## Spark SQL

We already mentioned at the beginning that there are other tools for expressing batch jobs as SQL queries. As such, Spark can also run SQL queries, which can come in handy if you already have a Spark cluster and setting up an additional tool for sporadic use isn't desirable.

Therefore, this section would be focusing on running batch jobs using `spark sql` by replicating dm_monthyl_zone_revenue.sql model from lesson 4 which is as follows:
```sql
{{
    config(
        materialized='table'
    )
}}

with fact_trips as (
    select
        *
    from {{ ref('fact_trips') }}
)

select
    -- Revenue grouping
    pickup_zone as revenue_zone,
    {{dbt.date_trunc("month", "pickup_datetime")}} as revenue_month,

    service_type,

    -- Revenue calculation
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

    from fact_trips
    group by 1,2,3
```

1. `fact_trips` table is a combination of yellow and green taxi data, hence our first step is to load the downloaded yellow and green taxi data into a spark session and combine them. But before combining we need to rename columns as well as identifying similar fields between both datasets.
```python
df_green = spark.read.parquet("data/raw/green/*/*")

# Change field name before combining
df_green = df_green.withColumnsRenamed({'lpep_pickup_datetime':'pickup_datetime',
                                        'lpep_dropoff_datetime':'dropoff_datetime'})

# First let us identify the similar column fields between both datasets
same_fields = [col for col in df_yellow.columns if col in df_green.columns]

```
> **NOTE** :  We are only going through the changes made for green taxi spark data frame, the same steps should be applied to yellow taxi spark dataframe. Please take a look at the full code block in the notebook [here](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-5-batch-processing/code/06_spark_sql.ipynb).

2. Next we need to filter each spark dataframe by selecting the common columns, followed by creating a new column called `service_type` to help differentiate.
```python
# We need to filter each dataframe as well as create new column `service_type`
from pyspark.sql import functions as F

df_green_filtered = df_green.select(same_fields).withColumn('service_type', F.lit('green'))
df_yellow_filtered = df_yellow.select(same_fields).withColumn('service_type', F.lit('yellow'))

# Now to combine both datasets
df_trips_data = df_green_filtered.unionAll(df_yellow_filtered)
```
> **REMINDER** : We select common fields so as to output a combined dataframe that is appended on top of each other without any empty records. Hope that makes sense.

3. Now to write a spark sql query.  We would not be able to query directly from the spark dataframe and hence we need to convert it into a temporary table first using the `registerTempTable()` method (this method is deprecated).
```python
# Convert spark dataframe into a temporary table
df_trips_data.createOrReplaceTempView("fact_trips")
```

4. Let’s do a sample query to see if all works as expected.
```python
# Test query #1
spark.sql(
    """
SELECT
    *
FROM fact_trips
LIMIT 10;
"""
).show()

# Test query #2
spark.sql("""
SELECT
    service_type,
    count(1)
FROM fact_trips
GROUP BY service_type;
"""
).show()
```

5. Finally, now that we know spark sql is operating as per expected, we can recreate the dbt script into a spark sql query as follows:
```sql
# SQL query for dm_monthly_zone_revenue.sql

df_result = spark.sql(
    """
    SELECT
        PULocationID AS revenue_zone,
        date_trunc('month', pickup_datetime) AS revenue_month,
        service_type,

        -- Revenue calculation
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

         -- Additional calculations
        AVG(passenger_count) AS avg_monthly_passenger_count,
        AVG(trip_distance) AS avg_monthly_trip_distance
    FROM
        fact_trips
    GROUP BY PULocationID, date_trunc('month', pickup_datetime), service_type
    """
)
```

However, with our current dataset, this will create more than 200 parquet files of very small size, which isn't very desirable. In order to reduce the amount of files, we need to reduce the amount of partitions of the dataset, which is done with the `coalesce()` methodl,  this reduces the amount of partitions to just 1.:
```python
df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')
```
# Spark Internals

So far, we have been running Spark in **local mode** using `master("local[*]”)`, meaning that all computations are executed on our local machine, whether it be a laptop or a virtual machine. In this setup, Spark utilizes the available CPU cores to process batch jobs. Unlike a distributed cluster setup where multiple executors run on different nodes, **local mode runs a single executor within a single JVM**, leveraging multiple threads to parallelize tasks. This allows us to experiment with Spark locally without requiring a full cluster environment This is essentially a local cluster and in the subsequent section, we would expand this concept by discussing an actual real distributed cluster and how it operates.

Conceptually this is what our Spark components look like:
![image](https://github.com/user-attachments/assets/1818c19d-d3b6-4b3b-ab59-0be80e2806d3)

## Anatomy of a Spark Cluster

Usually, the way it works is that you first create a script in Python, Scala or Java with some Spark code. And you usually do this from your laptop, or if you use Airflow for scheduling Spark jobs then Airflow executor does that. Let's assume you do it from your laptop.

On your laptop you will have a package with some spark code that you wrote in Python. You also have a Spark Cluster. In this cluster we have a computer that we call `Spark Master` – it’s role is a coordinating role. So when we are on our laptop with our Spark code we submit it to Spark Master and this is actually the url that we have. Remember we opened this locally – it was `localhost:4040`. It usually has a Web UI on a `port 4040` that we can use to connect to this master and see what is being executed on the cluster. So this you can think about as an entry point to a `Spark Cluster`. We use a special command called `spark submit` to send our package with our code to the master, we also specify some information like what kind of resources we need for this job and so on.

![image](https://github.com/user-attachments/assets/7cdebb16-9a8c-4082-92be-9cc50bef7442)

Then on the cluster we have computers that actually execute these jobs – they are called `executors`. These are the machines that actually do the computation. When we submit a job to `Spark Master`, the master coordinates between executors. So for example with our job it might send it to five executors with instructions on what to do. Then of course master should be up and running all the time. Let’s say one of the executors goes away for whatever reason then master knows about this and it reassigns the task to another executor.

Then what executors need to do is they need to pull some data and they process the data. For example, imagine we have a dataframe (image above) and this dataframe consists of partitions (a bunch of parquet files) . When we submit a job to a spark master, spark sends some information to executors and each executor pulls a partition. Each executor works through it’s allocated partition and they then mark their task as completed. Then they will move on to another task once they are done.

Each executor will fetch a dataframe partition stored in a Data Lake (usually S3, GCS or a similar cloud provider), do something with it and then store it somewhere, which could be the same Data Lake or somewhere else. If there are more partitions than executors, executors will keep fetching partitions until every single one has been processed.

**Hadoop/HDFS**

This is in contrast to [Hadoop](https://hadoop.apache.org/), another data analytics engine, whose executors locally store the data they process. Partitions in Hadoop are duplicated across several executors for redundancy, in case an executor fails for whatever reason (Hadoop is meant for clusters made of commodity hardware computers). However, data locality has become less important as storage and data transfer costs have dramatically decreased and nowadays it's feasible to separate storage from computation, so Hadoop has fallen out of fashion.

**S3/GCS**

However, these days because we have multiple cloud providers (e.g. S3, Google Cloud Storage, etc.) the cluster and dataframe usually live in the same data center – the spark cluster and the storage are in the same data center. Downloading 100MB for an executor is very fast. It’s a little bit slower than (eg. Hadoop/HDFS model) but it’s not significantly slower. Executors now instead of keeping all this data locally they can just pull data from S3/GCS, process this and then save the results back to our Data Lake. This is why Hadoop and HDFS are becoming less popular. There is overhead with Hadoop and HDFS. Now you just have a Spark Cluster and then you keep data in your Cloud storage.

<details>
  <summary>Click to expand summary</summary>
To summarise you have a driver that submits a job and the driver submits a job to a Spark Master. The driver can be an operator in Airflow (a task in Airflow that does ‘spark submit’) or something else that submits a job. Master is the thing that coordinates everything and executors are machines that are doing the actual computations. Spark keeps track of which machines are healthy and if some machine becomes unhealthy it reassigns the work and we keep the data in cloud storage [these days]. We read from the cloud storage and write results back to the cloud storage.
</details>

## GroupBy in Spark

So we had executed a spark query in the previous section, let’s execute another one to enable the understanding of what exactly happens behind the scenes as we execute the query. As always we start by:

1. Creating a session
```python
spark = SparkSession.builder.master("local[*]").appName("spark_sql_groupby_join").getOrCreate()
```
2. Next, we load the data. For this example, we will load green taxi data that we had downloaded in the previous section.
```python
df_green = spark.read.parquet("data/raw/green/*/*")
```
3. Now that we have loaded our sample data (i.e. green taxi data for 2020 and 2021) for this exercise, let's proceed with another `sql` query, but similar to the previous exercise, that involves a `groupby` statement.
```python
# Firstly, we need to always create a temp table to be queried on - sql queries cannot be queried on a spark dataframe
df_green.registerTempTable("green")
```
4. Next, we write a query that breaks down the revenue as well as the number of trips by hour by zone
```python
df_green_revenue = spark.sql(
    """
SELECT
    EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) as revenue,
    COUNT(1) as number_records
FROM green
WHERE lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY 1,2
ORDER BY 1,2
"""
)
```
5. Lastly, lets write the output of our query into a parquet file
```python
df_green_revenue.write.parquet('data/report/revenue_green', mode='overwrite')
```

Since the data is split along partitions, it's likely that we will need to group data which is in separate partitions, but executors only deal with individual partitions. Spark solves this issue by separating the grouping in 2 stages:

* In the first stage, each executor groups the results in the partition they're working on and outputs the results to a temporary partition. These temporary partitions are the **_intermediate results_**.

![partition 1](https://github.com/user-attachments/assets/00819def-273c-4b4f-93e7-5cfe5a2cd714)

* The second stage **_reshuffles_** the data: Spark will put all records with the **_same keys_** (in this case, the `GROUP BY` keys which are hour and zone) in the **_same partition_**. The algorithm to do this is called `external merge sort`. Once the reshuffling has finished, we can once again apply the `GROUP BY` to these new partitions and reduce the records to the **_final output_**.

>NOTE : The shuffled partitions may contain more than one key, but all records belonging to a key should end up in the same partition.

![Pasted Graphic 1](https://github.com/user-attachments/assets/6f6e14e8-6d74-4c3e-92e5-3bf2dafc49a5)

Running the query should display the following DAG in the Spark UI:

![Pasted Graphic 2](https://github.com/user-attachments/assets/ff6583b3-180d-442b-b330-e05b378f5de6)

> [!NOTE]
>  The `Exchange` tasks refer to the reshuffling process.

If we were to add sorting to the query (adding a `ORDER BY 1,2` at the end), Spark would perform a very similar operation to `GROUP BY` after grouping the data. The resulting DAG would look liked this:

![Pasted Graphic 3](https://github.com/user-attachments/assets/506929ae-91b3-4850-8825-e947d7d0ef85)

> [!IMPORTANT]
> By default, Spark will repartition the dataframe to 200 partitions after shuffling data. For the kind of data we're dealing with in this example this could be counterproductive because of the small size of each partition/file, but for larger datasets this is fine. Shuffling is an **_expensive operation_**, so it's in our best interest to reduce the amount of data to shuffle when querying.Keep in mind that repartitioning also involves shuffling data.

## Joins in Spark

Joining tables in Spark is implemented in a similar way to `GROUP BY` and `ORDER BY`, but there are two distinct cases: joining two large tables as well as joining a large table and a small table.

1. **Joining two large tables**

Let's assume that we've created a `df_yellow_revenue` Spark DataFrame in the same manner as the `df_green_revenue` we created in the previous section. We want to join both tables into one table, and the concept for doing so visually is as follows:

![Pasted Graphic](https://github.com/user-attachments/assets/a8d050cc-820f-46a7-ae35-e2b63e718834)

The above is ultimately what we want to achieve but before doing so, we will need to create temporary Spark DataFrames with changed column names for each so that we can tell apart data from their respective original tables:

```python
# Changing column names before joining with yellow table
df_green_revenue_tmp = df_green_revenue.withColumnsRenamed(
    {"revenue":"green_revenue", "number_records": "green_number_records"})

# Repeating same steps for yellow revenue table
df_yellow_revenue_tmp = df_yellow_revenue.withColumnsRenamed(
    {"revenue":"yellow_revenue", "number_records": "yellow_number_records"}
```

Now to merge both tables on common keys, in our case it’s our group by keys `hour` and `zone`. And we have to specify an `OUTER` join as we want all records from both tables. Hence, in an event where there is no record of `revenue` and `number_records` for green data but it’s available in yellow data, then the calculated fields will show `NULL`.

```python
df_merged = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=["hour","zone"], how='outer')
```

Now let’s consider what actually happens behind the scenes. The first thing that Spark does after the query is executed, it evaluates the size of the datasets and the available resources to decide the best join strategies which include:
- **Sort-merge join** : Used for large datasets
- **Broadcast join** : Used when one of the datasets is small enough to be broadcast to all nodes
- **Shuffle hash join** : Used for medium-sized datasets

Given the size of our dataset, Spark uses Sort-Merge Join (SMJ) by default since Spark 2.3, not just because of large datasets but also because it is efficient for joins on sorted and partitioned data. But if the data is already sorted and partitioned (e.g., stored as bucketed tables in Hive), Spark can skip sorting, making the join faster. Or If one of the tables is small, Spark might use Broadcast Join instead, avoiding sorting and shuffling altogether.

Step 1: Shuffling and partitioning using `sort-merge` algorithm
- Spark shuffles the data so that records with the same `(hour, zone)` end up in the same partition.
- So taking our example image from above, each partition **contains a subset of data** from both tables (`yellow_taxi` and `green_taxi`), grouped by key.

> [!TIP]
> Please note that shuffling is an expensive operation, hence always look at the query's logical plan to see if it can be optimised (e.g. if you need to make multiple joins and some joins are made on the same key, then order them one after the other so that it does not need to be repartitioned again). This is important because the `shuffle write` can be so large to the point there may not be enough memory and `disk write` may be needed to perform the task (i.e. `disk spillage`).
> Shuffling is expensive because potentially very large amounts of data need to be transferred over the network. Moreover, the actual reorganisation of records can be computationally expensive given the in-memory data structures used by Spark. Hence, it is important to optimise query involving `join()`, `groupBy()`, `repartition()`, `distinct()` etc.
> An example of a shuffle process:

![image](https://github.com/user-attachments/assets/fb772c07-ffd8-4756-a4f4-0b85b3135cc7)

Step 2: Sorting
- After which Spark then sorts the records within each partition by (hour, zone), ensuring that matching records from both tables are adjacent for efficient merging.

> [!IMPORTANT]
> The goal is parallelism and memory optimization—each executor processes only a subset of the data.
>  Sorting is a required step for the sort-merge join to work efficiently. Without sorting, Spark would need nested loops, which would be far slower.

![Partition 1](https://github.com/user-attachments/assets/0daac352-c95c-47f3-bca7-f65d1beb3d8d)

Step 3: Merged
- Now that each partition is sorted:
    - Spark **does not explicitly** split the data into two separate lists. - one for `yellow_taxi` and one for `green_taxi`.
    - Instead, sorting ensures that **yellow and green records with the same join key are adjacent.**
    - Spark then performs a **two-pointer merge**, efficiently joining records row by row.

For example, our 3 partitions above may look something like this:
```python
# implicit list
[
	[(23,256), 47793.06, 2202],  # Yellow
	[(23,256), 11064.3, 542],	# Green
	[(23,257), 1768.64, 54],
	[(23,257), 387.83, 17],
]
```

The merge process scans through the sorted partition and combines matching records. In the case of `outer` join, if a key only exists in one list then the merge will introduce `NULL` values.

>[!NOTE]
> `on=`receives a list of columns by which we will join the tables. This will result in a primary composite key for the resulting table.
> `how=` specifies the type of JOIN to execute.

2. **Joining a large table and a small table**

In such a scenario where you are trying to merge one small and one large table, Spark uses the `broadcast hash` join. It got its name because the smaller dataset can fit in memory, it is broadcasted to all of  the executors in the cluster. 

![Pasted Graphic 1](https://github.com/user-attachments/assets/ea1cf3e4-c4e8-4717-8248-34625f364b4b)

As you can tell, after the significantly smaller (`zones` table) is broadcasted, a copy of it lives inside each executor after which a standard hash join is performed to be joined with the larger `revenue` table.

Let's now use the `zones lookup` table to match each zone ID to its corresponding name.
```python
# Joining both tables
df_result = df_merged.join(
    df_zones, df_merged.zone == df_zones.LocationID
    ).drop("LocationID","zone").withColumnsRenamed(
    {"Borough":"pickup_Borough", "service_zone": "pickup_service_zone"}
```
> [!NOTE]
> We merge tables on their respective IDs (i.e. `zone` for left larger table and `LocationID` for the smaller right table).
> After which, we perform a `drop` and change column names using `withColumnsRenamed` .
> We also `write` out the data into a parquet file called `revenue-zones`
> Please find full notebook [here](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-5-batch-processing/code/07_groupby_join.ipynb).
> Shuffling isn't needed because each executor already has all of the necessary info to perform the join on each partition, thus speeding up the join operation by orders of magnitude.

