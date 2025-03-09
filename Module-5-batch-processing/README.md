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

Installation steps:
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

Now that we have setup `PySpark`, let’s test it out. Please take a look at [pyspark-demo.ipynb](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-5-batch-processing/notebooks/pyspark-demo.ipynb) for an introduction to a spark session.
