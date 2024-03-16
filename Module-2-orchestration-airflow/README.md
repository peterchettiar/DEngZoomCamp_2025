# Week 2 Notes


### Table of contents

- [Data Ingestion](#data-ingestion)
- [Data Lake](#data-lake)
  - [What is a Data Lake?](#what-is-a-data-lake)
  - [Data Lake vs Data Warehouse](#data-lake-vs-data-warehouse)
  - [ETL vs ELT](#etl-vs-elt)
  - [Data Swamp - Data Lakes gone wrong](#data-swamp---data-lakes-gone-wrong)
  - [Data Lake Cloud Providers](#data-lake-cloud-providers)
- [Orchestration with Airflow](#orchestration-with-airflow)
  - [Introduction to Workflow Orchestration](#introduction-to-workflow-orchestration)
  - [Airflow architecture](#airflow-architecture)
  - [Setting up Airflow with Docker](#setting-up-airflow-with-docker)
    - [Pre-requisites](#pre-requisites)
    - [Setup (full version)](#setup-full-version)
    - [Setup (lite version)](#setup-lite-version)
    - [Execution](#execution)
  - [Creating a DAG](#creating-a-dag)
  - [Running DAGs](#running-dags)
  - [Airflow and DAG tips and tricks](#airflow-and-dag-tips-and-tricks)
- [Airflow in action](#airflow-in-action)
  - [Ingesting data to local Postgres with Airflow](#ingesting-data-to-local-postgres-with-airflow)
  - [Ingesting data to GCP](#ingesting-data-to-gcp)
- [GCP's Transfer Service](#gcps-transfer-service)
  - [Creating a Transfer Service from GCP's web UI](#creating-a-transfer-service-from-gcps-web-ui)
  - [Creating a Transfer Service with Terraform](#creating-a-transfer-service-with-terraform)

# Data Ingestion

This lesson will cover the topics of _Data Lake_ and _pipelines orchestration with Airflow_.

_[Back to the top](#table-of-contents)_

# Data Lake

_[Video source](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=16)_

## What is a Data Lake?

A ***Data Lake*** is a _central repository_ that holds _big data_ from many sources.

The _data_ in a Data Lake could either be structured, unstructured or a mix of both.

The main goal behind a Data Lake is being able to ingest data as quickly as possible and making it available to the other team members.

A Data Lake should be:
* Secure
* Scalable
* Able to run on inexpensive hardware

## Data Lake vs Data Warehouse

![image](https://github.com/peterchettiar/DEngZoomCamp_2024/assets/89821181/6f95be17-1187-42f7-ad21-78fbe5eec1b7)

A Data Lake (DL) is not to be confused with a Data Warehouse (DW). There are several differences:

* Data Processing:
  * DL: The data is **raw** and has undergone minimal processing. The data is generally unstructured.
  * DW: the data is **refined**; it has been cleaned, pre-processed and structured for specific use cases.
* Size:
  * DL: Data Lakes are **large** and contains vast amounts of data, in the order of petabytes. Data is transformed when in use only and can be stored indefinitely.
  * DW: Data Warehouses are **small** in comparison with DLs. Data is always preprocessed before ingestion and may be purged periodically.
* Nature:
  * DL: data is **undefined** and can be used for a wide variety of purposes.
  * DW: data is historic and **relational**, such as transaction systems, etc.
* Users:
  * DL: Data scientists, data analysts.
  * DW: Business analysts.
* Use cases:
  * DL: Stream processing, machine learning, real-time analytics...
  * DW: Batch processing, business intelligence, reporting.

Data Lakes came into existence because as companies started to realize the importance of data, they soon found out that they couldn't ingest data right away into their DWs but they didn't want to waste uncollected data when their devs hadn't yet finished developing the necessary relationships for a DW, so the Data Lake was born to collect any potentially useful data that could later be used in later steps from the very start of any new projects.

## ETL vs ELT

When ingesting data, DWs use the ***Export, Transform and Load*** (ETL) model whereas DLs use ***Export, Load and Transform*** (ELT).

The main difference between them is the order of steps. In DWs, ETL (Schema on Write) means the data is _transformed_ (preprocessed, etc) before arriving to its final destination, whereas in DLs, ELT (Schema on read) the data is directly stored without any transformations and any schemas are derived when reading the data from the DL.

## Data Swamp - Data Lakes gone wrong

Data Lakes are only useful if data can be easily processed from it. Techniques such as versioning and metadata are very helpful in helping manage a Data Lake. A Data Lake risks degenerating into a ***Data Swamp*** if no such measures are taken, which can lead to:
* No versioning of the data
* Incompatible schemes for the same data
* No metadata associated
* Joins between different datasets are not possible

## Data Lake Cloud Providers

* Google Cloud Platform > [Cloud Storage](https://cloud.google.com/storage)
* Amazon Web Services > [Amazon S3](https://aws.amazon.com/s3/)
* Microsoft Azure > [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)

_[Back to the top](#table-of-contents)_

# Orchestration with Airflow

## Introduction to Workflow Orchestration

_[Video source](https://www.youtube.com/watch?v=0yK7LXwYeD0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=17)_

In the previous lesson we saw the definition of [data pipeline](1_intro.md#data-pipelines) and we created a [pipeline script](../1_intro/ingest_data.py) that downloaded a CSV and processed it so that we could ingest it to Postgres.

The script we created is an example of how **NOT** to create a pipeline, because it contains 2 steps which could otherwise be separated (downloading and processing). The reason is that if our internet connection is slow or if we're simply testing the script, it will have to download the CSV file every single time that we run the script, which is less than ideal.

Ideally, each of these steps would be contained as separate entities, like for example 2 separate scripts. For our pipeline, that would look like this:

```
(web) → DOWNLOAD → (csv) → INGEST → (Postgres)
```

We have now separated our pipeline into a `DOWNLOAD` script and a `INGEST` script.

In this lesson we will create a more complex pipeline:

```
(web)
  ↓
DOWNLOAD
  ↓
(csv)
  ↓
PARQUETIZE
  ↓
(parquet) ------→ UPLOAD TO S3
  ↓
UPLOAD TO GCS
  ↓
(parquet in GCS)
  ↓
UPLOAD TO BIGQUERY
  ↓
(table in BQ)
```
_Parquet_ is a [columnar storage datafile format](https://parquet.apache.org/) which is more efficient than CSV.

This ***Data Workflow*** has more steps and even branches. This type of workflow is often called a ***Directed Acyclic Graph*** (DAG) because it lacks any loops and the data flow is well defined.

The steps in capital letters are our ***jobs*** and the objects in between are the jobs' outputs, which behave as ***dependencies*** for other jobs. Each job may have its own set of ***parameters*** and there may also be global parameters which are the same for all of the jobs.

A ***Workflow Orchestration Tool*** allows us to define data workflows and parametrize them; it also provides additional tools such as history and logging.

The tool we will focus on in this course is **[Apache Airflow](https://airflow.apache.org/)**, but there are many others such as Luigi, Prefect, Argo, etc.

## Airflow architecture

_[Video source](https://www.youtube.com/watch?v=lqDMzReAtrw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=18)_

A typical Airflow installation consists of the following components:

![airflow architecture](https://airflow.apache.org/docs/apache-airflow/stable/_images/arch-diag-basic.png)

* The **scheduler** handles both triggering scheduled workflows as well as submitting _tasks_ to the executor to run. The scheduler is the main "core" of Airflow.
* The **executor** handles running tasks. In a default installation, the executor runs everything inside the scheduler but most production-suitable executors push task execution out to _workers_.
* A **worker** simply executes tasks given by the scheduler.
* A **webserver** which seves as the GUI.
* A **DAG directory**; a folder with _DAG files_ which is read by the scheduler and the executor (an by extension by any worker the executor might have)
* A **metadata database** (Postgres) used by the scheduler, the executor and the web server to store state. The backend of Airflow.
* Additional components (not shown in the diagram):
  * `redis`: a _message broker_ that forwards messages from the scheduler to workers.
  * `flower`: app for monitoring the environment, available at port `5555` by default.
  * `airflow-init`: initialization service which we will customize for our needs.

Airflow will create a folder structure when running:
* `./dags` - `DAG_FOLDER` for DAG files
* `./logs` - contains logs from task execution and scheduler.
* `./plugins` - for custom plugins

Additional definitions:
* ***DAG***: Directed acyclic graph, specifies the dependencies between a set of tasks with explicit execution order, and has a beginning as well as an end. (Hence, “acyclic”). A _DAG's Structure_ is as follows:
  * DAG Definition
  * Tasks (eg. Operators)
  * Task Dependencies (control flow: `>>` or `<<` )  
* ***Task***: a defined unit of work. The Tasks themselves describe what to do, be it fetching data, running analysis, triggering other systems, or more. Common Types of tasks are:
  * ***Operators*** (used in this workshop) are predefined tasks. They're the most common.
  * ***Sensors*** are a subclass of operator which wait for external events to happen.
  * ***TaskFlow decorators*** (subclasses of Airflow's BaseOperator) are custom Python functions packaged as tasks.
* ***DAG Run***: individual execution/run of a DAG. A run may be scheduled or triggered.
* ***Task Instance***: an individual run of a single task. Task instances also have an indicative state, which could be `running`, `success`, `failed`, `skipped`, `up for retry`, etc.
    * Ideally, a task should flow from `none`, to `scheduled`, to `queued`, to `running`, and finally to `success`.
