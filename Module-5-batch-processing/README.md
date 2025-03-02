# Week 5 Notes

### Table of contents

- [Introduction to Batch Processing](#introduction-to-batch-processing)
  - [Batch versus Streaming](#batch-versus-streaming)
  - [Types of batch jobs](#types-of-batch-jobs)
  - [Orchestrating batch jobs](#orchestrating-batch-jobs)
  - [Advantages and disadvantages of batch jobs](#advantages-and-disadvantages-of-batch-processing)
- [Introduction to Spark](#introduction-to-spark)
  - [What is Spark?](#what-is-spark)

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

![image](https://github.com/user-attachments/assets/efd17a0d-7043-4783-8643-7cad36d1daed)

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

![image](https://github.com/user-attachments/assets/e66277a4-d2d8-4622-8dc1-73539d030829)

## Advantages and disadvantages of batch jobs

| Advantages | Disadvantages |
|------------|---------------|
| Easy to manage | Delay |
| Retry | |
| Scale | |

# Introduction to Spark

## What is Spark?


## Why do we need Spark?

