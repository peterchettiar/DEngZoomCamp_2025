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

**Batch processing** is a computational technique in which a collection of data is amassed and then processed in a single operation, often without the need for real-time interaction. This approach is a particularly effective method for handling large volumes of data, where tasks can be executed as a group during off-peak hours to optimmize system resources and throughput. End-of-day bank reconcilliation and payroll processing is an example of batch processing.

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


## Types of batch jobs


## Orchestrating batch jobs


## Advantages and disadvantages of batch jobs


# Introduction to Spark

## What is Spark?


## Why do we need Spark?

