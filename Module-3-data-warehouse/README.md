# Week 3 Notes

### Table of contents

- [Data Warehouse](#data-warehouse)
- [OLAP vs OLTP](#olap-vs-oltp)
- [What is a Data Warehouse?](#what-is-a-data-warehouse)
- [BigQuery](#bigquery)
  - [Pricing](#pricing)
  - [External tables](#external-tables)
  - [Partitions](#partitions)
  - [Clustering](#clustering)
  - [Partitioning vs Clustering](#partitioning-vs-clustering)
  - [Best practices](#best-practices)
  - [Internals](#internals)
    - [BigQuery Architecture](#bigquery-architecture)
    - [Column-oriented vs record-oriented storage](#column-oriented-vs-record-oriented-storage)
- [Machine Learning with BigQuery](#machine-learning-with-bigquery)
  - [Introduction to BigQuery ML](#introduction-to-bigquery-ml)
  - [BigQuery ML deployment](#bigquery-ml-deployment)
- [Integrating BigQuery with Airflow](#integrating-bigquery-with-airflow)
  - [Airflow setup](#airflow-setup)
  - [Creating a Cloud Storage to BigQuery DAG](#creating-a-cloud-storage-to-bigquery-dag)

# Data Warehouse

This lesson will cover the topics of _Data Warehouse_ and _BigQuery_.
# OLAP vs OLTP

_[Video source](https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=25)_

In Data Sxience, when we're discussing data processing systems, there are 2 main types: **OLAP** and **OLTP** systems.

* ***OLTP***: Online Transaction Processing.
* ***OLAP***: Online Analytical Processing.
