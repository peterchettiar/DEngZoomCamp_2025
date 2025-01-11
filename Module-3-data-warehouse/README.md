# Week 3 Notes

### Table of contents

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

## OLAP vs OLTP

_[Video source](https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=25)_

In Data Science, when we're discussing data processing systems, there are 2 main types: **OLAP** and **OLTP** systems.

* ***OLTP***: Online Transaction Processing.
* ***OLAP***: Online Analytical Processing.

An intuitive way of looking at both of these systems is that OLTP systems are "classic databases" whereas OLAP systems are catered for advanced data analytics purposes.

|   | OLTP | OLAP |
|---|---|---|
| Purpose | Control and run essential business operations in real time | Plan, solve problems, support decisions, discover hidden insights |
| Data updates | Short, fast updates initiated by user | Data periodically refreshed with scheduled, long-running batch jobs |
| Database design | Normalized databases for efficiency | Denormalized databases for analysis |
| Space requirements | Generally small if historical data is archived | Generally large due to aggregating large datasets |
| Backup and recovery | Regular backups required to ensure business continuity and meet legal and governance requirements | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| Productivity | Increases productivity of end users | Increases productivity of business managers, data analysts and executives |
| Data view | Lists day-to-day business transactions | Multi-dimensional view of enterprise data |
| User examples | Customer-facing personnel, clerks, online shoppers | Knowledge workers such as data analysts, business analysts and executives |

## What is a Data Warehouse?

A **Data Warehouse** (DW) is an ***OLAP solution*** meant for ***reporting and data analysis***. Unlike Data Lakes, which follow the ELT model, DWs commonly use the ETL model which was [explained in lesson 2](https://github.com/peterchettiar/DEngZoomCamp_2025/blob/main/Module-2-workflow-orchestration/README.md#etl-vs-elt).

A DW receives data from different ***data sources*** which is then processed in a ***staging area*** before being ingested to the actual warehouse (a database) and arranged as needed. DWs may then feed data to separate ***Data Marts***; smaller database systems which end users may use for different purposes.

![image](https://github.com/user-attachments/assets/6c2b9869-9193-41ee-925e-042b0779ce62)

