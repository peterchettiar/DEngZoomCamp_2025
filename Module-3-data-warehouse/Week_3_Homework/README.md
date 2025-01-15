# Module 3 Homework Submission

### Question 1: What is count of records for the 2022 Green Taxi Data?

> Code:
```sql
-- Create a table with only the 2022 data for simplicity
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_tlc_415106/greentaxi_tripdata_2022/*.parquet']
);

-- Count of green taxi trips in 2022
SELECT count(*) FROM `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022_external`;
```
> Answer:
`840,402`

### Question 2: What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

> Code:
```sql
-- import an external table into BQ as a regular internal table
CREATE OR REPLACE TABLE ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022 AS
SELECT * FROM ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022_external;

-- Count of green taxi trips in 2022 internal table
SELECT count(*) FROM `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022`;
```

> Answer:
`0 MB for the External Table and 0MB for the Materialized Table`


### Question 3: How many records have a fare_amount of 0?

> Code:
```sql
  -- Count of green taxi trips in 2022 with fare amount = 0
SELECT
  COUNT(*)
FROM
  `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022_external`
WHERE
  fare_amount = 0;
```

> Answer:
`1,622`

### Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

> Code:
```sql
  -- Create a partioned and clustered table
CREATE OR REPLACE TABLE
  ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022_partioned_clustered
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY
  PUlocationID AS
SELECT
  *
FROM
  `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022`
```

> Answer:
`Partition by lpep_pickup_datetime Cluster on PUlocationID`

### Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

> Code:
```sql
  -- Query distinct PULocationID between specific dates from internal table
SELECT
  DISTINCT PULocationID
FROM
  `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022`
WHERE
  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01'
  AND '2022-06-30';
  -- Query distinct PULocationID between specific dates from partioned table
SELECT
  DISTINCT PULocationID
FROM
  `ny-rides-peter-415106.nyc_tlc_data.greentaxi_trips_2022_partioned_clustered`
WHERE
  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01'
  AND '2022-06-30';
```

> Answer:
`12.82 MB for non-partitioned table and 1.12 MB for the partitioned table`

### Question 6: Where is the data stored in the External Table you created?

Key Characteristics of External Tables
Data is NOT Imported into BigQuery:

Unlike standard or materialized tables, external tables don’t copy or store data in BigQuery’s internal columnar storage.
Query Reads Data Directly from Source:

Every query on the external table fetches the required data directly from the external source.
Performance Considerations:

Query performance depends on the external storage system's speed and location (e.g., querying data from GCS in the same region as your BigQuery instance is faster and more cost-effective).

> Answer: `GCP Bucket`

### Question 7: It is best practice in Big Query to always cluster your data


The statement "It is best practice in BigQuery to always cluster your data" is not entirely accurate. While clustering can significantly improve query performance and reduce costs in specific scenarios, it is not universally beneficial. The decision to cluster depends on your data and query patterns.

When is Clustering Beneficial?
Frequent Filtering or Sorting on Specific Columns:

If your queries often filter or sort by specific columns, clustering can reduce the amount of data scanned and improve query performance.
Example: Queries like WHERE user_id = '123' or ORDER BY event_timestamp benefit from clustering on user_id or event_timestamp.
High Cardinality Columns:

Clustering works well when the clustering columns have high cardinality (many unique values), as this increases the likelihood of clustering effectiveness.
Example: A column like user_id or transaction_id is ideal for clustering.
Large Tables:

For large tables with billions of rows, clustering can significantly reduce the number of blocks scanned, resulting in faster and cheaper queries.

If it is the opposite of the scenarios above, then clustering may not be useful.

> Answer: `False`

### Question 8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

> Answer: `0 Bytes`

When you query a materialized table, BigQuery doesn't re-scan the underlying source data but reads the pre-computed results directly which significantly reduces the data read and query processing time. For simple aggregate queries like `COUNT(*)`, BigQuery uses pre-computed statistics stored alongside the materialized table.
