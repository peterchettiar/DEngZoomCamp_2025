# Module 3 Homework Submission

## Question 1: What is count of records for the 2022 Green Taxi Data?

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

## Question 2: What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

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


## Question 3: How many records have a fare_amount of 0?

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
