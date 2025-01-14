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


