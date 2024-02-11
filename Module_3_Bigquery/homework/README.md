# bigquery commands

# Q1. What is count of records for the 2022 Green Taxi Data?
``` sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `sharp-harbor-411301.au_ny_taxi.external_green_tripdata_dlt`
OPTIONS (
  format = 'parquet',
  uris = ['gs://sharp-harbor-411301-bucket/ny_taxi/green_taxi/*.parquet']
);

SELECT count(*) as data_rows  FROM `sharp-harbor-411301.au_ny_taxi.external_green_tripdata_dlt`

data_rows
840402
```

# Q2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
# What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
``` sql
CREATE OR REPLACE TABLE `sharp-harbor-411301.au_ny_taxi.green_tripdata_non_partitioned`
AS SELECT * FROM `sharp-harbor-411301.au_ny_taxi.external_green_tripdata_dlt`

SELECT count(distinct pu_location_id) FROM `sharp-harbor-411301.au_ny_taxi.green_tripdata_non_partitioned`
```

# Q3. How many records have a fare_amount of 0?
``` sql
SELECT count(*) as data_rows FROM `sharp-harbor-411301.au_ny_taxi.green_tripdata_non_partitioned`
where fare_amount = 0

data_rows
1622
```

# Q4. What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
``` sql
CREATE OR REPLACE TABLE `sharp-harbor-411301.au_ny_taxi.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY pu_location_id AS
SELECT * FROM `sharp-harbor-411301.au_ny_taxi.external_green_tripdata_dlt`
```

# Q5. Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

# Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

``` sql
SELECT distinct pu_location_id FROM `sharp-harbor-411301.au_ny_taxi.green_tripdata_non_partitioned` 
where date(lpep_pickup_datetime) between "2022-06-01" and "2022-06-30"

SELECT distinct pu_location_id FROM `sharp-harbor-411301.au_ny_taxi.green_tripdata_partitioned_clustered` 
where date(lpep_pickup_datetime) between "2022-06-01" and "2022-06-30"
```