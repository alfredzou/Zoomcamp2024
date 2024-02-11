-- creating bigquery external table from gcs
CREATE OR REPLACE EXTERNAL TABLE `sharp-harbor-411301.au_ny_taxi.external_yellow_tripdata_dlt`
OPTIONS (
  format = 'parquet',
  uris = ['gs://sharp-harbor-411301-bucket/ny_taxi/yellow_taxi/*.parquet']
);

-- Creating non-partitioned table based on external table
CREATE OR REPLACE TABLE sharp-harbor-411301.au_ny_taxi.yellow_trip_data_non_partitioned AS
SELECT * FROM sharp-harbor-411301.au_ny_taxi.external_yellow_tripdata_dlt

-- Above but partitioned on pickup datetime
CREATE OR REPLACE TABLE sharp-harbor-411301.au_ny_taxi.yellow_trip_data_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM sharp-harbor-411301.au_ny_taxi.external_yellow_tripdata_dlt

-- Above but also clustered on vendor ID
CREATE OR REPLACE TABLE sharp-harbor-411301.au_ny_taxi.yellow_trip_data_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime) 
CLUSTER BY vendor_id AS
SELECT * FROM sharp-harbor-411301.au_ny_taxi.external_yellow_tripdata_dlt

-- billed data 1.6GB
SELECT distinct(vendor_id) FROM `sharp-harbor-411301.au_ny_taxi.yellow_trip_data_non_partitioned` 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- billed data 106 MB
SELECT distinct(vendor_id) FROM `sharp-harbor-411301.au_ny_taxi.yellow_trip_data_partitioned` 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- billed data 1.1 GB
SELECT count(*) as trips
FROM `sharp-harbor-411301.au_ny_taxi.yellow_trip_data_partitioned` 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND vendor_id=1

-- billed data 863 MB
SELECT count(*) as trips
FROM `sharp-harbor-411301.au_ny_taxi.yellow_trip_data_partitioned_clustered` 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND vendor_id=1
