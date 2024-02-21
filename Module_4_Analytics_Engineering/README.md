# bigquery commands

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `sharp-harbor-411301.mod4.green_taxi_trips` 
OPTIONS (
format = 'parquet',
uris = ['gs://sharp-harbor-411301-bucket/ny_taxi_mod4/green_taxi/*.parquet']);

CREATE EXTERNAL TABLE IF NOT EXISTS `sharp-harbor-411301.mod4.yellow_taxi_trips` 
OPTIONS (
format = 'parquet',
uris = ['gs://sharp-harbor-411301-bucket/ny_taxi_mod4/yellow_taxi/*.parquet']);

CREATE EXTERNAL TABLE IF NOT EXISTS `sharp-harbor-411301.mod4.fhv` 
OPTIONS (
format = 'parquet',
uris = ['gs://sharp-harbor-411301-bucket/ny_taxi_mod4/fhv/*.parquet']);
```