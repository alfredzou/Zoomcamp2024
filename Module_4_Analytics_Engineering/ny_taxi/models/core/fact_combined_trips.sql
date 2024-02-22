{{
    config(
        materialized='table'
    )
}}

with green_yellow_tripdata as (
    select 
        trip_id,
        service_type,
        pickup_locationid,
        pickup_borough,
        pickup_zone,
        dropoff_locationid,
        dropoff_borough,
        dropoff_zone,
        pickup_datetime,
        dropoff_datetime
    from {{ ref('fact_trips') }}
),
fhv_tripdata as (
    select 
        trip_id,
        service_type,
        pickup_locationid,
        pickup_borough,
        pickup_zone,
        dropoff_locationid,
        dropoff_borough,
        dropoff_zone,
        pickup_datetime,
        dropoff_datetime
    from {{ ref('fact_fhv_trips') }}
),
trips_unioned as (
    select * from green_yellow_tripdata
    union all
    select * from fhv_tripdata
)

select * from trips_unioned 
