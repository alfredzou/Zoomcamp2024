{{ config(materialized='view') }}
 
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,    
    {{ dbt.safe_cast("p_ulocation_id", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("d_olocation_id", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    dispatching_base_num,
    affiliated_base_number,

from {{ source('staging','fhv_taxi_trips') }}
where extract(YEAR from pickup_datetime) = 2019


-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}