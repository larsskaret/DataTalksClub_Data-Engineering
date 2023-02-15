{{ 
    config(materialized='view') 
}}

select 

    dispatching_base_num,

    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    SR_Flag

from {{ source('staging', 'fhv_2019') }}