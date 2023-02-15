{{ config(materialized='table') }}

with fhv_2019 as (
    select *, 
        'fhv' as service_type 
    from {{ ref('stg_fhv_2019_data') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
--
select 
    fhv_2019.dispatching_base_num,
    fhv_2019.service_type,
    fhv_2019.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_2019.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_2019.pickup_datetime, 
    fhv_2019.dropoff_datetime, 
    fhv_2019.SR_Flag
from fhv_2019
inner join dim_zones as pickup_zone
on fhv_2019.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_2019.dropoff_locationid = dropoff_zone.locationid