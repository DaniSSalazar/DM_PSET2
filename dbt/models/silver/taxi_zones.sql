{{ config(materialized='table') }}

select
    locationid as location_id,
    borough,
    zone,
    service_zone
from {{ source('bronze','taxi_zones') }}
