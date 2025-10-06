{{ config(materialized='table') }}

with yellow as (
    select
        vendorid           as vendor_id,
        tpep_pickup_datetime  as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid         as rate_code_id,
        store_and_fwd_flag,
        pulocationid       as pickup_location_id,
        dolocationid       as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
    from {{ source('bronze','yellow_trips') }}
),
green as (
    select
        vendorid           as vendor_id,
        lpep_pickup_datetime  as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid         as rate_code_id,
        store_and_fwd_flag,
        pulocationid       as pickup_location_id,
        dolocationid       as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        null as airport_fee
    from {{ source('bronze','green_trips') }}
)

select * from yellow
union all
select * from green
