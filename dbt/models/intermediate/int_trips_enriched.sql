/*
  int_trips_enriched
  ------------------
  Joins staged trips with zone names (pickup + dropoff) and filters out invalid records.

  Filtering rules (business logic, not data quality):
    - trip_distance_miles <= 0 : no movement recorded
    - fare_amount <= 0         : free or disputed rides skew revenue metrics
    - passenger_count = 0      : automated/empty trips
    - trip_duration_minutes < 1: sub-minute trips are almost certainly GPS errors
    - trip_duration_minutes > 180: trips > 3 hours are extreme outliers (~99.9th pct)
*/

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

zones as (
    select * from {{ ref('stg_taxi_zones') }}
),

enriched as (
    select
        t.trip_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.trip_duration_minutes,

        -- passenger & distance
        t.passenger_count,
        t.trip_distance_miles,

        -- location IDs
        t.pickup_location_id,
        t.dropoff_location_id,

        -- pickup zone details
        pu.zone_name        as pickup_zone,
        pu.borough          as pickup_borough,
        pu.service_zone     as pickup_service_zone,

        -- dropoff zone details
        do_.zone_name       as dropoff_zone,
        do_.borough         as dropoff_borough,

        -- financials
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        t.payment_type,

        -- derived
        case t.payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            else 'Unknown'
        end as payment_type_desc,

        -- convenience date parts for partitioning and aggregation
        date_trunc('day', t.pickup_datetime)   as pickup_date,
        date_part('year', t.pickup_datetime)   as pickup_year,
        date_part('month', t.pickup_datetime)  as pickup_month,
        date_part('hour', t.pickup_datetime)   as pickup_hour

    from trips t
    left join zones pu  on t.pickup_location_id  = pu.location_id
    left join zones do_ on t.dropoff_location_id = do_.location_id

    where
        t.trip_distance_miles > 0
        and t.fare_amount > 0
        and t.passenger_count > 0
        and t.trip_duration_minutes >= {{ var('trip_duration_min_minutes') }}
        and t.trip_duration_minutes <= {{ var('trip_duration_max_minutes') }}
)

select * from enriched
