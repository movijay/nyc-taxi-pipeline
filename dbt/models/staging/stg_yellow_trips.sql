/*
  stg_yellow_trips
  ----------------
  Renames columns to snake_case, casts types, adds trip_duration_minutes.
  Deduplicates on trip_id (MD5 surrogate key) to eliminate exact duplicate
  source rows before downstream models consume the data.
  Does NOT filter invalid rows here — filtering happens in int_trips_enriched
  so we have full visibility of raw data quality.
*/

with source as (
    select * from {{ source('nyc_tlc', 'yellow_trips') }}
),

renamed as (
    select
        -- surrogate key: combination of pickup time + locations + fare (no natural PK in raw data)
        md5(
            coalesce(cast(tpep_pickup_datetime as varchar), '') ||
            coalesce(cast(tpep_dropoff_datetime as varchar), '') ||
            coalesce(cast(PULocationID as varchar), '') ||
            coalesce(cast(DOLocationID as varchar), '') ||
            coalesce(cast(fare_amount as varchar), '') ||
            coalesce(cast(tip_amount as varchar), '') ||
            coalesce(cast(total_amount as varchar), '') ||
            coalesce(cast(passenger_count as varchar), '') ||
            coalesce(cast(trip_distance as varchar), '')
        ) as trip_id,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- dimensions
        cast(passenger_count as integer)  as passenger_count,
        cast(PULocationID as integer)     as pickup_location_id,
        cast(DOLocationID as integer)     as dropoff_location_id,
        cast(payment_type as integer)     as payment_type,

        -- measures
        cast(trip_distance as double)     as trip_distance_miles,
        cast(fare_amount as double)       as fare_amount,
        cast(tip_amount as double)        as tip_amount,
        cast(total_amount as double)      as total_amount,

        -- derived: duration in fractional minutes
        datediff(
            'minute',
            cast(tpep_pickup_datetime as timestamp),
            cast(tpep_dropoff_datetime as timestamp)
        ) as trip_duration_minutes

    from source
),

deduplicated as (
    select *
    from renamed
    qualify row_number() over (
        partition by trip_id
        order by pickup_datetime desc
    ) = 1
)

select * from deduplicated
