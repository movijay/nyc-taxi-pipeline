/*
  stg_taxi_zones
  --------------
  Loads the taxi zone lookup CSV (loaded as a dbt seed) and standardises column names.
*/

with source as (
    select * from {{ source('nyc_tlc', 'taxi_zone_lookup') }}
),

renamed as (
    select
        cast(LocationID as integer) as location_id,
        Borough                     as borough,
        Zone                        as zone_name,
        service_zone                as service_zone
    from source
)

select * from renamed
