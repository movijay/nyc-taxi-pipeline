/*
  dim_zones
  ---------
  Dimension table for NYC taxi zones.
*/

select
    location_id,
    zone_name,
    borough,
    service_zone
from {{ ref('stg_taxi_zones') }}
