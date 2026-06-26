/*
  fct_trips
  ---------
  Core fact table: one row per valid, enriched taxi trip.
  Materialized as a table for query performance.
*/

select * from {{ ref('int_trips_enriched') }}
