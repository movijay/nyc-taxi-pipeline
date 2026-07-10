/*
  Q3 — Consecutive trip gap analysis
  ------------------------------------
  For each day in 2023, find the maximum gap (minutes) between the end of one
  trip and the start of the next trip from the same pickup zone.

  Method:
    1. LAG() to get previous trip's dropoff time within same (date, zone)
    2. Compute gap = current pickup - previous dropoff
    3. Aggregate to max gap per (date, zone)

  Table reference:
    Snowflake (preferred) : NYC_TAXI.RAW_MARTS.fct_trips
    DuckDB (local dev)    : main_marts.fct_trips

  ---------------------------------------------------------------------------------
  PERFORMANCE NOTE (Snowflake-specific):

  1. CLUSTERING KEY: CLUSTER BY (pickup_date, pickup_location_id)
     Co-locates rows for same (date, zone) in same micro-partitions.
     Reduces shuffle cost for the window function dramatically.

  2. RESULT CACHING: Static 2023 dataset — after first run Snowflake caches
     results for 24h. Repeated dashboard/analyst runs are free.

  3. MATERIALISE AS TABLE: Run as a scheduled dbt model so downstream users
     never hit the raw 35M row fact table directly.

  4. PARTITION PRUNING: Adding WHERE pickup_date = '2023-06-01' lets Snowflake
     skip ~364/365 micro-partitions when querying a single day.
  ---------------------------------------------------------------------------------
*/

with trips_with_lag as (
    select
        pickup_date,
        pickup_location_id,
        pickup_zone,
        pickup_datetime,
        dropoff_datetime,
        lag(dropoff_datetime) over (
            partition by pickup_date, pickup_location_id
            order by pickup_datetime
        ) as prev_dropoff_datetime
    from NYC_TAXI.RAW_MARTS.fct_trips          -- Snowflake
    -- from main_marts.fct_trips               -- DuckDB
    where pickup_date >= '2023-01-01'
      and pickup_date <= '2023-12-31'
),

gaps as (
    select
        pickup_date,
        pickup_location_id,
        pickup_zone,
        datediff(
            'minute',
            prev_dropoff_datetime,
            pickup_datetime
        ) as gap_minutes
    from trips_with_lag
    where prev_dropoff_datetime is not null
      and pickup_datetime > prev_dropoff_datetime
)

select
    pickup_date,
    pickup_location_id,
    pickup_zone,
    max(gap_minutes) as max_gap_minutes
from gaps
group by pickup_date, pickup_location_id, pickup_zone
order by pickup_date, max_gap_minutes desc;