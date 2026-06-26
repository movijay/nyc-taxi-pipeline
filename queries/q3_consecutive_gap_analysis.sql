/*
  Q3 — Consecutive trip gap analysis
  ------------------------------------
  For each day in 2023, find the maximum gap (minutes) between the end of one trip
  and the start of the next trip originating from the same pickup zone.

  Method:
    1. For each trip, use LAG() to get the previous trip's dropoff time within
       the same (date, zone) partition, ordered by pickup_datetime.
    2. Compute gap = current pickup - previous dropoff.
    3. Aggregate to max gap per (date, zone).

  ---------------------------------------------------------------------------------
  BRAINSTORMER — Performance on 38M rows (Snowflake-specific):

  PROBLEM: LAG() over PARTITION BY (pickup_date, pickup_location_id) ORDER BY
  pickup_datetime requires Snowflake to sort within each partition. With 265 zones
  × 365 days = ~97k partitions, each holding ~390 rows on average, the sort is
  manageable — but the full 38M row scan is the bottleneck.

  STRATEGIES:

  1. CLUSTERING KEY on fct_trips: CLUSTER BY (pickup_date, pickup_location_id)
     Snowflake will co-locate all rows for the same (date, zone) in the same
     micro-partitions. The window function then only sorts within each micro-partition
     group rather than globally — dramatically reduces shuffle and sort cost.
     ALTER TABLE fct_trips CLUSTER BY (pickup_date, pickup_location_id);

  2. SEARCH OPTIMISATION SERVICE: Enables fast point lookups and equality filters.
     Less relevant here since we're scanning all dates, but valuable if analysts
     frequently query a single zone.

  3. RESULT CACHING: This is a deterministic, non-volatile query over a static
     2023 dataset. After first run, Snowflake caches the full result set for 24h.
     Repeated runs (dashboards, re-runs) are free and instant.

  4. MATERIALISE AS A TABLE: Run this as a scheduled dbt model rather than an
     ad-hoc query. Pre-compute and store — downstream users never hit the raw 38M rows.

  5. PARTITION PRUNING: If a user adds a date filter (WHERE pickup_date = '2023-06-01'),
     the cluster key on pickup_date lets Snowflake skip ~364/365 micro-partitions.
  ---------------------------------------------------------------------------------
*/

with trips_with_lag as (
    select
        pickup_date,
        pickup_location_id,
        pickup_zone,
        pickup_datetime,
        dropoff_datetime,

        -- Previous trip's dropoff time within the same (date, zone)
        lag(dropoff_datetime) over (
            partition by pickup_date, pickup_location_id
            order by pickup_datetime
        ) as prev_dropoff_datetime

    from marts.fct_trips
),

gaps as (
    select
        pickup_date,
        pickup_location_id,
        pickup_zone,

        -- Gap in minutes between previous trip's dropoff and this trip's pickup
        datediff(
            'minute',
            prev_dropoff_datetime,
            pickup_datetime
        ) as gap_minutes

    from trips_with_lag
    where prev_dropoff_datetime is not null      -- skip first trip of each group
      and pickup_datetime > prev_dropoff_datetime -- guard against overlapping trips
)

select
    pickup_date,
    pickup_location_id,
    pickup_zone,
    max(gap_minutes) as max_gap_minutes
from gaps
group by pickup_date, pickup_location_id, pickup_zone
order by pickup_date, max_gap_minutes desc
