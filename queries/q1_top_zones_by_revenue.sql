/*
  Q1 — Top 10 pickup zones by revenue in each month of 2023
  ----------------------------------------------------------
  Uses RANK() window function partitioned by month.

  Performance strategy (Snowflake X-Small):
    - agg_zone_performance is pre-aggregated by (zone, year, month)
      so this query scans ~3,180 rows — effectively instant.
    - If querying fct_trips directly, cluster key on (pickup_year, pickup_month,
      pickup_location_id) allows micro-partition pruning to ~3M rows per month.
    - Snowflake result caching means repeated runs are free.

  Table reference:
    Snowflake (preferred) : RAW_MARTS.agg_zone_performance
    DuckDB (local dev)    : main_marts.agg_zone_performance
*/

with monthly_revenue as (
    select
        trip_year,
        trip_month,
        pickup_zone,
        pickup_borough,
        total_revenue,
        rank() over (
            partition by trip_year, trip_month
            order by total_revenue desc
        ) as revenue_rank
    from NYC_TAXI.RAW_MARTS.agg_zone_performance   -- Snowflake
    -- from main_marts.agg_zone_performance         -- DuckDB
)

select
    trip_year,
    trip_month,
    revenue_rank,
    pickup_zone,
    pickup_borough,
    total_revenue
from monthly_revenue
where revenue_rank <= 10
order by trip_year, trip_month, revenue_rank;