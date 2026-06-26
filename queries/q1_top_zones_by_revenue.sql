/*
  Q1 — Top 10 pickup zones by revenue in each month of 2023
  ----------------------------------------------------------
  Uses RANK() window function partitioned by month.

  Performance strategy (Snowflake X-Small):
    - The mart table agg_zone_performance is already pre-aggregated by (zone, year, month)
      so this query scans ~265 zones × 12 months = ~3,180 rows — effectively instant.
    - If querying fct_trips directly, cluster key on (pickup_year, pickup_month,
      pickup_location_id) would allow micro-partition pruning to only the target month,
      reducing scan from 38M rows to ~3M per month.
    - On Snowflake, result caching means repeated runs in the same session are free.
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
    from {{ ref('agg_zone_performance') }}
    -- If running raw SQL against the mart schema, replace with:
    -- from marts.agg_zone_performance
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
order by trip_year, trip_month, revenue_rank
