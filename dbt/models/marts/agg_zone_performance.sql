/*
  agg_zone_performance
  --------------------
  Per pickup zone, per month: trip volume, avg distance, avg fare, revenue rank, high-volume flag.

  BRAINSTORMER — Monthly vs. overall ranking:
  -------------------------------------------
  RANK() over the entire year would give every zone a single static position.
  That tells you which zones are dominant in aggregate but hides seasonal patterns
  entirely: JFK may rank #1 in summer (tourists) but drop in winter, while Grand
  Central is steady year-round. For operational decisions — driver incentives,
  pricing adjustments, infrastructure planning — a business needs to know which
  zones are performing *right now relative to peers this month*, not just their
  all-time standing. Monthly ranking also makes it easy to spot fast-risers and
  declining zones, which an annual rank would mask.

  Decision: RANK() partitioned by (year, month), ordered by monthly revenue DESC.
*/

with trips as (
    select * from {{ ref('fct_trips') }}
),

zone_monthly as (
    select
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        pickup_year                             as trip_year,
        pickup_month                            as trip_month,

        count(*)                                as total_trips,
        round(avg(trip_distance_miles), 2)      as avg_trip_distance_miles,
        round(avg(fare_amount), 2)              as avg_fare,
        round(sum(fare_amount), 2)              as total_revenue

    from trips
    -- Filter to 2023 only: source data contains rows with corrupt timestamps
    -- (years 2001-2009, 2022) which are GPS/meter recording errors in the raw
    -- TLC data. Including them would pollute monthly zone rankings with
    -- near-zero revenue months that never actually existed.
    where pickup_date >= '2023-01-01'
      and pickup_date <= '2023-12-31'
    group by
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        pickup_year,
        pickup_month
),

ranked as (
    select
        *,
        -- Monthly revenue rank within each (year, month) window
        rank() over (
            partition by trip_year, trip_month
            order by total_revenue desc
        ) as revenue_rank,

        -- Flag zones that exceeded 10k trips in this month
        case when total_trips > 10000 then true else false end as is_high_volume
    from zone_monthly
)

select * from ranked
order by trip_year, trip_month, revenue_rank
