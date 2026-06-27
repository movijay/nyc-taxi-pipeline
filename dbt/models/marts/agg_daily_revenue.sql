/*
  agg_daily_revenue
  -----------------
  Daily revenue summary: trips, fares, tips, tip rate.
*/

with trips as (
    select * from {{ ref('fct_trips') }}
)

select
    pickup_date                                             as trip_date,
    count(*)                                               as total_trips,
    round(sum(fare_amount), 2)                             as total_fare,
    round(avg(fare_amount), 2)                             as avg_fare,
    round(sum(tip_amount), 2)                              as total_tips,
    round(
        100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0),
        2
    )                                                      as tip_rate_pct
from trips
-- Filter to 2023 only: source data contains ~9 rows with corrupt timestamps
-- (e.g. year 2001) which are clearly GPS/meter recording errors in the raw TLC data.
where pickup_date >= '2023-01-01'
  and pickup_date <= '2023-12-31'
group by pickup_date
order by trip_date