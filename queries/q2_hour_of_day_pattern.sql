/*
  Q2 — Hour-of-day demand pattern (full year 2023)
  -------------------------------------------------
  For each hour (0–23): total trips, avg fare, avg tip%, 3-hour rolling avg of trip count.
*/

with hourly as (
    select
        pickup_hour,
        count(*)                                                        as total_trips,
        round(avg(fare_amount), 2)                                      as avg_fare,
        round(
            100.0 * avg(
                case when fare_amount > 0 then tip_amount / fare_amount else null end
            ),
            2
        )                                                               as avg_tip_pct
    from marts.fct_trips
    group by pickup_hour
),

rolling as (
    select
        pickup_hour,
        total_trips,
        avg_fare,
        avg_tip_pct,
        -- 3-hour rolling average: current hour + 1 preceding + 1 following
        round(
            avg(total_trips) over (
                order by pickup_hour
                rows between 1 preceding and 1 following
            ),
            0
        ) as rolling_3hr_avg_trips
    from hourly
)

select *
from rolling
order by pickup_hour
