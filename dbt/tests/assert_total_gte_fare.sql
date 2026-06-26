/*
  assert_total_gte_fare
  ---------------------
  Singular test: no trip in fct_trips should have total_amount < fare_amount.
  Tips, surcharges, and tolls can only add to the base fare, never subtract.
  Returns rows that violate the assertion — test passes when 0 rows returned.
*/

select
    trip_id,
    fare_amount,
    total_amount,
    total_amount - fare_amount as delta
from {{ ref('fct_trips') }}
where total_amount < fare_amount
