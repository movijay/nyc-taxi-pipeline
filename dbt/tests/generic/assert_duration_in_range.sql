/*
  assert_duration_in_range  (generic / parametrised test)
  ---------------------------------------------------------
  Usage in schema.yml:
    - assert_duration_in_range:
        min_minutes: 1
        max_minutes: 180

  Returns rows that fall outside the configured [min, max] range.
  Test passes when 0 rows returned.
*/

{% test assert_duration_in_range(model, column_name, min_minutes, max_minutes) %}

select
    {{ column_name }},
    '{{ min_minutes }}' as configured_min,
    '{{ max_minutes }}' as configured_max
from {{ model }}
where
    {{ column_name }} < {{ min_minutes }}
    or {{ column_name }} > {{ max_minutes }}

{% endtest %}
