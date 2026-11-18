{#- One row per value of `columns`: the generated event-transaction views are tested with it. The grouping spills to
    disk past 1 GB, since the table grows with every swap. -#}
{% test unique_grain(model, columns) %}
select {{ columns | join(', ') }}, count() as n
from {{ model }} group by {{ columns | join(', ') }} having n > 1
settings max_bytes_before_external_group_by = 1000000000, max_bytes_ratio_before_external_group_by = 0.5
{% endtest %}
