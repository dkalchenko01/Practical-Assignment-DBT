{% macro calculate_age(birthday_column) %}
    date_part('year', age(current_date, cast({{ birthday_column }} as date)))
{% endmacro %}