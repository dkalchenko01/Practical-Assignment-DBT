{% test accepted_values_from_table(model, column_name, other_model, other_column, filter_key, filter_value) %}
  select
    {{ column_name }}
  from {{ model }}
  where {{ column_name }} is not null and
      {{ column_name }} not in (select {{ other_column }} from {{ ref(other_model) }}
                                                          where {{ filter_key }} = '{{ filter_value }}')
{% endtest %}