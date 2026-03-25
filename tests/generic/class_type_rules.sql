{% test class_type_rules(model, column_name, condition, should_be_null) %}
    select *
    from {{ model }}
    where {{ condition }} and
          ({% if should_be_null %}
              {{ column_name }} is not null
           {% else %}
              {{ column_name }} is null
           {% endif %})
{% endtest %}
