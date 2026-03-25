{% macro get_attendance_category(attendance_rate) %}
case
    when {{ attendance_rate }} >= 0.90 then 'Gold'
    when {{ attendance_rate }} >= 0.75 then 'Silver'
    when {{ attendance_rate }} >= 0.5 then 'Bronze'
    else 'Low Attendance'
end
{% endmacro %}