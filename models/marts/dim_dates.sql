{{
  config(
    materialized='incremental',
    unique_key='date_id'
  )
}}


with generated_dates as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2024-01-01' as date)",
    end_date="cast('2027-01-01' as date)"
  ) }}
),

date_details as (
    select
        cast(strftime(date_day, '%Y%m%d') as integer) as date_id,
        date_day as date_full,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        monthname(date_day) as month_name,
        extract(day from date_day) as day_of_month,
        dayname(date_day) as day_of_week,
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
    from generated_dates
)

select * from date_details

