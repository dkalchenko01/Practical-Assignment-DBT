{{
  config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy= 'delete+insert'
  )
}}

with

{% if is_incremental() %}
     max_date as (
        select max(payment_date) as max_payment_date from {{ this }}
        ),
{% endif %}

payment_source as (
    select
        payment_id,
        dancer_full_name,
        amount,
        payment_date,
        payment_type,
        target_class
    from {{ ref('stg_payments') }}
    {% if is_incremental() %}
        cross join max_date
        where payment_date > max_payment_date
    {% endif %}
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['dancer_full_name', 'payment_date', 'amount']) }} as payment_id,
        d.dancer_id,
        c.class_id,
        c.coach_id,
        dt.date_id as payment_date_id,
        p.payment_date,
        coalesce(s.subscription_id, 'invalid payment') as subscription_id,
        amount,
        payment_type

    from payment_source p
    left join {{ ref('dim_dancers') }} d
    on p.dancer_full_name = d.full_name
    left join {{ ref('dim_dates') }} dt
    on p.payment_date = dt.date_full
    left join {{ ref('dim_classes') }} c
    on p.target_class = c.class_name
    left join {{ ref('dim_subscription') }} s
    on p.amount = s.price_standard

)

select * from transformed