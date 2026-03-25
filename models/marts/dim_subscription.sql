with subscription_source as (
    select
        subscription_plan,
        plan_name,
        classes_included,
        price_standard,
        validity_days
    from {{ ref('manual_subscriptions') }}
),
transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['subscription_plan']) }} as subscription_id,
        plan_name,
        classes_included,
        price_standard,
        validity_days
    from subscription_source
)
select * from transformed