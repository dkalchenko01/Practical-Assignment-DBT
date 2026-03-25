with source as (

    select * from {{ ref('raw_payments') }}

),
transformed as (
    select
        cast(payment_id as integer) as payment_id,
        trim(dancer_full_name) as dancer_full_name,
        cast(amount as decimal(10, 2)) as amount,
        cast(payment_date as date) as payment_date,
        trim(payment_type) as payment_type,
        trim(target_class) as target_class
    from source
)
select * from transformed