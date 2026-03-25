with rooms_source as (
    select setting_key,
           setting_value
    from {{ ref('manual_settings') }}
),

valid_rooms as (
    select
        {{ dbt_utils.generate_surrogate_key(['setting_value']) }} as room_id,
        setting_value as room_name
    from rooms_source
    where setting_key = 'valid_room'
)

select * from valid_rooms
