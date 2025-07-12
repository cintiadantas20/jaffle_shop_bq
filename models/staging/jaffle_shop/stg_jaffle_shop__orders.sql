with 
    orders as (
        select 
            cast(id as string) as order_id
            , cast(user_id as string) as customer_id
            , cast(order_date as date) as order_date
            , cast(status as string) as order_status
            , cast(_etl_loaded_at as date) as loaded_at
        from {{ source('jaffle_shop_sources', 'orders') }}
    )

    , new_orders as (
        select 
            cast(order_id as string) as order_id
            , cast(customer_id as string) as customer_id
            , cast(order_date as date) as order_date
            , cast(order_status as string) as order_status
            , cast(loaded_at as date) as loaded_at
        from {{ ref('new_orders') }}
    )

    , all_orders as (
        select *
        from orders
        union all
        select *
        from new_orders
    )

select *
from all_orders

{{ limit_data_in_dev(column_name = 'order_date', dev_days_of_data = '5000') }}
