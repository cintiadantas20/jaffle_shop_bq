with
    customers as (
        select 
            cast(id as string) as customer_id
            , cast(first_name as string) as customer_first_name
            , cast(last_name as string) as customer_last_name
        from {{ source('jaffle_shop_sources', 'customers') }}
    )

    , new_customers as (
        select 
            cast(customer_id as string) as customer_id
            , cast(customer_first_name as string) as customer_first_name
            , cast(customer_last_name as string) as customer_last_name
        from {{ ref('new_customers') }}
    )

    , all_customers as (
        select * 
        from customers
        union all
        select *
        from new_customers
    )

select *
from all_customers 