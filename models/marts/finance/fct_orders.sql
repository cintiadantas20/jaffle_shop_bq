with
    orders as (
        select 
            order_id
            , customer_id
            , order_date
        from {{ ref('stg_jaffle_shop__orders') }}
    )

    , payments as (
        select 
            order_id
            , sum(payment_amount) as payment_amount
        from {{ ref('stg_stripe__payments') }}
        group by order_id
    )

    , final as (
        select 
            orders.order_id
            , orders.customer_id
            , orders.order_date
            , coalesce(payments.payment_amount, 0) as payment_amount
        from orders
        left join payments on
            orders.order_id = payments.order_id
    )

select *
from final
