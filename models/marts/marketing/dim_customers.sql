with
    customers as (
        select *
        from {{ ref('stg_jaffle_shop__customers') }}
    )

    , orders as (
        select *
        from {{ ref('stg_jaffle_shop__orders') }}
    )

    , payments as (
        select 
            order_id
            , sum(payment_amount) as payment_amount
        from {{ ref('stg_stripe__payments') }}
        group by order_id
    )
    -- Essa lógica também está na fct_customer_orders... Ver se é possível fazer outra int
    , customer_orders as (
        select
            customer_id
            , min(order_date) as first_order_date
            , max(order_date) as most_recent_order_date
            , count(order_id) as number_of_orders
        from orders
        group by customer_id
    )

    , orders_payments as (
        select
            orders.customer_id
            , sum(payments.payment_amount) as payment_amount
        from orders
        left join payments on
            orders.order_id = payments.order_id
        group by customer_id
    )

    , final as (
        select 
            customers.customer_id
            , customers.customer_first_name
            , customers.customer_last_name
            , customer_orders.first_order_date
            , customer_orders.most_recent_order_date
            , coalesce(customer_orders.number_of_orders, 0) as number_of_orders
            , orders_payments.payment_amount
        from customers
        left join customer_orders on
            customers.customer_id = customer_orders.customer_id    
        left join orders_payments on
            customers.customer_id = orders_payments.customer_id
    )

select *
from final
