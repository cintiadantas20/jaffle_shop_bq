with
    orders as (
        select *
        from {{ ref('stg_jaffle_shop__orders') }}
    )

    , payments as (
        select *
        from {{ ref("stg_stripe__payments") }}
    )

    , grouped_payments as (
        select 
            order_id
            , max(created_at) as payment_finalized_date
            , sum(payment_amount) as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by order_id
    )

    , paid_orders as (
        select 
            orders.order_id
            , orders.customer_id
            , orders.order_date
            , orders.order_status
            , grouped_payments.total_amount_paid
            , grouped_payments.payment_finalized_date
        from orders
        left join grouped_payments on
            orders.order_id = grouped_payments.order_id
    )

select *
from paid_orders