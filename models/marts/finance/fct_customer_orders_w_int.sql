{{ 
    config(
        materialized='view'
)}}

with
    orders as (
        select *
        from {{ ref('stg_jaffle_shop__orders') }}
    )

    , customers as (
        select *
        from {{ ref('stg_jaffle_shop__customers') }}
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

    , customer_orders as (
        select 
            customers.customer_id
            , customers.customer_first_name
            , customers.customer_last_name
            , min(paid_orders.order_date) as first_order_date
            , max(paid_orders.order_date) as most_recent_order_date
            , count(paid_orders.order_id) as number_of_orders
        from customers
        left join paid_orders on
            paid_orders.customer_id = customers.customer_id
        group by customer_id, customer_first_name, customer_last_name
    )

    , final as (
        select 
            paid_orders.order_id
            , paid_orders.customer_id
            , paid_orders.order_date
            , paid_orders.order_status
            , paid_orders.total_amount_paid
            , paid_orders.payment_finalized_date
            , customer_orders.customer_first_name
            , customer_orders.customer_last_name
            -- , row_number() over(
            --     order by paid_orders.order_id
            -- ) as transaction_seq
            , row_number() over(
                partition by paid_orders.customer_id
                order by paid_orders.order_id
            ) as customer_sales_seq
            , sum(paid_orders.total_amount_paid) over(
                partition by paid_orders.customer_id
                order by paid_orders.order_id
            ) as customer_lifetime_value
            , customer_orders.first_order_date as fdos
            , case when customer_orders.first_order_date = paid_orders.order_date
                then 'new'
                else 'return'
            end as new_vs_return
        from paid_orders
        left join customer_orders on
            paid_orders.customer_id = customer_orders.customer_id
        order by paid_orders.order_id
    )

select * from final
