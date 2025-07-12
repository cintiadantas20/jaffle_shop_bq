with
    orders as (
        select *
        from {{ ref('int_orders') }}
    )

    , customers as (
        select *
        from {{ ref('stg_jaffle_shop__customers') }}
    )

    , customer_orders as (
        select 
            customers.customer_id
            , customers.customer_first_name
            , customers.customer_last_name
            , min(orders.order_date) as first_order_date
            , max(orders.order_date) as most_recent_order_date
            , count(orders.order_id) as number_of_orders
        from customers
        left join orders on
            orders.customer_id = customers.customer_id
        group by customer_id, customer_first_name, customer_last_name
    )

    , final as (
        select 
            orders.order_id
            , orders.customer_id
            , orders.order_date
            , orders.order_status
            , orders.total_amount_paid
            , orders.payment_finalized_date
            , customer_orders.customer_first_name
            , customer_orders.customer_last_name
            -- , row_number() over(
            --     order by orders.order_id
            -- ) as transaction_seq
            , row_number() over(
                partition by orders.customer_id
                order by orders.order_id
            ) as customer_sales_seq
            , sum(orders.total_amount_paid) over(
                partition by orders.customer_id
                order by orders.order_id
            ) as customer_lifetime_value
            , customer_orders.first_order_date as fdos
            , case when customer_orders.first_order_date = orders.order_date
                then 'new'
                else 'return'
            end as new_vs_return
        from orders
        left join customer_orders on
            orders.customer_id = customer_orders.customer_id
        order by orders.order_id
    )

select * from final
