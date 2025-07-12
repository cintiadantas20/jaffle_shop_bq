with
    payments as (
        select 
            cast(id as string) as payment_id
            , cast(orderid as string) as order_id
            , cast(paymentmethod as string) as payment_method
            , cast(status as string) as payment_status
            , cast({{ cents_to_dolars('amount') }} as float64) as payment_amount
            , cast(created as date) as created_at
            , cast(_batched_at as date) as loaded_at
        from {{ source('stripe_source', 'payment') }}
    )

    , new_payments as (
        select 
            cast(payment_id as string) as payment_id
            , cast(order_id as string) as order_id
            , cast(payment_method as string) as payment_method
            , cast(payment_status as string) as payment_status
            , cast({{ cents_to_dolars('payment_amount') }} as float64) as payment_amount
            , cast(created_at as date) as created_at
            , cast(loaded_at as date) as loaded_at
        from {{ ref('new_payments') }}
    )

    , all_payments as (
        select *
        from payments
        union all
        select *
        from new_payments
    )

select *
from all_payments