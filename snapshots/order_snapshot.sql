{% snapshot order_snapshot %}

{{
    config(
      target_database=target.project,
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['id', 'user_id', 'order_date', 'status'],
      hard_deletes='ignore'
    )
}}

select *
from {{ source('jaffle_shop_sources', 'orders') }}

{% endsnapshot %}