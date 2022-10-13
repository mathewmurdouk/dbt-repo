with source as (
    select * from {{ source('jaffle_shop','orders') }}
),

orders as (
    select
    ID as order_id,
    USER_ID	as customer_id,
    ORDER_DATE AS order_placed_at,
    STATUS AS order_status
    from source
)

select * from orders