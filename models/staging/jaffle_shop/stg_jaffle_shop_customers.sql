with source as (
    select * from {{ source('jaffle_shop', 'customers')}}
),

customers as(
    select ID as customer_id,
    FIRST_NAME as customer_first_name,
    LAST_NAME as customer_last_name
    from source
)

select * from customers