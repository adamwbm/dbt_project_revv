with

customers as (
    select 
        customer_id,
        customer_name
    from {{ ref('stg_stripe__customer') }}
)

select * from customers 