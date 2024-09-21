with 

source as (
    select 
        REGEXP_REPLACE(customer_id,'cus_','') as customer_id,
        customer_name
    from {{ ref('stripe_customer') }}
)

select * from source