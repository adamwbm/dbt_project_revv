with 

source as (
    select 
        cast(REGEXP_REPLACE(customer_id,'cus_','') as int) as customer_id,
        customer_name
    from {{ ref('stripe_customer') }}
)

select * from source