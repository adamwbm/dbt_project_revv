with 

source as (
    select 
        cast(REGEXP_REPLACE(payment_id,'pay_','') as int) as payment_id,
        cast(REGEXP_REPLACE(subscription_id,'sub_','') as int) as subscription_id,
        cast(REGEXP_REPLACE(customer_id,'cus_','') as int) as customer_id,
        date as payment_date,
        amount/100.00 as amount
    from {{ ref('stripe_payment') }}
)

select * from source