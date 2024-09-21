with 

payments as(
    select 
        payment_id,
        subscription_id,
        customer_id,
        payment_date,
        amount
    from {{ ref('stg_stripe__payment') }}
)

select * from payments