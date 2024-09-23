with 

source as (
    select 
        REGEXP_REPLACE(si.subscription_item_id,'si_','') as subscription_item_id,
        cast(REGEXP_REPLACE(si.subscription_id,'sub_','') as int) as subscription_id,
        cast(REGEXP_REPLACE(si.product_id,'prod_','') as int) as product_id,
        cast(REGEXP_REPLACE(s.customer_id,'cus_','') as int) as customer_id,
        s.subscription_date as subscription_start_date
    from {{ ref('stripe_subscription_item') }} si
        inner join {{ ref('stripe_subscription') }} s
        using(subscription_id)
)

select * from source