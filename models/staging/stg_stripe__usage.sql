with 

source as (
    select 
        REGEXP_REPLACE(subscription_item_id,'si_','') as subscription_item_id,
        timestamp as product_used_at,
        quantity
    from {{ ref('stripe_usage') }}
)

select * from source