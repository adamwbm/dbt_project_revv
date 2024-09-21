with

subscriptions as (
    select 
        subscription_item_id,
        subscription_id,
        product_id,
        customer_id,
        subscription_start_date
    from {{ ref('stg_stripe__subscription') }} si
)

select * from subscriptions