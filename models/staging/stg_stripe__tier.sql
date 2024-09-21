with 

source as (
    select 
        REGEXP_REPLACE(product_id,'prod_','') as product_id,
        max(usage_to) as max_billing_amount,
        max(unit_amount/100.00) as unit_cost_dollars
    from {{ ref('stripe_tier') }}
    group by 1
)

select * from source