with 

source as (
    select 
        REGEXP_REPLACE(product_id,'prod_','') as product_id,
        interval_type,
        interval_count,
        type,
        unit_amount/100.00 as unit_cost_dollars
    from {{ ref('stripe_product') }}
)

select * from source