with

products as (
    select 
        p.product_id,
        p.interval_type,
        p.interval_count,
        p.type as product_type,
        t.max_billing_amount,
        coalesce(p.unit_cost_dollars,t.unit_cost_dollars) as unit_cost_dollars
    from {{ ref('stg_stripe__product') }} p
        left join {{ ref('stg_stripe__tier') }} t
            using(product_id)
)

select * from products