with 

usage as (
	select 
		u.subscription_item_id,
		case when EXTRACT(DAY FROM u.product_used_at) >= EXTRACT(DAY FROM s.subscription_start_date) 
			then EXTRACT(MONTH FROM u.product_used_at) 
			else EXTRACT(MONTH FROM u.product_used_at - INTERVAL '1 month')
			end as billing_month,
		case when EXTRACT(DAY FROM u.product_used_at) >= EXTRACT(DAY FROM s.subscription_start_date) 
			then EXTRACT(YEAR FROM u.product_used_at) 
			else EXTRACT(YEAR FROM u.product_used_at - INTERVAL '1 month')
			end as billing_year,
		u.quantity
	from {{ ref('stg_stripe__usage') }} u
		inner join {{ ref('stg_stripe__subscription') }} s
		using(subscription_item_id)
)

,final as (
    select 
        subscription_item_id,
        billing_month || '/' || billing_year as billing_month_year,
        sum(quantity) as usage_quantity
    from usage
    group by 1,2
)

select * from final