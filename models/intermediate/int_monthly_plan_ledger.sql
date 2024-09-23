with

recursive plan_monthly as (
	select 
		s.subscription_item_id, 
		s.subscription_id,
		s.product_id,
		customer_id,
		subscription_start_date as billing_date,
		interval_type,
		interval_count,
		product_type,
		max_billing_amount,
		unit_cost_dollars
	from {{ ref('fct_subscription') }} s
		inner join {{ ref('dim_product') }} p
			on s.product_id = p.product_id
		left join {{ ref('fct_usage_monthly') }} um
			on s.subscription_item_id = um.subscription_item_id
	where product_type = 'plan'
    
    union all
    
	select 
		subscription_item_id, 
		subscription_id,
		product_id,
		customer_id,
		(billing_date + INTERVAL '1 month')::DATE as billing_date, 
		interval_type,
		interval_count,
		product_type,
		max_billing_amount,
		unit_cost_dollars
    from plan_monthly
    where (billing_date + INTERVAL '1 month')::DATE < '2024-06-01'
    
)

,ranked as (
	select 
        * 
        ,extract(month from billing_date) as billing_month
        ,extract(year from billing_date) as billing_year
        ,case when interval_count > 1 then (ROW_NUMBER() over (partition by subscription_item_id order by billing_date) -1) % interval_count + 1 
            else 1 end as interval_rank
	from plan_monthly
	order by 1,billing_date
)

,final as (
	select  
		subscription_item_id, 
		subscription_id,
		product_id,
		customer_id,
		billing_date, 
		interval_type,
		interval_count,
		product_type,
		max_billing_amount,
		unit_cost_dollars,
		billing_month,
		billing_year
	from ranked
	where interval_rank = 1
)

select * from final
