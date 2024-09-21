with


recursive plan_monthly AS (

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
		(billing_date + INTERVAL '1 month')::DATE, 
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
        ,extract(month from billing_date) || '/' || extract(year from billing_date) as billing_month_year
        ,case when interval_count > 1 then (ROW_NUMBER() over (partition by subscription_item_id order by billing_date) -1) % interval_count + 1 
            else 1 end as interval_rank
	FROM plan_monthly
	order by 1,billing_date
)

,reports as (
	select 
		s.subscription_item_id, 
		s.subscription_id,
		s.product_id,
		customer_id,
		subscription_start_date,
		um.billing_month_year,
		interval_type,
		product_type,
		max_billing_amount,
		unit_cost_dollars,
		um.usage_quantity,
		case when usage_quantity > 100 then unit_cost_dollars * 100 else unit_cost_dollars * usage_quantity end as reports_usage_cost
	from {{ ref('fct_subscription') }} s
		inner join {{ ref('dim_product') }} p
		on s.product_id = p.product_id
		left join {{ ref('fct_usage_monthly') }} um
		on s.subscription_item_id = um.subscription_item_id
	where product_type = 'reports'
)

,final as (
    select 
        re.*
        ,ra.interval_count
    ,case when interval_rank = 1 then ra.unit_cost_dollars
        else null 
        end as plan_unit_cost_dollars
    ,case when interval_rank = 1 then ra.unit_cost_dollars + re.reports_usage_cost
        else re.reports_usage_cost 
        end as total_unit_cost_dollars
    from reports re
        left join ranked ra
        using(billing_month_year,subscription_id)
)

select * from final