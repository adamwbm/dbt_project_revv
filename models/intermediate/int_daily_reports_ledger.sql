with 
	
each_use as (
	select 
		u.subscription_item_id,
		s.subscription_id,
		s.customer_id,
		s.product_id,
		u.product_used_at::date,
		s.subscription_start_date,
		case when EXTRACT(DAY FROM u.product_used_at) >= EXTRACT(DAY FROM s.subscription_start_date) 
			then EXTRACT(MONTH FROM u.product_used_at) 
			else EXTRACT(MONTH FROM u.product_used_at - INTERVAL '1 month')
			end as billing_month,
		case when EXTRACT(DAY FROM u.product_used_at) >= EXTRACT(DAY FROM s.subscription_start_date) 
			then EXTRACT(YEAR FROM u.product_used_at) 
			else EXTRACT(YEAR FROM u.product_used_at - INTERVAL '1 month')
			end as billing_year,
		u.quantity,
		SUM(u.quantity) OVER (partition by u.subscription_item_id ORDER BY u.product_used_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_sum_quantity
	from {{ ref('stg_stripe__usage') }} u
		inner join {{ ref('stg_stripe__subscription') }} s
		using(subscription_item_id)
)

,daily_usage as (
	select 
		subscription_item_id,
		subscription_id,
		customer_id,
		product_id,
		product_used_at,
		subscription_start_date,
		billing_month,
		billing_year,
		sum(quantity) as quantity,
		max(rolling_sum_quantity) as rolling_sum_quantity
	from each_use
	group by 1,2,3,4,5,6,7,8
)

,rolling_payments as (
	select 
		customer_id,
		payment_date as last_payment_date, 
		SUM(amount) OVER (partition by customer_id ORDER BY payment_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_sum_payment,
		LEAD(payment_date) over (partition by customer_id order by payment_date) as next_payment_date
	from {{ ref('fct_payment') }} fp 
)

,final as (
	select 
		subscription_item_id,
		subscription_id,
		du.customer_id,
		du.product_id,
		product_used_at,
		subscription_start_date,
		billing_month,
		billing_year,
		quantity,
		rolling_sum_quantity,
		case when rolling_sum_quantity >= 100 then rolling_sum_quantity - 100 
			else 0 end as billable_quantity,
		case when billing_month = extract(month from product_used_at) then 1 
			else 0 end as same_month_ind,
		unit_cost_dollars,
		unit_cost_dollars * (case when rolling_sum_quantity >= 100 then rolling_sum_quantity - 100 else 0 end) as total_reports_billable_dollars,
		last_payment_date,
		next_payment_date,
		rolling_sum_payment
	from daily_usage du
		left join rolling_payments rpr
		on du.product_used_at between (rpr.last_payment_date) and coalesce(rpr.next_payment_date - interval '1 day','01-01-2099')
		and du.customer_id = rpr.customer_id
		left join dim_product dp 
		on dp.product_id = du.product_id
	order by 1,4
)

select * from final 
