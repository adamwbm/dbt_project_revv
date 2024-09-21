
with 

fifteenth_of_month AS (
    select 
        date_day AS fifteenth_date 
    from {{ ref('dim_calendar') }}
    where extract(day from date_day) = 15 
    and date_day between '2024-01-01' and '2024-05-31' 
)
 
,report_balance_due as (  
    select 
    	fifteenth_of_month.fifteenth_date as report_date,
	    customer_id,
	    max(total_reports_billable_dollars) as report_balance_due
    from {{ ref('int_daily_reports_ledger') }} 
    join fifteenth_of_month
    	on product_used_at < fifteenth_of_month.fifteenth_date
    where same_month_ind = 0
    group by 1,2
) 

,plan_balance_due as (
	select 
		  fifteenth_of_month.fifteenth_date as report_date,
	  customer_id,
	  sum(unit_cost_dollars) as plan_balance_due
	from {{ ref('int_monthly_plan_ledger') }}
	join fifteenth_of_month
    	on billing_date < fifteenth_of_month.fifteenth_date
	group by 1,2
)

,payment as (
	select 
		fifteenth_of_month.fifteenth_date as report_date,
		customer_id,
		SUM(amount) as total_payment_completed
	from {{ ref('fct_payment') }}
	join fifteenth_of_month
   		on payment_date < fifteenth_of_month.fifteenth_date
   	group by 1,2
)


,final as (
	select 
		r.report_date,
		c.customer_name,
		report_balance_due + plan_balance_due - total_payment_completed as total_overdue_balance	
	from report_balance_due r
		left join plan_balance_due pl
		on r.customer_id = pl.customer_id
		and r.report_date = pl.report_date
		left join payment pa 
		on r.customer_id::int = pa.customer_id
		and r.report_date = pa.report_date
		left join dim_customer c
		on r.customer_id = c.customer_id
)

select 
* 
from final
order by 1,2
