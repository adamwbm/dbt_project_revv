with 

billing_usage as (
    select 
        u.subscription_item_id,
        s.subscription_id,
        s.customer_id,
        s.product_id,
        u.product_used_at::date,
        s.subscription_start_date,
        extract(DAY from u.product_used_at) as used_day,
        extract(DAY from s.subscription_start_date) as start_day,
        u.quantity
    from {{ ref('stg_stripe__usage') }} u
    inner join stg_stripe__subscription s
    using (subscription_item_id)
)

,billing_dates as (
    select 
        subscription_item_id,
        subscription_id,
        customer_id,
        product_id,
        product_used_at,
        subscription_start_date,
        case 
            when used_day >= start_day 
            then extract(month from product_used_at)
            else extract(month from product_used_at - interval '1 month') 
        end as billing_month,
        case 
            when used_day >= start_day 
            then extract(year from product_used_at) 
            else extract(year from product_used_at - interval '1 month') 
        end as billing_year,
        quantity,
        sum(quantity) over (partition by subscription_item_id, 
                                case when used_day >= start_day 
                                    then extract(month from product_used_at)
                                    else extract(month from product_used_at - interval '1 month') end 
                            order by product_used_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rolling_sum_quantity_in_billing_period,
        sum(quantity) OVER (partition by subscription_item_id ORDER BY product_used_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_sum_quantity_total
    from billing_usage
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
        max(rolling_sum_quantity_total) as rolling_sum_quantity_total,
        sum(quantity) as quantity,
        max(rolling_sum_quantity_in_billing_period) as rolling_sum_quantity_in_billing_period
    from billing_dates
    group by 1,2,3,4,5,6,7,8
)

,rolling_payments as (
    select 
        customer_id,
        payment_date as last_payment_date, 
        sum(amount) over (partition by customer_id order by payment_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rolling_sum_payment,
        lead(payment_date) over (
            partition by customer_id 
            order by payment_date
        ) as next_payment_date
  	from {{ ref('fct_payment') }} fp
)

,consolidation as (
    select 
        du.subscription_item_id,
        du.subscription_id,
        du.customer_id,
        du.product_id,
        du.product_used_at,
        du.subscription_start_date,
        du.billing_month,
        du.billing_year,
        dense_rank() over (partition by du.subscription_item_id order by du.billing_month) as billing_period_num_billing_periods,
        max_billing_amount as free_reports,
        du.quantity,
        du.rolling_sum_quantity_in_billing_period,
        case when rolling_sum_quantity_in_billing_period >= max_billing_amount 
            then rolling_sum_quantity_in_billing_period - max_billing_amount 
            else 0 end as billable_quantity,
        case when du.billing_month = extract(month from du.product_used_at) then 1 else 0 
            end as same_month_ind,
        dp.unit_cost_dollars,
        rolling_sum_quantity_total,
        rpr.last_payment_date,
        rpr.next_payment_date,
        rpr.rolling_sum_payment
    from daily_usage du
    left join rolling_payments rpr
        on du.product_used_at between rpr.last_payment_date and coalesce(rpr.next_payment_date - interval '1 day', '2099-01-01')
        and du.customer_id = rpr.customer_id
    left join {{ ref('dim_product') }} dp 
        on dp.product_id = du.product_id
    order by du.subscription_item_id, du.product_id
)

,final as (
    select 
        subscription_item_id,
        subscription_id,
        customer_id,
        product_id,
        product_used_at,
        subscription_start_date,
        billing_month,
        billing_year,
        billing_period_num_billing_periods,
        free_reports,
        quantity,
        unit_cost_dollars,
        rolling_sum_quantity_in_billing_period,
        rolling_sum_quantity_total,
        coalesce(billable_quantity - LAG(billable_quantity, 1) 
            over (partition by billing_period_num_billing_periods,subscription_item_id order by product_used_at),0) AS billable_quantity,
        same_month_ind,
        last_payment_date,
        next_payment_date,
        rolling_sum_payment
    from consolidation
)

select 
    * 
from final
order by subscription_item_id, product_used_at
