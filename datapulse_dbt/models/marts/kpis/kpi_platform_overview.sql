{{ config(
    materialized='table',
    tags=['marts', 'kpis', 'overview']
) }}

/*
    Platform-level KPI overview - high-level metrics for dashboard
*/

with orders as (
    select * from {{ ref('int_unified_orders') }}
),

platform_metrics as (
    select
        platform,
        
        -- Total metrics (all time)
        count(distinct order_id) as total_orders,
        count(distinct case when not is_cancelled then order_id end) as completed_orders,
        count(distinct case when is_cancelled then order_id end) as cancelled_orders,
        sum(case when not is_cancelled then total_amount_usd else 0 end) as total_revenue_usd,
        
        -- This month metrics
        count(distinct case when order_month = date_trunc('month', current_date) then order_id end) as orders_this_month,
        sum(case when order_month = date_trunc('month', current_date) and not is_cancelled then total_amount_usd else 0 end) as revenue_this_month_usd,
        
        -- Last month metrics
        count(distinct case when order_month = date_trunc('month', current_date - interval '1 month') then order_id end) as orders_last_month,
        sum(case when order_month = date_trunc('month', current_date - interval '1 month') and not is_cancelled then total_amount_usd else 0 end) as revenue_last_month_usd,
        
        -- Today's metrics
        count(distinct case when order_date = current_date then order_id end) as orders_today,
        sum(case when order_date = current_date and not is_cancelled then total_amount_usd else 0 end) as revenue_today_usd,
        
        -- Averages
        avg(case when not is_cancelled then total_amount_usd end) as avg_order_value_usd,
        avg(case when not is_cancelled then item_count end) as avg_items_per_order,
        
        -- Rates
        round(100.0 * count(case when is_paid then 1 end) / nullif(count(*), 0), 2) as payment_rate,
        round(100.0 * count(case when is_fulfilled then 1 end) / nullif(count(case when is_paid then 1 end), 0), 2) as fulfillment_rate,
        round(100.0 * count(case when is_cancelled then 1 end) / nullif(count(*), 0), 2) as cancellation_rate,
        
        -- Date ranges
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct order_date) as active_days
        
    from orders
    group by 1
),

with_growth as (
    select
        *,
        -- Month over month growth
        case 
            when revenue_last_month_usd > 0 
            then round(100.0 * (revenue_this_month_usd - revenue_last_month_usd) / revenue_last_month_usd, 2)
            else 0 
        end as revenue_mom_growth_pct,
        
        case 
            when orders_last_month > 0 
            then round(100.0 * (orders_this_month - orders_last_month) / orders_last_month, 2)
            else 0 
        end as orders_mom_growth_pct
        
    from platform_metrics
)

select
    *,
    current_timestamp as _generated_at
from with_growth

