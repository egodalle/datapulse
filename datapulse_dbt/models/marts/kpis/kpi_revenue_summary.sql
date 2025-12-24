{{ config(
    materialized='table',
    tags=['marts', 'kpis', 'revenue']
) }}

/*
    Revenue KPIs aggregated by platform and time period
*/

with orders as (
    select * from {{ ref('int_unified_orders') }}
    where is_cancelled = false
),

daily_revenue as (
    select
        order_date,
        platform,
        
        -- Revenue metrics
        count(distinct order_id) as total_orders,
        sum(total_amount) as gross_revenue,
        sum(total_amount_usd) as gross_revenue_usd,
        sum(discount_amount) as total_discounts,
        sum(total_amount - discount_amount) as net_revenue,
        
        -- Averages
        avg(total_amount) as avg_order_value,
        avg(total_amount_usd) as avg_order_value_usd,
        avg(item_count) as avg_items_per_order,
        
        -- Payment metrics
        count(case when is_paid then 1 end) as paid_orders,
        count(case when not is_paid then 1 end) as unpaid_orders,
        
        -- Fulfillment metrics
        count(case when is_fulfilled then 1 end) as fulfilled_orders,
        count(case when not is_fulfilled and is_paid then 1 end) as pending_fulfillment
        
    from orders
    group by 1, 2
),

with_growth as (
    select
        *,
        -- Day over day growth
        lag(gross_revenue_usd) over (partition by platform order by order_date) as prev_day_revenue,
        gross_revenue_usd - lag(gross_revenue_usd) over (partition by platform order by order_date) as revenue_change,
        
        -- Running totals
        sum(gross_revenue_usd) over (
            partition by platform, date_trunc('month', order_date) 
            order by order_date
        ) as mtd_revenue_usd,
        
        sum(total_orders) over (
            partition by platform, date_trunc('month', order_date) 
            order by order_date
        ) as mtd_orders
        
    from daily_revenue
)

select
    *,
    case 
        when prev_day_revenue > 0 
        then round((revenue_change / prev_day_revenue) * 100, 2)
        else 0 
    end as revenue_growth_pct,
    current_timestamp as _generated_at
from with_growth

