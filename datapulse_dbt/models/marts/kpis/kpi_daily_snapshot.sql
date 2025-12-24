{{ config(
    materialized='table',
    tags=['marts', 'kpis', 'daily']
) }}

/*
    Daily snapshot of all key metrics across platforms
    This is the main table the API will query for dashboard KPIs
*/

with orders as (
    select * from {{ ref('int_unified_orders') }}
    where not is_cancelled
),

daily_all_platforms as (
    select
        order_date,
        
        -- Overall metrics
        count(distinct order_id) as total_orders,
        sum(total_amount_usd) as total_revenue_usd,
        avg(total_amount_usd) as avg_order_value_usd,
        sum(item_count) as total_items_sold,
        
        -- Platform breakdown
        count(distinct case when platform = 'shopify' then order_id end) as shopify_orders,
        count(distinct case when platform = 'amazon' then order_id end) as amazon_orders,
        count(distinct case when platform = 'lazada' then order_id end) as lazada_orders,
        count(distinct case when platform = 'shopee' then order_id end) as shopee_orders,
        
        sum(case when platform = 'shopify' then total_amount_usd else 0 end) as shopify_revenue_usd,
        sum(case when platform = 'amazon' then total_amount_usd else 0 end) as amazon_revenue_usd,
        sum(case when platform = 'lazada' then total_amount_usd else 0 end) as lazada_revenue_usd,
        sum(case when platform = 'shopee' then total_amount_usd else 0 end) as shopee_revenue_usd,
        
        -- Customer metrics
        count(distinct customer_id) as unique_customers,
        
        -- Fulfillment metrics
        count(case when is_fulfilled then 1 end) as fulfilled_orders,
        round(100.0 * count(case when is_fulfilled then 1 end) / nullif(count(*), 0), 2) as fulfillment_rate
        
    from orders
    group by 1
),

with_trends as (
    select
        *,
        -- 7 day moving averages
        avg(total_revenue_usd) over (order by order_date rows between 6 preceding and current row) as revenue_7d_avg,
        avg(total_orders) over (order by order_date rows between 6 preceding and current row) as orders_7d_avg,
        
        -- 30 day moving averages
        avg(total_revenue_usd) over (order by order_date rows between 29 preceding and current row) as revenue_30d_avg,
        avg(total_orders) over (order by order_date rows between 29 preceding and current row) as orders_30d_avg,
        
        -- Day over day change
        total_revenue_usd - lag(total_revenue_usd) over (order by order_date) as revenue_dod_change,
        total_orders - lag(total_orders) over (order by order_date) as orders_dod_change,
        
        -- Week over week (same day last week)
        total_revenue_usd - lag(total_revenue_usd, 7) over (order by order_date) as revenue_wow_change,
        total_orders - lag(total_orders, 7) over (order by order_date) as orders_wow_change
        
    from daily_all_platforms
)

select
    *,
    current_timestamp as _generated_at
from with_trends
order by order_date desc

