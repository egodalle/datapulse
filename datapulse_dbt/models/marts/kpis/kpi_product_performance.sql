{{ config(
    materialized='table',
    tags=['marts', 'kpis', 'products']
) }}

/*
    Product performance KPIs
*/

with order_items as (
    select * from {{ ref('int_unified_order_items') }}
),

orders as (
    select * from {{ ref('int_unified_orders') }}
    where is_cancelled = false
),

items_with_orders as (
    select
        oi.*,
        o.order_date,
        o.order_month,
        o.is_paid,
        o.currency_code
    from order_items oi
    inner join orders o on oi.order_id = o.order_id and oi.platform = o.platform
),

product_metrics as (
    select
        platform,
        product_id,
        product_name,
        sku,
        
        -- Volume metrics
        count(distinct order_id) as total_orders,
        sum(quantity) as total_units_sold,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        avg(unit_price) as avg_selling_price,
        
        -- Time metrics
        count(distinct order_date) as days_with_sales,
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        
        -- This month
        sum(case when order_month = date_trunc('month', current_date) then quantity else 0 end) as units_this_month,
        sum(case when order_month = date_trunc('month', current_date) then line_total else 0 end) as revenue_this_month,
        
        -- Calculated velocity
        case 
            when count(distinct order_date) > 0 
            then round(sum(quantity)::numeric / count(distinct order_date), 2)
            else 0 
        end as avg_daily_units
        
    from items_with_orders
    group by 1, 2, 3, 4
),

ranked as (
    select
        *,
        row_number() over (partition by platform order by total_revenue desc) as revenue_rank,
        row_number() over (partition by platform order by total_units_sold desc) as units_rank,
        percent_rank() over (partition by platform order by total_revenue) as revenue_percentile
    from product_metrics
)

select
    *,
    case 
        when revenue_rank <= 10 then 'Top 10'
        when revenue_percentile >= 0.8 then 'Top Performer'
        when revenue_percentile >= 0.5 then 'Average'
        else 'Underperformer'
    end as performance_tier,
    current_timestamp as _generated_at
from ranked

