{{ config(
    materialized='view',
    tags=['intermediate', 'orders']
) }}

/*
    Unified orders from all e-commerce platforms
    This model standardizes orders across Shopify, Amazon, Lazada, and Shopee
*/

with shopify_orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

amazon_orders as (
    select * from {{ ref('stg_amazon__orders') }}
),

lazada_orders as (
    select * from {{ ref('stg_lazada__orders') }}
),

shopee_orders as (
    select * from {{ ref('stg_shopee__orders') }}
),

unified as (
    select * from shopify_orders
    union all
    select * from amazon_orders
    union all
    select * from lazada_orders
    union all
    select * from shopee_orders
),

with_calculated_fields as (
    select
        *,
        -- Convert to USD using var rates
        case currency_code
            when 'PHP' then total_amount * {{ var('currency_rates')['PHP'] }}
            when 'MYR' then total_amount * {{ var('currency_rates')['MYR'] }}
            when 'SGD' then total_amount * {{ var('currency_rates')['SGD'] }}
            when 'IDR' then total_amount * {{ var('currency_rates')['IDR'] }}
            when 'USD' then total_amount
            else total_amount  -- Default: assume USD
        end as total_amount_usd,
        
        -- Date dimensions
        date(order_created_at) as order_date,
        date_trunc('week', order_created_at)::date as order_week,
        date_trunc('month', order_created_at)::date as order_month,
        date_trunc('quarter', order_created_at)::date as order_quarter,
        date_trunc('year', order_created_at)::date as order_year,
        
        -- Time dimensions
        extract(hour from order_created_at)::int as order_hour,
        extract(dow from order_created_at)::int as order_day_of_week,
        
        -- Status flags
        case 
            when payment_status in ('paid', 'authorized', 'partially_paid') then true
            else false
        end as is_paid,
        
        case 
            when fulfillment_status in ('fulfilled', 'shipped', 'delivered', 'COMPLETED', 'Shipped') then true
            else false
        end as is_fulfilled,
        
        case 
            when order_cancelled_at is not null then true
            else false
        end as is_cancelled
        
    from unified
)

select * from with_calculated_fields

