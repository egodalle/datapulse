"""
DBT-like transformations endpoint - Creates views and tables for KPIs
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.database import get_db

router = APIRouter()

@router.post("/run-models")
async def run_dbt_models(db: AsyncSession = Depends(get_db)):
    """
    Run dbt-like transformations to create staging, intermediate, and mart tables
    """
    try:
        # ============== STAGING VIEWS ==============
        
        # Shopify Orders Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_shopify__orders AS
            SELECT
                id::varchar as order_id,
                'shopify' as platform,
                created_at::timestamp as order_created_at,
                updated_at::timestamp as order_updated_at,
                processed_at::timestamp as order_processed_at,
                cancelled_at::timestamp as order_cancelled_at,
                closed_at::timestamp as order_closed_at,
                customer_id::varchar as customer_id,
                email as customer_email,
                total_price::decimal(12,2) as total_amount,
                subtotal_price::decimal(12,2) as subtotal_amount,
                total_tax::decimal(12,2) as tax_amount,
                total_discounts::decimal(12,2) as discount_amount,
                currency as currency_code,
                financial_status as payment_status,
                fulfillment_status,
                cancel_reason,
                coalesce(line_items_count, 0) as item_count,
                source_name as order_source,
                tags as order_tags,
                note as order_notes,
                current_timestamp as _loaded_at
            FROM raw.shopify_orders
        """))

        # Shopify Order Items Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_shopify__order_items AS
            SELECT
                id::varchar as line_item_id,
                order_id::varchar as order_id,
                product_id::varchar as product_id,
                variant_id::varchar as variant_id,
                'shopify' as platform,
                title as product_name,
                variant_title,
                sku,
                quantity::int as quantity,
                price::decimal(12,2) as unit_price,
                (quantity * price)::decimal(12,2) as line_total,
                coalesce(total_discount::decimal(12,2), 0) as discount_amount,
                fulfillment_status,
                fulfillable_quantity::int as fulfillable_quantity,
                gift_card as is_gift_card,
                taxable as is_taxable,
                requires_shipping as requires_shipping,
                current_timestamp as _loaded_at
            FROM raw.shopify_order_line_items
        """))

        # Amazon Orders Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_amazon__orders AS
            SELECT
                amazon_order_id::varchar as order_id,
                'amazon' as platform,
                purchase_date::timestamp as order_created_at,
                last_update_date::timestamp as order_updated_at,
                null::timestamp as order_processed_at,
                null::timestamp as order_cancelled_at,
                null::timestamp as order_closed_at,
                buyer_email as customer_id,
                buyer_email as customer_email,
                (order_total->>'Amount')::decimal(12,2) as total_amount,
                (order_total->>'Amount')::decimal(12,2) as subtotal_amount,
                0::decimal(12,2) as tax_amount,
                0::decimal(12,2) as discount_amount,
                (order_total->>'CurrencyCode')::varchar as currency_code,
                payment_method as payment_status,
                order_status as fulfillment_status,
                null as cancel_reason,
                number_of_items_shipped + number_of_items_unshipped as item_count,
                sales_channel as order_source,
                null as order_tags,
                null as order_notes,
                current_timestamp as _loaded_at
            FROM raw.amazon_orders
        """))

        # Amazon Order Items Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_amazon__order_items AS
            SELECT
                order_item_id::varchar as line_item_id,
                amazon_order_id::varchar as order_id,
                asin::varchar as product_id,
                null::varchar as variant_id,
                'amazon' as platform,
                title as product_name,
                null as variant_title,
                seller_sku as sku,
                quantity_ordered::int as quantity,
                (item_price->>'Amount')::decimal(12,2) / nullif(quantity_ordered, 0) as unit_price,
                (item_price->>'Amount')::decimal(12,2) as line_total,
                coalesce((promotion_discount->>'Amount')::decimal(12,2), 0) as discount_amount,
                case when quantity_shipped > 0 then 'shipped' else 'pending' end as fulfillment_status,
                (quantity_ordered - quantity_shipped)::int as fulfillable_quantity,
                false as is_gift_card,
                true as is_taxable,
                true as requires_shipping,
                current_timestamp as _loaded_at
            FROM raw.amazon_order_items
        """))

        # Lazada Orders Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_lazada__orders AS
            SELECT
                order_id::varchar as order_id,
                'lazada' as platform,
                created_at::timestamp as order_created_at,
                updated_at::timestamp as order_updated_at,
                null::timestamp as order_processed_at,
                null::timestamp as order_cancelled_at,
                null::timestamp as order_closed_at,
                customer_id::varchar as customer_id,
                buyer_email as customer_email,
                price::decimal(12,2) as total_amount,
                price::decimal(12,2) as subtotal_amount,
                0::decimal(12,2) as tax_amount,
                coalesce(voucher::decimal(12,2), 0) as discount_amount,
                'PHP' as currency_code,
                payment_method as payment_status,
                statuses as fulfillment_status,
                null as cancel_reason,
                items_count::int as item_count,
                'lazada' as order_source,
                null as order_tags,
                remarks as order_notes,
                current_timestamp as _loaded_at
            FROM raw.lazada_orders
        """))

        # Lazada Order Items Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_lazada__order_items AS
            SELECT
                order_item_id::varchar as line_item_id,
                order_id::varchar as order_id,
                product_id::varchar as product_id,
                null::varchar as variant_id,
                'lazada' as platform,
                name as product_name,
                variation as variant_title,
                sku,
                1::int as quantity,
                paid_price::decimal(12,2) as unit_price,
                paid_price::decimal(12,2) as line_total,
                coalesce(voucher_amount::decimal(12,2), 0) as discount_amount,
                status as fulfillment_status,
                0::int as fulfillable_quantity,
                false as is_gift_card,
                true as is_taxable,
                true as requires_shipping,
                current_timestamp as _loaded_at
            FROM raw.lazada_order_items
        """))

        # Shopee Orders Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_shopee__orders AS
            SELECT
                order_sn::varchar as order_id,
                'shopee' as platform,
                to_timestamp(create_time) as order_created_at,
                to_timestamp(update_time) as order_updated_at,
                to_timestamp(pay_time) as order_processed_at,
                case when order_status = 'CANCELLED' then to_timestamp(update_time) else null end as order_cancelled_at,
                case when order_status = 'COMPLETED' then to_timestamp(update_time) else null end as order_closed_at,
                buyer_user_id::varchar as customer_id,
                buyer_username as customer_email,
                total_amount::decimal(12,2) as total_amount,
                (total_amount - coalesce(estimated_shipping_fee, 0))::decimal(12,2) as subtotal_amount,
                0::decimal(12,2) as tax_amount,
                coalesce(voucher_absorbed::decimal(12,2), 0) as discount_amount,
                currency as currency_code,
                case 
                    when order_status in ('READY_TO_SHIP', 'PROCESSED', 'SHIPPED', 'COMPLETED') then 'paid'
                    when order_status = 'UNPAID' then 'pending'
                    else 'unknown'
                end as payment_status,
                order_status as fulfillment_status,
                cancel_reason,
                1 as item_count,
                'shopee' as order_source,
                null as order_tags,
                message_to_seller as order_notes,
                current_timestamp as _loaded_at
            FROM raw.shopee_orders
        """))

        # Shopee Order Items Staging
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_staging.stg_shopee__order_items AS
            SELECT
                (order_sn || '_' || item_id::varchar)::varchar as line_item_id,
                order_sn::varchar as order_id,
                item_id::varchar as product_id,
                model_id::varchar as variant_id,
                'shopee' as platform,
                item_name as product_name,
                model_name as variant_title,
                model_sku as sku,
                model_quantity_purchased::int as quantity,
                model_discounted_price::decimal(12,2) as unit_price,
                (model_quantity_purchased * model_discounted_price)::decimal(12,2) as line_total,
                coalesce((model_original_price - model_discounted_price) * model_quantity_purchased, 0)::decimal(12,2) as discount_amount,
                'pending' as fulfillment_status,
                0::int as fulfillable_quantity,
                false as is_gift_card,
                true as is_taxable,
                true as requires_shipping,
                current_timestamp as _loaded_at
            FROM raw.shopee_order_items
        """))

        await db.commit()

        # ============== INTERMEDIATE VIEWS ==============

        # Unified Orders
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_intermediate.int_unified_orders AS
            WITH unified AS (
                SELECT * FROM public_staging.stg_shopify__orders
                UNION ALL
                SELECT * FROM public_staging.stg_amazon__orders
                UNION ALL
                SELECT * FROM public_staging.stg_lazada__orders
                UNION ALL
                SELECT * FROM public_staging.stg_shopee__orders
            )
            SELECT
                *,
                case currency_code
                    when 'PHP' then total_amount * 0.018
                    when 'MYR' then total_amount * 0.21
                    when 'SGD' then total_amount * 0.74
                    when 'IDR' then total_amount * 0.000063
                    when 'USD' then total_amount
                    else total_amount
                end as total_amount_usd,
                date(order_created_at) as order_date,
                date_trunc('week', order_created_at)::date as order_week,
                date_trunc('month', order_created_at)::date as order_month,
                date_trunc('quarter', order_created_at)::date as order_quarter,
                date_trunc('year', order_created_at)::date as order_year,
                extract(hour from order_created_at)::int as order_hour,
                extract(dow from order_created_at)::int as order_day_of_week,
                case when payment_status in ('paid', 'authorized', 'partially_paid') then true else false end as is_paid,
                case when fulfillment_status in ('fulfilled', 'shipped', 'delivered', 'COMPLETED', 'Shipped') then true else false end as is_fulfilled,
                case when order_cancelled_at is not null then true else false end as is_cancelled
            FROM unified
        """))

        # Unified Order Items
        await db.execute(text("""
            CREATE OR REPLACE VIEW public_intermediate.int_unified_order_items AS
            SELECT * FROM public_staging.stg_shopify__order_items
            UNION ALL
            SELECT * FROM public_staging.stg_amazon__order_items
            UNION ALL
            SELECT * FROM public_staging.stg_lazada__order_items
            UNION ALL
            SELECT * FROM public_staging.stg_shopee__order_items
        """))

        await db.commit()

        # ============== MART TABLES ==============

        # Platform Overview
        await db.execute(text("DROP TABLE IF EXISTS public_marts.kpi_platform_overview"))
        await db.execute(text("""
            CREATE TABLE public_marts.kpi_platform_overview AS
            WITH orders AS (SELECT * FROM public_intermediate.int_unified_orders),
            platform_metrics AS (
                SELECT
                    platform,
                    count(distinct order_id) as total_orders,
                    count(distinct case when not is_cancelled then order_id end) as completed_orders,
                    count(distinct case when is_cancelled then order_id end) as cancelled_orders,
                    sum(case when not is_cancelled then total_amount_usd else 0 end) as total_revenue_usd,
                    count(distinct case when order_month = date_trunc('month', current_date) then order_id end) as orders_this_month,
                    sum(case when order_month = date_trunc('month', current_date) and not is_cancelled then total_amount_usd else 0 end) as revenue_this_month_usd,
                    count(distinct case when order_month = date_trunc('month', current_date - interval '1 month') then order_id end) as orders_last_month,
                    sum(case when order_month = date_trunc('month', current_date - interval '1 month') and not is_cancelled then total_amount_usd else 0 end) as revenue_last_month_usd,
                    count(distinct case when order_date = current_date then order_id end) as orders_today,
                    sum(case when order_date = current_date and not is_cancelled then total_amount_usd else 0 end) as revenue_today_usd,
                    avg(case when not is_cancelled then total_amount_usd end) as avg_order_value_usd,
                    avg(case when not is_cancelled then item_count end) as avg_items_per_order,
                    round(100.0 * count(case when is_paid then 1 end) / nullif(count(*), 0), 2) as payment_rate,
                    round(100.0 * count(case when is_fulfilled then 1 end) / nullif(count(case when is_paid then 1 end), 0), 2) as fulfillment_rate,
                    round(100.0 * count(case when is_cancelled then 1 end) / nullif(count(*), 0), 2) as cancellation_rate,
                    min(order_date) as first_order_date,
                    max(order_date) as last_order_date,
                    count(distinct order_date) as active_days
                FROM orders GROUP BY 1
            )
            SELECT *,
                case when revenue_last_month_usd > 0 then round(100.0 * (revenue_this_month_usd - revenue_last_month_usd) / revenue_last_month_usd, 2) else 0 end as revenue_mom_growth_pct,
                case when orders_last_month > 0 then round(100.0 * (orders_this_month - orders_last_month) / orders_last_month, 2) else 0 end as orders_mom_growth_pct,
                current_timestamp as _generated_at
            FROM platform_metrics
        """))

        # Daily Snapshot
        await db.execute(text("DROP TABLE IF EXISTS public_marts.kpi_daily_snapshot"))
        await db.execute(text("""
            CREATE TABLE public_marts.kpi_daily_snapshot AS
            WITH orders AS (SELECT * FROM public_intermediate.int_unified_orders WHERE not is_cancelled),
            daily_all_platforms AS (
                SELECT
                    order_date,
                    count(distinct order_id) as total_orders,
                    sum(total_amount_usd) as total_revenue_usd,
                    avg(total_amount_usd) as avg_order_value_usd,
                    sum(item_count) as total_items_sold,
                    count(distinct case when platform = 'shopify' then order_id end) as shopify_orders,
                    count(distinct case when platform = 'amazon' then order_id end) as amazon_orders,
                    count(distinct case when platform = 'lazada' then order_id end) as lazada_orders,
                    count(distinct case when platform = 'shopee' then order_id end) as shopee_orders,
                    sum(case when platform = 'shopify' then total_amount_usd else 0 end) as shopify_revenue_usd,
                    sum(case when platform = 'amazon' then total_amount_usd else 0 end) as amazon_revenue_usd,
                    sum(case when platform = 'lazada' then total_amount_usd else 0 end) as lazada_revenue_usd,
                    sum(case when platform = 'shopee' then total_amount_usd else 0 end) as shopee_revenue_usd,
                    count(distinct customer_id) as unique_customers,
                    count(case when is_fulfilled then 1 end) as fulfilled_orders,
                    round(100.0 * count(case when is_fulfilled then 1 end) / nullif(count(*), 0), 2) as fulfillment_rate
                FROM orders GROUP BY 1
            )
            SELECT *,
                avg(total_revenue_usd) over (order by order_date rows between 6 preceding and current row) as revenue_7d_avg,
                avg(total_orders) over (order by order_date rows between 6 preceding and current row) as orders_7d_avg,
                avg(total_revenue_usd) over (order by order_date rows between 29 preceding and current row) as revenue_30d_avg,
                avg(total_orders) over (order by order_date rows between 29 preceding and current row) as orders_30d_avg,
                total_revenue_usd - lag(total_revenue_usd) over (order by order_date) as revenue_dod_change,
                total_orders - lag(total_orders) over (order by order_date) as orders_dod_change,
                total_revenue_usd - lag(total_revenue_usd, 7) over (order by order_date) as revenue_wow_change,
                total_orders - lag(total_orders, 7) over (order by order_date) as orders_wow_change,
                current_timestamp as _generated_at
            FROM daily_all_platforms ORDER BY order_date DESC
        """))

        # Revenue Summary
        await db.execute(text("DROP TABLE IF EXISTS public_marts.kpi_revenue_summary"))
        await db.execute(text("""
            CREATE TABLE public_marts.kpi_revenue_summary AS
            WITH orders AS (SELECT * FROM public_intermediate.int_unified_orders WHERE is_cancelled = false),
            daily_revenue AS (
                SELECT
                    order_date, platform,
                    count(distinct order_id) as total_orders,
                    sum(total_amount) as gross_revenue,
                    sum(total_amount_usd) as gross_revenue_usd,
                    sum(total_amount - discount_amount) as net_revenue,
                    avg(total_amount) as avg_order_value,
                    avg(total_amount_usd) as avg_order_value_usd,
                    count(case when is_paid then 1 end) as paid_orders,
                    count(case when not is_paid then 1 end) as unpaid_orders,
                    count(case when is_fulfilled then 1 end) as fulfilled_orders,
                    count(case when not is_fulfilled and is_paid then 1 end) as pending_fulfillment
                FROM orders GROUP BY 1, 2
            )
            SELECT *,
                lag(gross_revenue_usd) over (partition by platform order by order_date) as prev_day_revenue,
                gross_revenue_usd - lag(gross_revenue_usd) over (partition by platform order by order_date) as revenue_change,
                sum(gross_revenue_usd) over (partition by platform, date_trunc('month', order_date) order by order_date) as mtd_revenue_usd,
                sum(total_orders) over (partition by platform, date_trunc('month', order_date) order by order_date) as mtd_orders,
                case when lag(gross_revenue_usd) over (partition by platform order by order_date) > 0 
                    then round((gross_revenue_usd - lag(gross_revenue_usd) over (partition by platform order by order_date)) / lag(gross_revenue_usd) over (partition by platform order by order_date) * 100, 2)
                    else 0 end as revenue_growth_pct,
                current_timestamp as _generated_at
            FROM daily_revenue
        """))

        # Product Performance
        await db.execute(text("DROP TABLE IF EXISTS public_marts.kpi_product_performance"))
        await db.execute(text("""
            CREATE TABLE public_marts.kpi_product_performance AS
            WITH order_items AS (SELECT * FROM public_intermediate.int_unified_order_items),
            orders AS (SELECT * FROM public_intermediate.int_unified_orders WHERE is_cancelled = false),
            items_with_orders AS (
                SELECT oi.*, o.order_date, o.order_month, o.is_paid, o.currency_code
                FROM order_items oi INNER JOIN orders o ON oi.order_id = o.order_id AND oi.platform = o.platform
            ),
            product_metrics AS (
                SELECT
                    platform, product_id, product_name, sku,
                    count(distinct order_id) as total_orders,
                    sum(quantity) as total_units_sold,
                    sum(line_total) as total_revenue,
                    avg(unit_price) as avg_selling_price,
                    count(distinct order_date) as days_with_sales,
                    min(order_date) as first_sale_date,
                    max(order_date) as last_sale_date,
                    sum(case when order_month = date_trunc('month', current_date) then quantity else 0 end) as units_this_month,
                    sum(case when order_month = date_trunc('month', current_date) then line_total else 0 end) as revenue_this_month,
                    case when count(distinct order_date) > 0 then round(sum(quantity)::numeric / count(distinct order_date), 2) else 0 end as avg_daily_units
                FROM items_with_orders GROUP BY 1, 2, 3, 4
            ),
            ranked AS (
                SELECT *,
                    row_number() over (partition by platform order by total_revenue desc) as revenue_rank,
                    row_number() over (partition by platform order by total_units_sold desc) as units_rank,
                    percent_rank() over (partition by platform order by total_revenue) as revenue_percentile
                FROM product_metrics
            )
            SELECT *,
                case when revenue_rank <= 10 then 'Top 10'
                     when revenue_percentile >= 0.8 then 'Top Performer'
                     when revenue_percentile >= 0.5 then 'Average'
                     else 'Underperformer' end as performance_tier,
                current_timestamp as _generated_at
            FROM ranked
        """))

        await db.commit()

        return {
            "status": "success",
            "message": "All models created successfully",
            "models": {
                "staging": ["stg_shopify__orders", "stg_shopify__order_items", "stg_amazon__orders", "stg_amazon__order_items", "stg_lazada__orders", "stg_lazada__order_items", "stg_shopee__orders", "stg_shopee__order_items"],
                "intermediate": ["int_unified_orders", "int_unified_order_items"],
                "marts": ["kpi_platform_overview", "kpi_daily_snapshot", "kpi_revenue_summary", "kpi_product_performance"]
            }
        }

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Model creation failed: {str(e)}")

