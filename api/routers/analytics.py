"""
Analytics endpoints for Lovable UI
- Products Analytics
- Customers Analytics
- Locations Analytics
- Profitability Analytics
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional, List
from datetime import date, datetime, timedelta

from core.database import get_db

router = APIRouter()


# ============================================
# PRODUCTS ANALYTICS
# ============================================

@router.get("/products")
async def get_product_analytics(
    platform: Optional[str] = Query(None, description="Filter by platform"),
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get product analytics - top products, sales by product, category performance"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    
    # Top selling products by revenue
    top_products_sql = """
        SELECT 
            p.title as product_name,
            p.product_type as category,
            p.vendor,
            COUNT(DISTINCT li.order_id) as total_orders,
            SUM(li.quantity) as units_sold,
            SUM(li.price * li.quantity) as total_revenue,
            AVG(li.price) as avg_price
        FROM raw.shopify_order_line_items li
        JOIN raw.shopify_products p ON li.product_id = p.id
        JOIN raw.shopify_orders o ON li.order_id = o.id
        WHERE o.created_at >= :date_filter
        GROUP BY p.id, p.title, p.product_type, p.vendor
        ORDER BY total_revenue DESC
        LIMIT 20
    """
    
    result = await db.execute(text(top_products_sql), {"date_filter": date_filter})
    top_products = [
        {
            "product_name": row[0],
            "category": row[1] or "Uncategorized",
            "vendor": row[2] or "Unknown",
            "total_orders": row[3],
            "units_sold": row[4],
            "total_revenue": float(row[5]) if row[5] else 0,
            "avg_price": float(row[6]) if row[6] else 0
        }
        for row in result.fetchall()
    ]
    
    # Category performance
    category_sql = """
        SELECT 
            COALESCE(p.product_type, 'Uncategorized') as category,
            COUNT(DISTINCT p.id) as product_count,
            SUM(li.quantity) as units_sold,
            SUM(li.price * li.quantity) as total_revenue
        FROM raw.shopify_order_line_items li
        JOIN raw.shopify_products p ON li.product_id = p.id
        JOIN raw.shopify_orders o ON li.order_id = o.id
        WHERE o.created_at >= :date_filter
        GROUP BY p.product_type
        ORDER BY total_revenue DESC
    """
    
    result = await db.execute(text(category_sql), {"date_filter": date_filter})
    categories = [
        {
            "category": row[0],
            "product_count": row[1],
            "units_sold": row[2],
            "total_revenue": float(row[3]) if row[3] else 0
        }
        for row in result.fetchall()
    ]
    
    # Summary stats
    summary_sql = """
        SELECT 
            COUNT(DISTINCT p.id) as total_products,
            COUNT(DISTINCT li.order_id) as orders_with_products,
            SUM(li.quantity) as total_units_sold,
            SUM(li.price * li.quantity) as total_revenue,
            AVG(li.price * li.quantity) as avg_item_value
        FROM raw.shopify_order_line_items li
        JOIN raw.shopify_products p ON li.product_id = p.id
        JOIN raw.shopify_orders o ON li.order_id = o.id
        WHERE o.created_at >= :date_filter
    """
    
    result = await db.execute(text(summary_sql), {"date_filter": date_filter})
    row = result.fetchone()
    
    summary = {
        "total_products": row[0] or 0,
        "orders_with_products": row[1] or 0,
        "total_units_sold": row[2] or 0,
        "total_revenue": float(row[3]) if row[3] else 0,
        "avg_item_value": float(row[4]) if row[4] else 0,
        "period_days": days
    }
    
    return {
        "summary": summary,
        "top_products": top_products,
        "categories": categories
    }


@router.get("/products/trending")
async def get_trending_products(
    days: int = Query(7, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get trending products - fastest growing in recent period"""
    
    current_period = datetime.utcnow() - timedelta(days=days)
    previous_period = current_period - timedelta(days=days)
    
    sql = """
        WITH current_sales AS (
            SELECT 
                p.id,
                p.title,
                SUM(li.quantity) as units_sold,
                SUM(li.price * li.quantity) as revenue
            FROM raw.shopify_order_line_items li
            JOIN raw.shopify_products p ON li.product_id = p.id
            JOIN raw.shopify_orders o ON li.order_id = o.id
            WHERE o.created_at >= :current_start
            GROUP BY p.id, p.title
        ),
        previous_sales AS (
            SELECT 
                p.id,
                SUM(li.quantity) as units_sold,
                SUM(li.price * li.quantity) as revenue
            FROM raw.shopify_order_line_items li
            JOIN raw.shopify_products p ON li.product_id = p.id
            JOIN raw.shopify_orders o ON li.order_id = o.id
            WHERE o.created_at >= :prev_start AND o.created_at < :current_start
            GROUP BY p.id
        )
        SELECT 
            c.title,
            c.units_sold as current_units,
            c.revenue as current_revenue,
            COALESCE(p.units_sold, 0) as previous_units,
            COALESCE(p.revenue, 0) as previous_revenue,
            CASE WHEN COALESCE(p.revenue, 0) > 0 
                THEN ((c.revenue - p.revenue) / p.revenue * 100)
                ELSE 100 
            END as growth_pct
        FROM current_sales c
        LEFT JOIN previous_sales p ON c.id = p.id
        ORDER BY growth_pct DESC
        LIMIT 10
    """
    
    result = await db.execute(text(sql), {
        "current_start": current_period,
        "prev_start": previous_period
    })
    
    trending = [
        {
            "product_name": row[0],
            "current_units": row[1],
            "current_revenue": float(row[2]) if row[2] else 0,
            "previous_units": row[3],
            "previous_revenue": float(row[4]) if row[4] else 0,
            "growth_pct": float(row[5]) if row[5] else 0
        }
        for row in result.fetchall()
    ]
    
    return {"trending_products": trending, "period_days": days}


# ============================================
# CUSTOMERS ANALYTICS
# ============================================

@router.get("/customers")
async def get_customer_analytics(
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get customer analytics - metrics, segments, cohorts"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    
    # Customer summary
    summary_sql = """
        SELECT 
            COUNT(*) as total_customers,
            COUNT(CASE WHEN orders_count > 0 THEN 1 END) as customers_with_orders,
            AVG(orders_count) as avg_orders_per_customer,
            AVG(total_spent::numeric) as avg_lifetime_value,
            SUM(total_spent::numeric) as total_customer_value
        FROM raw.shopify_customers
    """
    
    result = await db.execute(text(summary_sql))
    row = result.fetchone()
    
    summary = {
        "total_customers": row[0] or 0,
        "customers_with_orders": row[1] or 0,
        "avg_orders_per_customer": float(row[2]) if row[2] else 0,
        "avg_lifetime_value": float(row[3]) if row[3] else 0,
        "total_customer_value": float(row[4]) if row[4] else 0
    }
    
    # Customer segments (by spending)
    segments_sql = """
        SELECT 
            CASE 
                WHEN total_spent::numeric >= 1000 THEN 'VIP'
                WHEN total_spent::numeric >= 500 THEN 'High Value'
                WHEN total_spent::numeric >= 100 THEN 'Regular'
                WHEN total_spent::numeric > 0 THEN 'Low Value'
                ELSE 'No Purchases'
            END as segment,
            COUNT(*) as customer_count,
            AVG(total_spent::numeric) as avg_spent,
            SUM(total_spent::numeric) as total_spent
        FROM raw.shopify_customers
        GROUP BY 1
        ORDER BY total_spent DESC
    """
    
    result = await db.execute(text(segments_sql))
    segments = [
        {
            "segment": row[0],
            "customer_count": row[1],
            "avg_spent": float(row[2]) if row[2] else 0,
            "total_spent": float(row[3]) if row[3] else 0
        }
        for row in result.fetchall()
    ]
    
    # Cohort analysis (by signup month)
    cohort_sql = """
        SELECT 
            TO_CHAR(created_at, 'YYYY-MM') as cohort_month,
            COUNT(*) as customers,
            AVG(orders_count) as avg_orders,
            AVG(total_spent::numeric) as avg_ltv
        FROM raw.shopify_customers
        WHERE created_at IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 12
    """
    
    result = await db.execute(text(cohort_sql))
    cohorts = [
        {
            "cohort_month": row[0],
            "customers": row[1],
            "avg_orders": float(row[2]) if row[2] else 0,
            "avg_ltv": float(row[3]) if row[3] else 0
        }
        for row in result.fetchall()
    ]
    
    # New vs Returning (based on order count)
    retention_sql = """
        SELECT 
            CASE 
                WHEN orders_count <= 1 THEN 'New'
                WHEN orders_count <= 3 THEN 'Returning'
                ELSE 'Loyal'
            END as customer_type,
            COUNT(*) as count,
            AVG(total_spent::numeric) as avg_spent
        FROM raw.shopify_customers
        GROUP BY 1
    """
    
    result = await db.execute(text(retention_sql))
    retention = [
        {
            "customer_type": row[0],
            "count": row[1],
            "avg_spent": float(row[2]) if row[2] else 0
        }
        for row in result.fetchall()
    ]
    
    # Top customers
    top_customers_sql = """
        SELECT 
            CONCAT(first_name, ' ', last_name) as name,
            email,
            orders_count,
            total_spent::numeric as total_spent,
            created_at
        FROM raw.shopify_customers
        ORDER BY total_spent::numeric DESC
        LIMIT 10
    """
    
    result = await db.execute(text(top_customers_sql))
    top_customers = [
        {
            "name": row[0] or "Unknown",
            "email": row[1],
            "orders_count": row[2],
            "total_spent": float(row[3]) if row[3] else 0,
            "customer_since": str(row[4])[:10] if row[4] else None
        }
        for row in result.fetchall()
    ]
    
    return {
        "summary": summary,
        "segments": segments,
        "cohorts": cohorts,
        "retention": retention,
        "top_customers": top_customers
    }


@router.get("/customers/acquisition")
async def get_customer_acquisition(
    days: int = Query(90, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get customer acquisition trends"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    
    sql = """
        SELECT 
            DATE(created_at) as signup_date,
            COUNT(*) as new_customers
        FROM raw.shopify_customers
        WHERE created_at >= :date_filter
        GROUP BY 1
        ORDER BY 1
    """
    
    result = await db.execute(text(sql), {"date_filter": date_filter})
    daily = [
        {
            "date": str(row[0]),
            "new_customers": row[1]
        }
        for row in result.fetchall()
    ]
    
    return {"daily_acquisition": daily, "period_days": days}


# ============================================
# LOCATIONS ANALYTICS
# ============================================

@router.get("/locations")
async def get_location_analytics(
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get location analytics - revenue and orders by region"""
    
    # Revenue by country from customer addresses
    country_sql = """
        SELECT 
            COALESCE(
                (default_address::jsonb->>'country'),
                'Unknown'
            ) as country,
            COUNT(*) as customer_count,
            SUM(total_spent::numeric) as total_revenue,
            AVG(total_spent::numeric) as avg_customer_value
        FROM raw.shopify_customers
        WHERE default_address IS NOT NULL
        GROUP BY 1
        ORDER BY total_revenue DESC
        LIMIT 20
    """
    
    try:
        result = await db.execute(text(country_sql))
        by_country = [
            {
                "country": row[0],
                "customer_count": row[1],
                "total_revenue": float(row[2]) if row[2] else 0,
                "avg_customer_value": float(row[3]) if row[3] else 0
            }
            for row in result.fetchall()
        ]
    except:
        by_country = []
    
    # Revenue by city
    city_sql = """
        SELECT 
            COALESCE(
                (default_address::jsonb->>'city'),
                'Unknown'
            ) as city,
            COALESCE(
                (default_address::jsonb->>'country'),
                'Unknown'
            ) as country,
            COUNT(*) as customer_count,
            SUM(total_spent::numeric) as total_revenue
        FROM raw.shopify_customers
        WHERE default_address IS NOT NULL
        GROUP BY 1, 2
        ORDER BY total_revenue DESC
        LIMIT 20
    """
    
    try:
        result = await db.execute(text(city_sql))
        by_city = [
            {
                "city": row[0],
                "country": row[1],
                "customer_count": row[2],
                "total_revenue": float(row[3]) if row[3] else 0
            }
            for row in result.fetchall()
        ]
    except:
        by_city = []
    
    # Summary
    summary = {
        "total_countries": len(by_country),
        "total_cities": len(by_city),
        "top_country": by_country[0]["country"] if by_country else "N/A",
        "top_city": by_city[0]["city"] if by_city else "N/A"
    }
    
    return {
        "summary": summary,
        "by_country": by_country,
        "by_city": by_city
    }


# ============================================
# PROFITABILITY ANALYTICS
# ============================================

@router.get("/profitability")
async def get_profitability_analytics(
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get profitability analytics - revenue breakdown, margins (partial data)"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    
    # Revenue breakdown from orders
    revenue_sql = """
        SELECT 
            SUM(total_price) as gross_revenue,
            SUM(subtotal_price) as subtotal,
            SUM(total_tax) as total_tax,
            SUM(total_discounts) as total_discounts,
            SUM(total_price - total_discounts) as net_revenue,
            COUNT(*) as total_orders,
            AVG(total_price) as avg_order_value
        FROM raw.shopify_orders
        WHERE created_at >= :date_filter
        AND cancelled_at IS NULL
    """
    
    result = await db.execute(text(revenue_sql), {"date_filter": date_filter})
    row = result.fetchone()
    
    revenue_summary = {
        "gross_revenue": float(row[0]) if row[0] else 0,
        "subtotal": float(row[1]) if row[1] else 0,
        "total_tax": float(row[2]) if row[2] else 0,
        "total_discounts": float(row[3]) if row[3] else 0,
        "net_revenue": float(row[4]) if row[4] else 0,
        "total_orders": row[5] or 0,
        "avg_order_value": float(row[6]) if row[6] else 0
    }
    
    # Calculate estimated metrics (note: COGS not available)
    revenue_summary["discount_rate"] = (
        (revenue_summary["total_discounts"] / revenue_summary["gross_revenue"] * 100)
        if revenue_summary["gross_revenue"] > 0 else 0
    )
    revenue_summary["tax_rate"] = (
        (revenue_summary["total_tax"] / revenue_summary["subtotal"] * 100)
        if revenue_summary["subtotal"] > 0 else 0
    )
    
    # Daily breakdown
    daily_sql = """
        SELECT 
            DATE(created_at) as order_date,
            SUM(total_price) as gross_revenue,
            SUM(total_discounts) as discounts,
            SUM(total_price - total_discounts) as net_revenue,
            COUNT(*) as orders
        FROM raw.shopify_orders
        WHERE created_at >= :date_filter
        AND cancelled_at IS NULL
        GROUP BY 1
        ORDER BY 1
    """
    
    result = await db.execute(text(daily_sql), {"date_filter": date_filter})
    daily = [
        {
            "date": str(row[0]),
            "gross_revenue": float(row[1]) if row[1] else 0,
            "discounts": float(row[2]) if row[2] else 0,
            "net_revenue": float(row[3]) if row[3] else 0,
            "orders": row[4]
        }
        for row in result.fetchall()
    ]
    
    # By platform (all platforms)
    platform_sql = """
        SELECT 
            'shopify' as platform,
            SUM(total_price) as gross_revenue,
            SUM(total_discounts) as discounts,
            COUNT(*) as orders
        FROM raw.shopify_orders
        WHERE created_at >= :date_filter AND cancelled_at IS NULL
        UNION ALL
        SELECT 
            'amazon' as platform,
            SUM((order_total::jsonb->>'Amount')::numeric) as gross_revenue,
            0 as discounts,
            COUNT(*) as orders
        FROM raw.amazon_orders
        WHERE purchase_date >= :date_filter
        UNION ALL
        SELECT 
            'lazada' as platform,
            SUM(price::numeric) as gross_revenue,
            SUM(COALESCE(voucher::numeric, 0)) as discounts,
            COUNT(*) as orders
        FROM raw.lazada_orders
        WHERE created_at >= :date_filter
        UNION ALL
        SELECT 
            'shopee' as platform,
            SUM(total_amount::numeric) as gross_revenue,
            SUM(COALESCE(voucher_absorbed::numeric, 0)) as discounts,
            COUNT(*) as orders
        FROM raw.shopee_orders
        WHERE create_time >= :date_filter
    """
    
    result = await db.execute(text(platform_sql), {"date_filter": date_filter})
    by_platform = [
        {
            "platform": row[0],
            "gross_revenue": float(row[1]) if row[1] else 0,
            "discounts": float(row[2]) if row[2] else 0,
            "orders": row[3] or 0
        }
        for row in result.fetchall()
    ]
    
    return {
        "summary": revenue_summary,
        "daily": daily,
        "by_platform": by_platform,
        "period_days": days,
        "note": "COGS and shipping costs not available - showing revenue metrics only"
    }


@router.get("/profitability/comparison")
async def get_profitability_comparison(
    db: AsyncSession = Depends(get_db)
):
    """Compare profitability metrics across time periods"""
    
    today = datetime.utcnow().date()
    
    periods = {
        "today": (today, today + timedelta(days=1)),
        "yesterday": (today - timedelta(days=1), today),
        "this_week": (today - timedelta(days=today.weekday()), today + timedelta(days=1)),
        "last_week": (today - timedelta(days=today.weekday() + 7), today - timedelta(days=today.weekday())),
        "this_month": (today.replace(day=1), today + timedelta(days=1)),
        "last_month": ((today.replace(day=1) - timedelta(days=1)).replace(day=1), today.replace(day=1))
    }
    
    results = {}
    
    for period_name, (start, end) in periods.items():
        sql = """
            SELECT 
                COALESCE(SUM(total_price), 0) as revenue,
                COALESCE(SUM(total_discounts), 0) as discounts,
                COUNT(*) as orders
            FROM raw.shopify_orders
            WHERE DATE(created_at) >= :start 
            AND DATE(created_at) < :end
            AND cancelled_at IS NULL
        """
        
        result = await db.execute(text(sql), {"start": start, "end": end})
        row = result.fetchone()
        
        results[period_name] = {
            "revenue": float(row[0]) if row[0] else 0,
            "discounts": float(row[1]) if row[1] else 0,
            "orders": row[2] or 0
        }
    
    return {"periods": results}

