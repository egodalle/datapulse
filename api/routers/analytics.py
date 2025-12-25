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

# Valid platforms
VALID_PLATFORMS = ["shopify", "amazon", "lazada", "shopee"]


# ============================================
# PRODUCTS ANALYTICS
# ============================================

@router.get("/products")
async def get_product_analytics(
    platform: Optional[str] = Query(None, description="Filter by platform (shopify, amazon, lazada, shopee)"),
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get product analytics - top products, sales by product, category performance"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    
    # If platform specified, only query that platform
    if platform and platform.lower() in VALID_PLATFORMS:
        platform = platform.lower()
        
        if platform == "shopify":
            return await get_shopify_products(db, date_filter, days)
        elif platform == "amazon":
            return await get_amazon_products(db, date_filter, days)
        elif platform == "lazada":
            return await get_lazada_products(db, date_filter, days)
        elif platform == "shopee":
            return await get_shopee_products(db, date_filter, days)
    
    # Default: return Shopify data (most detailed product info)
    return await get_shopify_products(db, date_filter, days)


async def get_shopify_products(db: AsyncSession, date_filter: datetime, days: int):
    """Get Shopify product analytics"""
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
            "avg_price": float(row[6]) if row[6] else 0,
            "platform": "shopify"
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
        "period_days": days,
        "platform": "shopify"
    }
    
    return {
        "summary": summary,
        "top_products": top_products,
        "categories": categories,
        "platform": "shopify"
    }


async def get_amazon_products(db: AsyncSession, date_filter: datetime, days: int):
    """Get Amazon product analytics"""
    try:
        # Amazon order items
        sql = """
            SELECT 
                li.title as product_name,
                'Amazon' as category,
                li.sku,
                COUNT(DISTINCT li.order_id) as total_orders,
                SUM(li.quantity_ordered) as units_sold,
                SUM(li.item_price::numeric) as total_revenue,
                AVG(li.item_price::numeric) as avg_price
            FROM raw.amazon_order_items li
            JOIN raw.amazon_orders o ON li.order_id = o.amazon_order_id
            WHERE o.purchase_date >= :date_filter
            GROUP BY li.title, li.sku
            ORDER BY total_revenue DESC
            LIMIT 20
        """
        result = await db.execute(text(sql), {"date_filter": date_filter})
        top_products = [
            {
                "product_name": row[0] or "Unknown",
                "category": row[1],
                "vendor": row[2] or "Unknown",
                "total_orders": row[3],
                "units_sold": row[4] or 0,
                "total_revenue": float(row[5]) if row[5] else 0,
                "avg_price": float(row[6]) if row[6] else 0,
                "platform": "amazon"
            }
            for row in result.fetchall()
        ]
    except:
        top_products = []
    
    summary = {
        "total_products": len(top_products),
        "orders_with_products": sum(p["total_orders"] for p in top_products),
        "total_units_sold": sum(p["units_sold"] for p in top_products),
        "total_revenue": sum(p["total_revenue"] for p in top_products),
        "avg_item_value": sum(p["total_revenue"] for p in top_products) / max(len(top_products), 1),
        "period_days": days,
        "platform": "amazon"
    }
    
    return {
        "summary": summary,
        "top_products": top_products,
        "categories": [{"category": "Amazon Products", "product_count": len(top_products), "units_sold": summary["total_units_sold"], "total_revenue": summary["total_revenue"]}],
        "platform": "amazon"
    }


async def get_lazada_products(db: AsyncSession, date_filter: datetime, days: int):
    """Get Lazada product analytics"""
    try:
        sql = """
            SELECT 
                li.name as product_name,
                'Lazada' as category,
                li.sku,
                COUNT(DISTINCT li.order_id) as total_orders,
                SUM(li.quantity) as units_sold,
                SUM(li.paid_price::numeric) as total_revenue,
                AVG(li.paid_price::numeric) as avg_price
            FROM raw.lazada_order_items li
            JOIN raw.lazada_orders o ON li.order_id = o.order_id
            WHERE o.created_at >= :date_filter
            GROUP BY li.name, li.sku
            ORDER BY total_revenue DESC
            LIMIT 20
        """
        result = await db.execute(text(sql), {"date_filter": date_filter})
        top_products = [
            {
                "product_name": row[0] or "Unknown",
                "category": row[1],
                "vendor": row[2] or "Unknown",
                "total_orders": row[3],
                "units_sold": row[4] or 0,
                "total_revenue": float(row[5]) if row[5] else 0,
                "avg_price": float(row[6]) if row[6] else 0,
                "platform": "lazada"
            }
            for row in result.fetchall()
        ]
    except:
        top_products = []
    
    summary = {
        "total_products": len(top_products),
        "orders_with_products": sum(p["total_orders"] for p in top_products),
        "total_units_sold": sum(p["units_sold"] for p in top_products),
        "total_revenue": sum(p["total_revenue"] for p in top_products),
        "avg_item_value": sum(p["total_revenue"] for p in top_products) / max(len(top_products), 1),
        "period_days": days,
        "platform": "lazada"
    }
    
    return {
        "summary": summary,
        "top_products": top_products,
        "categories": [{"category": "Lazada Products", "product_count": len(top_products), "units_sold": summary["total_units_sold"], "total_revenue": summary["total_revenue"]}],
        "platform": "lazada"
    }


async def get_shopee_products(db: AsyncSession, date_filter: datetime, days: int):
    """Get Shopee product analytics"""
    try:
        sql = """
            SELECT 
                li.item_name as product_name,
                'Shopee' as category,
                li.item_sku,
                COUNT(DISTINCT li.order_sn) as total_orders,
                SUM(li.model_quantity_purchased) as units_sold,
                SUM(li.model_discounted_price::numeric * li.model_quantity_purchased) as total_revenue,
                AVG(li.model_discounted_price::numeric) as avg_price
            FROM raw.shopee_order_items li
            JOIN raw.shopee_orders o ON li.order_sn = o.order_sn
            WHERE o.create_time >= :date_filter
            GROUP BY li.item_name, li.item_sku
            ORDER BY total_revenue DESC
            LIMIT 20
        """
        result = await db.execute(text(sql), {"date_filter": date_filter})
        top_products = [
            {
                "product_name": row[0] or "Unknown",
                "category": row[1],
                "vendor": row[2] or "Unknown",
                "total_orders": row[3],
                "units_sold": row[4] or 0,
                "total_revenue": float(row[5]) if row[5] else 0,
                "avg_price": float(row[6]) if row[6] else 0,
                "platform": "shopee"
            }
            for row in result.fetchall()
        ]
    except:
        top_products = []
    
    summary = {
        "total_products": len(top_products),
        "orders_with_products": sum(p["total_orders"] for p in top_products),
        "total_units_sold": sum(p["units_sold"] for p in top_products),
        "total_revenue": sum(p["total_revenue"] for p in top_products),
        "avg_item_value": sum(p["total_revenue"] for p in top_products) / max(len(top_products), 1),
        "period_days": days,
        "platform": "shopee"
    }
    
    return {
        "summary": summary,
        "top_products": top_products,
        "categories": [{"category": "Shopee Products", "product_count": len(top_products), "units_sold": summary["total_units_sold"], "total_revenue": summary["total_revenue"]}],
        "platform": "shopee"
    }


@router.get("/products/trending")
async def get_trending_products(
    platform: Optional[str] = Query(None, description="Filter by platform"),
    days: int = Query(7, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get trending products - fastest growing in recent period"""
    
    current_period = datetime.utcnow() - timedelta(days=days)
    previous_period = current_period - timedelta(days=days)
    
    # Currently only Shopify has detailed product data for trending analysis
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
            "growth_pct": float(row[5]) if row[5] else 0,
            "platform": platform or "shopify"
        }
        for row in result.fetchall()
    ]
    
    return {"trending_products": trending, "period_days": days, "platform": platform or "shopify"}


# ============================================
# CUSTOMERS ANALYTICS
# ============================================

@router.get("/customers")
async def get_customer_analytics(
    platform: Optional[str] = Query(None, description="Filter by platform (shopify, amazon, lazada, shopee)"),
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get customer analytics - metrics, segments, cohorts"""
    
    # Platform-specific customer queries
    if platform and platform.lower() in VALID_PLATFORMS:
        platform = platform.lower()
        return await get_platform_customers(db, platform, days)
    
    # Default: Shopify customers (most detailed)
    return await get_platform_customers(db, "shopify", days)


async def get_platform_customers(db: AsyncSession, platform: str, days: int):
    """Get customer analytics for a specific platform"""
    
    if platform == "shopify":
        # Shopify has dedicated customer table
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
            "total_customer_value": float(row[4]) if row[4] else 0,
            "platform": platform
        }
        
        # Segments
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
        
        # Cohorts
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
        
        # Retention
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
        
    else:
        # For other platforms, derive customer data from orders
        if platform == "amazon":
            table = "raw.amazon_orders"
            email_col = "buyer_email"
            date_col = "purchase_date"
            amount_col = "(order_total::jsonb->>'Amount')::numeric"
        elif platform == "lazada":
            table = "raw.lazada_orders"
            email_col = "buyer_email"
            date_col = "created_at"
            amount_col = "price::numeric"
        else:  # shopee
            table = "raw.shopee_orders"
            email_col = "buyer_username"
            date_col = "create_time"
            amount_col = "total_amount::numeric"
        
        summary_sql = f"""
            SELECT 
                COUNT(DISTINCT {email_col}) as total_customers,
                COUNT(DISTINCT {email_col}) as customers_with_orders,
                COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT {email_col}), 0) as avg_orders,
                SUM({amount_col}) / NULLIF(COUNT(DISTINCT {email_col}), 0) as avg_ltv,
                SUM({amount_col}) as total_value
            FROM {table}
        """
        
        result = await db.execute(text(summary_sql))
        row = result.fetchone()
        
        summary = {
            "total_customers": row[0] or 0,
            "customers_with_orders": row[1] or 0,
            "avg_orders_per_customer": float(row[2]) if row[2] else 0,
            "avg_lifetime_value": float(row[3]) if row[3] else 0,
            "total_customer_value": float(row[4]) if row[4] else 0,
            "platform": platform
        }
        
        # Top customers for other platforms
        top_sql = f"""
            SELECT 
                {email_col} as customer,
                COUNT(*) as orders,
                SUM({amount_col}) as total_spent,
                MIN({date_col}) as first_order
            FROM {table}
            GROUP BY {email_col}
            ORDER BY total_spent DESC
            LIMIT 10
        """
        
        result = await db.execute(text(top_sql))
        top_customers = [
            {
                "name": row[0] or "Unknown",
                "email": row[0],
                "orders_count": row[1],
                "total_spent": float(row[2]) if row[2] else 0,
                "customer_since": str(row[3])[:10] if row[3] else None
            }
            for row in result.fetchall()
        ]
        
        segments = []
        cohorts = []
        retention = []
    
    return {
        "summary": summary,
        "segments": segments,
        "cohorts": cohorts,
        "retention": retention,
        "top_customers": top_customers,
        "platform": platform
    }


@router.get("/customers/acquisition")
async def get_customer_acquisition(
    platform: Optional[str] = Query(None, description="Filter by platform"),
    days: int = Query(90, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get customer acquisition trends"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    platform = (platform or "shopify").lower()
    
    if platform == "shopify":
        sql = """
            SELECT DATE(created_at) as signup_date, COUNT(*) as new_customers
            FROM raw.shopify_customers
            WHERE created_at >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    elif platform == "amazon":
        sql = """
            SELECT DATE(purchase_date) as signup_date, COUNT(DISTINCT buyer_email) as new_customers
            FROM raw.amazon_orders
            WHERE purchase_date >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    elif platform == "lazada":
        sql = """
            SELECT DATE(created_at) as signup_date, COUNT(DISTINCT buyer_email) as new_customers
            FROM raw.lazada_orders
            WHERE created_at >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    else:  # shopee
        sql = """
            SELECT DATE(create_time) as signup_date, COUNT(DISTINCT buyer_username) as new_customers
            FROM raw.shopee_orders
            WHERE create_time >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    
    result = await db.execute(text(sql), {"date_filter": date_filter})
    daily = [
        {"date": str(row[0]), "new_customers": row[1]}
        for row in result.fetchall()
    ]
    
    return {"daily_acquisition": daily, "period_days": days, "platform": platform}


# ============================================
# LOCATIONS ANALYTICS
# ============================================

@router.get("/locations")
async def get_location_analytics(
    platform: Optional[str] = Query(None, description="Filter by platform"),
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get location analytics - revenue and orders by region"""
    
    platform = (platform or "shopify").lower()
    
    if platform == "shopify":
        # Revenue by country from customer addresses
        country_sql = """
            SELECT 
                COALESCE((default_address::jsonb->>'country'), 'Unknown') as country,
                COUNT(*) as customer_count,
                SUM(total_spent::numeric) as total_revenue,
                AVG(total_spent::numeric) as avg_customer_value
            FROM raw.shopify_customers
            WHERE default_address IS NOT NULL
            GROUP BY 1
            ORDER BY total_revenue DESC
            LIMIT 20
        """
        
        city_sql = """
            SELECT 
                COALESCE((default_address::jsonb->>'city'), 'Unknown') as city,
                COALESCE((default_address::jsonb->>'country'), 'Unknown') as country,
                COUNT(*) as customer_count,
                SUM(total_spent::numeric) as total_revenue
            FROM raw.shopify_customers
            WHERE default_address IS NOT NULL
            GROUP BY 1, 2
            ORDER BY total_revenue DESC
            LIMIT 20
        """
    else:
        # For other platforms, use shipping address from orders
        if platform == "amazon":
            country_sql = """
                SELECT 
                    COALESCE((shipping_address::jsonb->>'CountryCode'), 'Unknown') as country,
                    COUNT(*) as customer_count,
                    SUM((order_total::jsonb->>'Amount')::numeric) as total_revenue,
                    AVG((order_total::jsonb->>'Amount')::numeric) as avg_customer_value
                FROM raw.amazon_orders
                WHERE shipping_address IS NOT NULL
                GROUP BY 1
                ORDER BY total_revenue DESC
                LIMIT 20
            """
            city_sql = """
                SELECT 
                    COALESCE((shipping_address::jsonb->>'City'), 'Unknown') as city,
                    COALESCE((shipping_address::jsonb->>'CountryCode'), 'Unknown') as country,
                    COUNT(*) as customer_count,
                    SUM((order_total::jsonb->>'Amount')::numeric) as total_revenue
                FROM raw.amazon_orders
                WHERE shipping_address IS NOT NULL
                GROUP BY 1, 2
                ORDER BY total_revenue DESC
                LIMIT 20
            """
        elif platform == "lazada":
            country_sql = """
                SELECT 'Southeast Asia' as country, COUNT(*) as customer_count,
                    SUM(price::numeric) as total_revenue, AVG(price::numeric) as avg_customer_value
                FROM raw.lazada_orders
            """
            city_sql = """
                SELECT 'Various' as city, 'Southeast Asia' as country,
                    COUNT(*) as customer_count, SUM(price::numeric) as total_revenue
                FROM raw.lazada_orders
            """
        else:  # shopee
            country_sql = """
                SELECT 'Southeast Asia' as country, COUNT(*) as customer_count,
                    SUM(total_amount::numeric) as total_revenue, AVG(total_amount::numeric) as avg_customer_value
                FROM raw.shopee_orders
            """
            city_sql = """
                SELECT 'Various' as city, 'Southeast Asia' as country,
                    COUNT(*) as customer_count, SUM(total_amount::numeric) as total_revenue
                FROM raw.shopee_orders
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
    
    summary = {
        "total_countries": len(by_country),
        "total_cities": len(by_city),
        "top_country": by_country[0]["country"] if by_country else "N/A",
        "top_city": by_city[0]["city"] if by_city else "N/A",
        "platform": platform
    }
    
    return {
        "summary": summary,
        "by_country": by_country,
        "by_city": by_city,
        "platform": platform
    }


# ============================================
# PROFITABILITY ANALYTICS
# ============================================

@router.get("/profitability")
async def get_profitability_analytics(
    platform: Optional[str] = Query(None, description="Filter by platform (shopify, amazon, lazada, shopee)"),
    days: int = Query(30, description="Number of days to analyze"),
    db: AsyncSession = Depends(get_db)
):
    """Get profitability analytics - revenue breakdown, margins (partial data)"""
    
    date_filter = datetime.utcnow() - timedelta(days=days)
    
    # If specific platform requested
    if platform and platform.lower() in VALID_PLATFORMS:
        platform = platform.lower()
        return await get_platform_profitability(db, platform, date_filter, days)
    
    # All platforms summary
    by_platform = []
    
    # Shopify
    try:
        result = await db.execute(text(
            "SELECT COALESCE(SUM(total_price), 0), COALESCE(SUM(total_discounts), 0), COUNT(*) FROM raw.shopify_orders WHERE cancelled_at IS NULL"
        ))
        row = result.fetchone()
        by_platform.append({
            "platform": "shopify",
            "gross_revenue": float(row[0]) if row[0] else 0,
            "discounts": float(row[1]) if row[1] else 0,
            "orders": row[2] or 0
        })
    except:
        by_platform.append({"platform": "shopify", "gross_revenue": 0, "discounts": 0, "orders": 0})
    
    # Amazon
    try:
        result = await db.execute(text(
            "SELECT COALESCE(SUM((order_total::jsonb->>'Amount')::numeric), 0), COUNT(*) FROM raw.amazon_orders"
        ))
        row = result.fetchone()
        by_platform.append({
            "platform": "amazon",
            "gross_revenue": float(row[0]) if row[0] else 0,
            "discounts": 0,
            "orders": row[1] or 0
        })
    except:
        by_platform.append({"platform": "amazon", "gross_revenue": 0, "discounts": 0, "orders": 0})
    
    # Lazada
    try:
        result = await db.execute(text(
            "SELECT COALESCE(SUM(price::numeric), 0), COALESCE(SUM(voucher::numeric), 0), COUNT(*) FROM raw.lazada_orders"
        ))
        row = result.fetchone()
        by_platform.append({
            "platform": "lazada",
            "gross_revenue": float(row[0]) if row[0] else 0,
            "discounts": float(row[1]) if row[1] else 0,
            "orders": row[2] or 0
        })
    except:
        by_platform.append({"platform": "lazada", "gross_revenue": 0, "discounts": 0, "orders": 0})
    
    # Shopee
    try:
        result = await db.execute(text(
            "SELECT COALESCE(SUM(total_amount::numeric), 0), COALESCE(SUM(voucher_absorbed::numeric), 0), COUNT(*) FROM raw.shopee_orders"
        ))
        row = result.fetchone()
        by_platform.append({
            "platform": "shopee",
            "gross_revenue": float(row[0]) if row[0] else 0,
            "discounts": float(row[1]) if row[1] else 0,
            "orders": row[2] or 0
        })
    except:
        by_platform.append({"platform": "shopee", "gross_revenue": 0, "discounts": 0, "orders": 0})
    
    # Calculate totals
    total_revenue = sum(p["gross_revenue"] for p in by_platform)
    total_discounts = sum(p["discounts"] for p in by_platform)
    total_orders = sum(p["orders"] for p in by_platform)
    
    summary = {
        "gross_revenue": total_revenue,
        "total_discounts": total_discounts,
        "net_revenue": total_revenue - total_discounts,
        "total_orders": total_orders,
        "avg_order_value": total_revenue / total_orders if total_orders > 0 else 0,
        "discount_rate": (total_discounts / total_revenue * 100) if total_revenue > 0 else 0
    }
    
    return {
        "summary": summary,
        "by_platform": by_platform,
        "period_days": days,
        "platform": "all",
        "note": "COGS and shipping costs not available - showing revenue metrics only"
    }


async def get_platform_profitability(db: AsyncSession, platform: str, date_filter: datetime, days: int):
    """Get profitability for a specific platform"""
    
    if platform == "shopify":
        sql = """
            SELECT 
                COALESCE(SUM(total_price), 0) as gross_revenue,
                COALESCE(SUM(subtotal_price), 0) as subtotal,
                COALESCE(SUM(total_tax), 0) as total_tax,
                COALESCE(SUM(total_discounts), 0) as total_discounts,
                COUNT(*) as total_orders,
                COALESCE(AVG(total_price), 0) as avg_order_value
            FROM raw.shopify_orders
            WHERE created_at >= :date_filter AND cancelled_at IS NULL
        """
        daily_sql = """
            SELECT DATE(created_at), COALESCE(SUM(total_price), 0), COALESCE(SUM(total_discounts), 0), COUNT(*)
            FROM raw.shopify_orders
            WHERE created_at >= :date_filter AND cancelled_at IS NULL
            GROUP BY 1 ORDER BY 1
        """
    elif platform == "amazon":
        sql = """
            SELECT 
                COALESCE(SUM((order_total::jsonb->>'Amount')::numeric), 0) as gross_revenue,
                COALESCE(SUM((order_total::jsonb->>'Amount')::numeric), 0) as subtotal,
                0 as total_tax,
                0 as total_discounts,
                COUNT(*) as total_orders,
                COALESCE(AVG((order_total::jsonb->>'Amount')::numeric), 0) as avg_order_value
            FROM raw.amazon_orders
            WHERE purchase_date >= :date_filter
        """
        daily_sql = """
            SELECT DATE(purchase_date), COALESCE(SUM((order_total::jsonb->>'Amount')::numeric), 0), 0, COUNT(*)
            FROM raw.amazon_orders
            WHERE purchase_date >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    elif platform == "lazada":
        sql = """
            SELECT 
                COALESCE(SUM(price::numeric), 0) as gross_revenue,
                COALESCE(SUM(price::numeric), 0) as subtotal,
                0 as total_tax,
                COALESCE(SUM(voucher::numeric), 0) as total_discounts,
                COUNT(*) as total_orders,
                COALESCE(AVG(price::numeric), 0) as avg_order_value
            FROM raw.lazada_orders
            WHERE created_at >= :date_filter
        """
        daily_sql = """
            SELECT DATE(created_at), COALESCE(SUM(price::numeric), 0), COALESCE(SUM(voucher::numeric), 0), COUNT(*)
            FROM raw.lazada_orders
            WHERE created_at >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    else:  # shopee
        sql = """
            SELECT 
                COALESCE(SUM(total_amount::numeric), 0) as gross_revenue,
                COALESCE(SUM(total_amount::numeric), 0) as subtotal,
                0 as total_tax,
                COALESCE(SUM(voucher_absorbed::numeric), 0) as total_discounts,
                COUNT(*) as total_orders,
                COALESCE(AVG(total_amount::numeric), 0) as avg_order_value
            FROM raw.shopee_orders
            WHERE create_time >= :date_filter
        """
        daily_sql = """
            SELECT DATE(create_time), COALESCE(SUM(total_amount::numeric), 0), COALESCE(SUM(voucher_absorbed::numeric), 0), COUNT(*)
            FROM raw.shopee_orders
            WHERE create_time >= :date_filter
            GROUP BY 1 ORDER BY 1
        """
    
    result = await db.execute(text(sql), {"date_filter": date_filter})
    row = result.fetchone()
    
    summary = {
        "gross_revenue": float(row[0]) if row[0] else 0,
        "subtotal": float(row[1]) if row[1] else 0,
        "total_tax": float(row[2]) if row[2] else 0,
        "total_discounts": float(row[3]) if row[3] else 0,
        "net_revenue": float(row[0]) - float(row[3]) if row[0] and row[3] else 0,
        "total_orders": row[4] or 0,
        "avg_order_value": float(row[5]) if row[5] else 0,
        "discount_rate": (float(row[3]) / float(row[0]) * 100) if row[0] and float(row[0]) > 0 else 0
    }
    
    result = await db.execute(text(daily_sql), {"date_filter": date_filter})
    daily = [
        {
            "date": str(row[0]),
            "gross_revenue": float(row[1]) if row[1] else 0,
            "discounts": float(row[2]) if row[2] else 0,
            "orders": row[3]
        }
        for row in result.fetchall()
    ]
    
    return {
        "summary": summary,
        "daily": daily,
        "by_platform": [{"platform": platform, "gross_revenue": summary["gross_revenue"], "discounts": summary["total_discounts"], "orders": summary["total_orders"]}],
        "period_days": days,
        "platform": platform,
        "note": "COGS and shipping costs not available - showing revenue metrics only"
    }


@router.get("/profitability/comparison")
async def get_profitability_comparison(
    platform: Optional[str] = Query(None, description="Filter by platform"),
    db: AsyncSession = Depends(get_db)
):
    """Compare profitability metrics across time periods"""
    
    today = datetime.utcnow().date()
    platform = (platform or "shopify").lower()
    
    periods = {
        "today": (today, today + timedelta(days=1)),
        "yesterday": (today - timedelta(days=1), today),
        "this_week": (today - timedelta(days=today.weekday()), today + timedelta(days=1)),
        "last_week": (today - timedelta(days=today.weekday() + 7), today - timedelta(days=today.weekday())),
        "this_month": (today.replace(day=1), today + timedelta(days=1)),
        "last_month": ((today.replace(day=1) - timedelta(days=1)).replace(day=1), today.replace(day=1))
    }
    
    # Platform-specific query
    if platform == "shopify":
        base_sql = """
            SELECT COALESCE(SUM(total_price), 0), COALESCE(SUM(total_discounts), 0), COUNT(*)
            FROM raw.shopify_orders
            WHERE DATE(created_at) >= :start AND DATE(created_at) < :end AND cancelled_at IS NULL
        """
    elif platform == "amazon":
        base_sql = """
            SELECT COALESCE(SUM((order_total::jsonb->>'Amount')::numeric), 0), 0, COUNT(*)
            FROM raw.amazon_orders
            WHERE DATE(purchase_date) >= :start AND DATE(purchase_date) < :end
        """
    elif platform == "lazada":
        base_sql = """
            SELECT COALESCE(SUM(price::numeric), 0), COALESCE(SUM(voucher::numeric), 0), COUNT(*)
            FROM raw.lazada_orders
            WHERE DATE(created_at) >= :start AND DATE(created_at) < :end
        """
    else:  # shopee
        base_sql = """
            SELECT COALESCE(SUM(total_amount::numeric), 0), COALESCE(SUM(voucher_absorbed::numeric), 0), COUNT(*)
            FROM raw.shopee_orders
            WHERE DATE(create_time) >= :start AND DATE(create_time) < :end
        """
    
    results = {}
    
    for period_name, (start, end) in periods.items():
        result = await db.execute(text(base_sql), {"start": start, "end": end})
        row = result.fetchone()
        
        results[period_name] = {
            "revenue": float(row[0]) if row[0] else 0,
            "discounts": float(row[1]) if row[1] else 0,
            "orders": row[2] or 0
        }
    
    return {"periods": results, "platform": platform}
