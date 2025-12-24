"""
KPI Service - Business logic for fetching KPIs from the data warehouse
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from datetime import date, timedelta

class KPIService:
    """Service for fetching KPIs from dbt mart tables"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_platform_overview(self) -> List[dict]:
        """Get overview metrics for all platforms"""
        query = text("""
            SELECT * FROM public_marts.kpi_platform_overview
            ORDER BY total_revenue_usd DESC
        """)
        result = await self.db.execute(query)
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def get_daily_snapshots(
        self, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 30
    ) -> List[dict]:
        """Get daily snapshot metrics"""
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = end_date - timedelta(days=limit)
        
        query = text("""
            SELECT * FROM public_marts.kpi_daily_snapshot
            WHERE order_date BETWEEN :start_date AND :end_date
            ORDER BY order_date DESC
            LIMIT :limit
        """)
        result = await self.db.execute(
            query, 
            {"start_date": start_date, "end_date": end_date, "limit": limit}
        )
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def get_revenue_by_platform(
        self,
        platform: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[dict]:
        """Get revenue metrics broken down by platform"""
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        base_query = """
            SELECT * FROM public_marts.kpi_revenue_summary
            WHERE order_date BETWEEN :start_date AND :end_date
        """
        
        if platform:
            base_query += " AND platform = :platform"
        
        base_query += " ORDER BY order_date DESC, platform"
        
        params = {"start_date": start_date, "end_date": end_date}
        if platform:
            params["platform"] = platform
        
        result = await self.db.execute(text(base_query), params)
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def get_product_performance(
        self,
        platform: Optional[str] = None,
        tier: Optional[str] = None,
        limit: int = 50,
    ) -> List[dict]:
        """Get product performance metrics"""
        base_query = """
            SELECT * FROM public_marts.kpi_product_performance
            WHERE 1=1
        """
        
        params = {"limit": limit}
        
        if platform:
            base_query += " AND platform = :platform"
            params["platform"] = platform
        
        if tier:
            base_query += " AND performance_tier = :tier"
            params["tier"] = tier
        
        base_query += " ORDER BY total_revenue DESC LIMIT :limit"
        
        result = await self.db.execute(text(base_query), params)
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def get_dashboard_summary(self) -> dict:
        """Get complete dashboard summary"""
        # Get platform overview
        platforms = await self.get_platform_overview()
        
        # Get recent daily snapshots
        recent_days = await self.get_daily_snapshots(limit=7)
        
        # Calculate totals
        total_revenue = sum(p.get("total_revenue_usd", 0) or 0 for p in platforms)
        total_orders = sum(p.get("total_orders", 0) or 0 for p in platforms)
        revenue_this_month = sum(p.get("revenue_this_month_usd", 0) or 0 for p in platforms)
        revenue_last_month = sum(p.get("revenue_last_month_usd", 0) or 0 for p in platforms)
        orders_this_month = sum(p.get("orders_this_month", 0) or 0 for p in platforms)
        orders_last_month = sum(p.get("orders_last_month", 0) or 0 for p in platforms)
        
        # Calculate growth
        revenue_growth = 0
        if revenue_last_month > 0:
            revenue_growth = round(
                ((revenue_this_month - revenue_last_month) / revenue_last_month) * 100, 2
            )
        
        orders_growth = 0
        if orders_last_month > 0:
            orders_growth = round(
                ((orders_this_month - orders_last_month) / orders_last_month) * 100, 2
            )
        
        # Average order value
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        
        return {
            "total_revenue_usd": round(total_revenue, 2),
            "total_orders": total_orders,
            "avg_order_value_usd": round(avg_order_value, 2),
            "revenue_growth_pct": revenue_growth,
            "orders_growth_pct": orders_growth,
            "total_customers": 0,  # Would need customer data
            "platforms": platforms,
            "recent_days": recent_days,
        }

