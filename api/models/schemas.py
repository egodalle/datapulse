"""
Pydantic schemas for API responses
"""
from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional, List
from enum import Enum

class Platform(str, Enum):
    SHOPIFY = "shopify"
    AMAZON = "amazon"
    LAZADA = "lazada"
    SHOPEE = "shopee"

# ============== KPI Responses ==============

class PlatformOverview(BaseModel):
    """High-level platform metrics"""
    platform: str
    total_orders: int
    completed_orders: int
    cancelled_orders: int
    total_revenue_usd: float
    orders_this_month: int
    revenue_this_month_usd: float
    orders_last_month: int
    revenue_last_month_usd: float
    orders_today: int
    revenue_today_usd: float
    avg_order_value_usd: float
    avg_items_per_order: float
    payment_rate: float
    fulfillment_rate: float
    cancellation_rate: float
    revenue_mom_growth_pct: float
    orders_mom_growth_pct: float
    first_order_date: Optional[date]
    last_order_date: Optional[date]

class DailySnapshot(BaseModel):
    """Daily KPI snapshot"""
    order_date: date
    total_orders: int
    total_revenue_usd: float
    avg_order_value_usd: float
    total_items_sold: int
    shopify_orders: int
    amazon_orders: int
    lazada_orders: int
    shopee_orders: int
    shopify_revenue_usd: float
    amazon_revenue_usd: float
    lazada_revenue_usd: float
    shopee_revenue_usd: float
    unique_customers: int
    fulfilled_orders: int
    fulfillment_rate: float
    revenue_7d_avg: Optional[float]
    orders_7d_avg: Optional[float]
    revenue_dod_change: Optional[float]
    orders_dod_change: Optional[int]

class RevenueByPlatform(BaseModel):
    """Revenue breakdown by platform"""
    order_date: date
    platform: str
    total_orders: int
    gross_revenue: float
    gross_revenue_usd: float
    net_revenue: float
    avg_order_value: float
    paid_orders: int
    fulfilled_orders: int
    revenue_growth_pct: float
    mtd_revenue_usd: float

class ProductPerformance(BaseModel):
    """Product performance metrics"""
    platform: str
    product_id: str
    product_name: str
    sku: Optional[str]
    total_orders: int
    total_units_sold: int
    total_revenue: float
    avg_selling_price: float
    units_this_month: int
    revenue_this_month: float
    avg_daily_units: float
    revenue_rank: int
    performance_tier: str

# ============== Dashboard Response ==============

class DashboardSummary(BaseModel):
    """Main dashboard summary"""
    total_revenue_usd: float
    total_orders: int
    avg_order_value_usd: float
    revenue_growth_pct: float
    orders_growth_pct: float
    total_customers: int
    platforms: List[PlatformOverview]
    recent_days: List[DailySnapshot]

# ============== Store/Connection Models ==============

class StoreConnection(BaseModel):
    """Store connection status"""
    platform: str
    is_connected: bool
    last_sync: Optional[datetime]
    status: str
    connection_id: Optional[str]

class StoreConnectionRequest(BaseModel):
    """Request to connect a store"""
    platform: Platform
    credentials: dict

