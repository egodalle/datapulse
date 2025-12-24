"""
KPI endpoints
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
from datetime import date

from core.database import get_db
from services.kpi_service import KPIService
from models.schemas import (
    PlatformOverview,
    DailySnapshot,
    RevenueByPlatform,
    ProductPerformance,
    DashboardSummary,
    Platform,
)

router = APIRouter()

@router.get("/dashboard", response_model=dict)
async def get_dashboard(db: AsyncSession = Depends(get_db)):
    """
    Get main dashboard summary with all key KPIs
    """
    try:
        service = KPIService(db)
        return await service.get_dashboard_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dashboard: {str(e)}")

@router.get("/platforms", response_model=List[dict])
async def get_platform_overview(db: AsyncSession = Depends(get_db)):
    """
    Get overview metrics for all connected platforms
    """
    try:
        service = KPIService(db)
        return await service.get_platform_overview()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching platform overview: {str(e)}")

@router.get("/daily", response_model=List[dict])
async def get_daily_snapshots(
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(30, ge=1, le=365, description="Number of days to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get daily KPI snapshots
    """
    try:
        service = KPIService(db)
        return await service.get_daily_snapshots(start_date, end_date, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching daily snapshots: {str(e)}")

@router.get("/revenue", response_model=List[dict])
async def get_revenue_by_platform(
    platform: Optional[Platform] = Query(None, description="Filter by platform"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get revenue metrics by platform
    """
    try:
        service = KPIService(db)
        platform_str = platform.value if platform else None
        return await service.get_revenue_by_platform(platform_str, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching revenue data: {str(e)}")

@router.get("/products", response_model=List[dict])
async def get_product_performance(
    platform: Optional[Platform] = Query(None, description="Filter by platform"),
    tier: Optional[str] = Query(None, description="Filter by performance tier"),
    limit: int = Query(50, ge=1, le=500, description="Number of products to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get product performance metrics
    """
    try:
        service = KPIService(db)
        platform_str = platform.value if platform else None
        return await service.get_product_performance(platform_str, tier, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product data: {str(e)}")

@router.get("/summary/today")
async def get_today_summary(db: AsyncSession = Depends(get_db)):
    """
    Get today's summary metrics
    """
    try:
        service = KPIService(db)
        snapshots = await service.get_daily_snapshots(
            start_date=date.today(),
            end_date=date.today(),
            limit=1
        )
        if snapshots:
            return snapshots[0]
        return {"message": "No data for today yet"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching today's summary: {str(e)}")

