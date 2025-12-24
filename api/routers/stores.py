"""
Store connection management endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from core.database import get_db
from models.schemas import StoreConnection, StoreConnectionRequest, Platform

router = APIRouter()

# Mock store connections (in production, this would be in the database)
STORE_CONNECTIONS = {
    Platform.SHOPIFY: {
        "platform": "shopify",
        "is_connected": False,
        "last_sync": None,
        "status": "not_connected",
        "connection_id": None,
    },
    Platform.AMAZON: {
        "platform": "amazon",
        "is_connected": False,
        "last_sync": None,
        "status": "not_connected",
        "connection_id": None,
    },
    Platform.LAZADA: {
        "platform": "lazada",
        "is_connected": False,
        "last_sync": None,
        "status": "not_connected",
        "connection_id": None,
    },
    Platform.SHOPEE: {
        "platform": "shopee",
        "is_connected": False,
        "last_sync": None,
        "status": "not_connected",
        "connection_id": None,
    },
}

@router.get("/", response_model=List[StoreConnection])
async def list_stores():
    """
    List all store connections and their status
    """
    return list(STORE_CONNECTIONS.values())

@router.get("/{platform}", response_model=StoreConnection)
async def get_store(platform: Platform):
    """
    Get connection status for a specific platform
    """
    if platform not in STORE_CONNECTIONS:
        raise HTTPException(status_code=404, detail=f"Platform {platform} not found")
    return STORE_CONNECTIONS[platform]

@router.post("/connect", response_model=StoreConnection)
async def connect_store(request: StoreConnectionRequest):
    """
    Connect a new store (initiates Airbyte connection)
    
    This endpoint would:
    1. Validate credentials
    2. Create Airbyte source connection
    3. Set up sync schedule
    4. Store connection details
    """
    platform = request.platform
    
    # In production, this would:
    # 1. Call Airbyte API to create source
    # 2. Test connection
    # 3. Create sync to PostgreSQL destination
    
    # Mock successful connection
    STORE_CONNECTIONS[platform] = {
        "platform": platform.value,
        "is_connected": True,
        "last_sync": None,
        "status": "connected",
        "connection_id": f"airbyte-{platform.value}-001",
    }
    
    return STORE_CONNECTIONS[platform]

@router.delete("/{platform}")
async def disconnect_store(platform: Platform):
    """
    Disconnect a store
    """
    if platform not in STORE_CONNECTIONS:
        raise HTTPException(status_code=404, detail=f"Platform {platform} not found")
    
    # In production, this would also delete the Airbyte connection
    STORE_CONNECTIONS[platform] = {
        "platform": platform.value,
        "is_connected": False,
        "last_sync": None,
        "status": "disconnected",
        "connection_id": None,
    }
    
    return {"message": f"Successfully disconnected {platform.value}"}

@router.post("/{platform}/sync")
async def trigger_sync(platform: Platform):
    """
    Trigger a manual sync for a platform
    """
    if platform not in STORE_CONNECTIONS:
        raise HTTPException(status_code=404, detail=f"Platform {platform} not found")
    
    connection = STORE_CONNECTIONS[platform]
    if not connection["is_connected"]:
        raise HTTPException(status_code=400, detail=f"Platform {platform} is not connected")
    
    # In production, this would trigger an Airbyte sync job
    return {
        "message": f"Sync triggered for {platform.value}",
        "status": "running",
        "job_id": f"sync-{platform.value}-001",
    }

