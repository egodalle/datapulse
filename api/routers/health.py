"""
Health check endpoints
"""
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_db

router = APIRouter()

@router.get("/health")
async def health_check():
    """Basic health check"""
    return {"status": "healthy"}

@router.get("/health/db")
async def database_health(db: AsyncSession = Depends(get_db)):
    """Database connection health check"""
    try:
        result = await db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
        }

@router.get("/health/schemas")
async def schemas_health(db: AsyncSession = Depends(get_db)):
    """Check if required schemas and tables exist"""
    try:
        # Check schemas
        schema_query = text("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'staging', 'intermediate', 'marts')
        """)
        result = await db.execute(schema_query)
        schemas = [row[0] for row in result.fetchall()]
        
        # Check mart tables
        tables_query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'marts'
        """)
        result = await db.execute(tables_query)
        tables = [row[0] for row in result.fetchall()]
        
        return {
            "status": "healthy",
            "schemas": schemas,
            "mart_tables": tables,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }

