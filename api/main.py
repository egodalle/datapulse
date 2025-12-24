"""
DataPulse API - E-commerce Analytics Backend
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from routers import kpis, stores, health, seed, dbt_run, auth
from core.config import settings
from core.database import engine

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    print(f"🚀 DataPulse API starting on {settings.API_HOST}:{settings.API_PORT}")
    yield
    # Shutdown
    await engine.dispose()
    print("👋 DataPulse API shutting down")

app = FastAPI(
    title="DataPulse API",
    description="E-commerce Analytics API - Aggregating KPIs from Shopify, Amazon, Lazada, and Shopee",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://datapulsestore.lovable.app",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8080",
        "*",  # Allow all origins for development (restrict in production)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(kpis.router, prefix="/api/v1/kpis", tags=["KPIs"])
app.include_router(stores.router, prefix="/api/v1/stores", tags=["Stores"])
app.include_router(seed.router, prefix="/api/v1/admin", tags=["Admin"])
app.include_router(dbt_run.router, prefix="/api/v1/admin", tags=["Admin"])
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication"])

@app.get("/")
async def root():
    return {
        "name": "DataPulse API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
    )

