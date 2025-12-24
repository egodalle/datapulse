"""
Application configuration
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings"""
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = int(os.getenv("PORT", "6000"))  # Render uses PORT env var
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"
    
    # Database - Render provides DATABASE_URL
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://edgodalle@localhost:5432/datapulse"
    )
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Airbyte
    AIRBYTE_API_URL: str = "http://localhost:8001/api/v1"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings() -> Settings:
    return Settings()

settings = get_settings()
