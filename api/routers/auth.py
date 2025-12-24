"""
Authentication endpoints for Lovable UI
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import jwt
import bcrypt
from datetime import datetime, timedelta
import os

from core.database import get_db

router = APIRouter()
security = HTTPBearer()

# Configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

# Request/Response Models
class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str = ""

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict

class UserResponse(BaseModel):
    id: int
    email: str
    name: str

# Helper Functions
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())

def create_token(user_id: int, email: str) -> str:
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Ensure users table exists
async def ensure_users_table(db: AsyncSession):
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            name VARCHAR(255) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    await db.commit()

# Endpoints
@router.post("/register", response_model=TokenResponse)
async def register(request: RegisterRequest, db: AsyncSession = Depends(get_db)):
    """Register a new user"""
    await ensure_users_table(db)
    
    # Check if user exists
    result = await db.execute(
        text("SELECT id FROM users WHERE email = :email"),
        {"email": request.email}
    )
    if result.fetchone():
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create user
    password_hash = hash_password(request.password)
    result = await db.execute(
        text("""
            INSERT INTO users (email, password_hash, name) 
            VALUES (:email, :password_hash, :name) 
            RETURNING id
        """),
        {"email": request.email, "password_hash": password_hash, "name": request.name}
    )
    user_id = result.fetchone()[0]
    await db.commit()
    
    # Generate token
    token = create_token(user_id, request.email)
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user_id, "email": request.email, "name": request.name}
    }

@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Login with email and password"""
    await ensure_users_table(db)
    
    # Get user
    result = await db.execute(
        text("SELECT id, email, password_hash, name FROM users WHERE email = :email"),
        {"email": request.email}
    )
    user = result.fetchone()
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    user_id, email, password_hash, name = user
    
    # Verify password
    if not verify_password(request.password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Generate token
    token = create_token(user_id, email)
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user_id, "email": email, "name": name or ""}
    }

@router.get("/me", response_model=UserResponse)
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Get current user from token"""
    payload = decode_token(credentials.credentials)
    
    result = await db.execute(
        text("SELECT id, email, name FROM users WHERE id = :id"),
        {"id": int(payload["sub"])}
    )
    user = result.fetchone()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"id": user[0], "email": user[1], "name": user[2] or ""}

@router.post("/logout")
async def logout():
    """Logout (client should discard token)"""
    return {"message": "Logged out successfully"}

@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Refresh access token"""
    payload = decode_token(credentials.credentials)
    
    result = await db.execute(
        text("SELECT id, email, name FROM users WHERE id = :id"),
        {"id": int(payload["sub"])}
    )
    user = result.fetchone()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Generate new token
    token = create_token(user[0], user[1])
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user[0], "email": user[1], "name": user[2] or ""}
    }

