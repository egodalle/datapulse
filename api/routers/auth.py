"""
Authentication endpoints for Lovable UI
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import jwt
import bcrypt
from datetime import datetime, timedelta
import os
import secrets

from core.database import get_db

router = APIRouter()
security = HTTPBearer()

# Configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

# Request/Response Models
class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
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

class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

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
            reset_token VARCHAR(255),
            reset_token_expires TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    await db.commit()

# Add reset token columns if they don't exist (for existing tables)
async def ensure_reset_columns(db: AsyncSession):
    try:
        await db.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token VARCHAR(255)"))
        await db.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token_expires TIMESTAMP"))
        await db.commit()
    except:
        await db.rollback()

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


@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    """
    Request a password reset. 
    Returns a reset token (in production, this would be sent via email).
    """
    await ensure_users_table(db)
    await ensure_reset_columns(db)
    
    # Check if user exists
    result = await db.execute(
        text("SELECT id, email FROM users WHERE email = :email"),
        {"email": request.email}
    )
    user = result.fetchone()
    
    # Always return success to prevent email enumeration
    if not user:
        return {
            "message": "If an account with that email exists, a password reset link has been sent.",
            "success": True
        }
    
    # Generate reset token
    reset_token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(hours=1)  # Token valid for 1 hour
    
    # Save reset token to database
    await db.execute(
        text("""
            UPDATE users 
            SET reset_token = :token, reset_token_expires = :expires 
            WHERE id = :id
        """),
        {"token": reset_token, "expires": expires, "id": user[0]}
    )
    await db.commit()
    
    # In production, send email here with reset link
    # For now, return token in response (for development/testing)
    return {
        "message": "If an account with that email exists, a password reset link has been sent.",
        "success": True,
        # Include token in dev mode - remove in production!
        "reset_token": reset_token,
        "reset_url": f"https://datapulsestore.lovable.app/reset-password?token={reset_token}"
    }


@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    """Reset password using the token from forgot-password"""
    await ensure_users_table(db)
    await ensure_reset_columns(db)
    
    # Find user with valid reset token
    result = await db.execute(
        text("""
            SELECT id, email FROM users 
            WHERE reset_token = :token 
            AND reset_token_expires > :now
        """),
        {"token": request.token, "now": datetime.utcnow()}
    )
    user = result.fetchone()
    
    if not user:
        raise HTTPException(
            status_code=400, 
            detail="Invalid or expired reset token"
        )
    
    # Update password and clear reset token
    new_hash = hash_password(request.new_password)
    await db.execute(
        text("""
            UPDATE users 
            SET password_hash = :hash, reset_token = NULL, reset_token_expires = NULL 
            WHERE id = :id
        """),
        {"hash": new_hash, "id": user[0]}
    )
    await db.commit()
    
    return {
        "message": "Password has been reset successfully",
        "success": True
    }

