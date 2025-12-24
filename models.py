"""
Database models for HYDRA server.
Uses SQLAlchemy with PostgreSQL (Supabase).
"""

import os
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()
from typing import Optional, List
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean, 
    DateTime, Text, ForeignKey, JSON, Index, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import QueuePool

Base = declarative_base()


class Token(Base):
    """Token configuration stored on server."""
    __tablename__ = "tokens"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True)  # e.g., "SOL-USDT"
    base = Column(String(50), nullable=False)  # e.g., "SOL"
    quote = Column(String(50), default="USDT")  # e.g., "USDT"
    
    # DEX configuration
    dexes = Column(JSON, nullable=True)  # ["pancake", "jupiter", "matcha"]
    jupiter_mint = Column(String(100), nullable=True)
    jupiter_decimals = Column(Integer, nullable=True)
    bsc_address = Column(String(100), nullable=True)
    mexc_price_scale = Column(Integer, nullable=True)
    matcha_address = Column(String(100), nullable=True)
    matcha_decimals = Column(Integer, nullable=True)
    cg_id = Column(String(100), nullable=True)  # CoinGecko ID
    
    # Spread settings
    spread_direct = Column(Boolean, default=True)
    spread_reverse = Column(Boolean, default=True)
    spread_threshold = Column(Float, nullable=True)
    spread_direct_threshold = Column(Float, nullable=True)
    spread_reverse_threshold = Column(Float, nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    spread_history = relationship("SpreadHistory", back_populates="token", cascade="all, delete-orphan")


class SpreadHistory(Base):
    """2-day spread history for charts."""
    __tablename__ = "spread_history"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    token_id = Column(Integer, ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False)
    dex_name = Column(String(50), nullable=False)  # "pancake", "jupiter", "matcha"
    timestamp = Column(Float, nullable=False)  # Unix timestamp
    direct_spread = Column(Float, nullable=True)
    reverse_spread = Column(Float, nullable=True)
    dex_price = Column(Float, nullable=True)
    cex_bid = Column(Float, nullable=True)
    cex_ask = Column(Float, nullable=True)
    
    # Relationships
    token = relationship("Token", back_populates="spread_history")
    
    __table_args__ = (
        Index("idx_token_dex_timestamp", "token_id", "dex_name", "timestamp"),
    )


class Proxy(Base):
    """Proxy configuration for server requests."""
    __tablename__ = "proxies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    proxy_string = Column(String(255), nullable=False)  # login:pass@ip:port
    protocol = Column(String(10), default="socks5")  # "socks5" or "http"
    is_active = Column(Boolean, default=True)
    last_used = Column(DateTime, nullable=True)
    fail_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class AdminUser(Base):
    """Admin users for admin panel."""
    __tablename__ = "admin_users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ProductKey(Base):
    """Product keys for client activation."""
    __tablename__ = "product_keys"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), nullable=False)  # Имя пользователя
    key = Column(String(100), unique=True, nullable=False, index=True)  # Ключ продукта
    is_active = Column(Boolean, default=True)
    is_used = Column(Boolean, default=False)  # Был ли ключ активирован
    used_at = Column(DateTime, nullable=True)  # Когда был активирован
    created_at = Column(DateTime, default=datetime.utcnow)


class DefaultToken(Base):
    """Default tokens for new users."""
    __tablename__ = "default_tokens"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True)  # e.g., "SOL-USDT"
    base = Column(String(50), nullable=False)  # e.g., "SOL"
    quote = Column(String(50), default="USDT")  # e.g., "USDT"
    
    # DEX configuration
    dexes = Column(JSON, nullable=True)  # ["pancake", "jupiter", "matcha"]
    jupiter_mint = Column(String(100), nullable=True)
    jupiter_decimals = Column(Integer, nullable=True)
    bsc_address = Column(String(100), nullable=True)
    mexc_price_scale = Column(Integer, nullable=True)
    matcha_address = Column(String(100), nullable=True)
    matcha_decimals = Column(Integer, nullable=True)
    cg_id = Column(String(100), nullable=True)  # CoinGecko ID
    
    # Spread settings
    spread_direct = Column(Boolean, default=True)
    spread_reverse = Column(Boolean, default=True)
    spread_threshold = Column(Float, nullable=True)
    spread_direct_threshold = Column(Float, nullable=True)
    spread_reverse_threshold = Column(Float, nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ServerSettings(Base):
    """Global server settings."""
    __tablename__ = "server_settings"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), unique=True, nullable=False)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ============================================================
# Database setup - PostgreSQL (Supabase)
# ============================================================

# Get DATABASE_URL from environment variable or use default Supabase connection
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:OMdAvhDaaNmAscwQPoKAYtXVJIQcghxT@turntable.proxy.rlwy.net:53749/railway"
)

# Create engine with connection pooling for PostgreSQL
engine = None
SessionLocal = None

def create_db_engine():
    """Create database engine with retry logic."""
    import sys
    global engine, SessionLocal
    
    print(f"create_db_engine() called, DATABASE_URL starts with: {DATABASE_URL[:50]}...", flush=True)
    sys.stdout.flush()
    
    try:
        print("Creating engine...", flush=True)
        engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"connect_timeout": 30},  # Increased timeout
            echo=False
        )
        print(f"Engine created: {engine}", flush=True)
        
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        print(f"SessionLocal created: {SessionLocal}", flush=True)
        
        # Test connection
        print("Testing connection...", flush=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connected successfully!", flush=True)
        return True
    except Exception as e:
        import traceback
        print(f"Database connection failed: {e}", flush=True)
        print(f"Traceback: {traceback.format_exc()}", flush=True)
        engine = None
        SessionLocal = None
        return False


def init_db():
    """Initialize database tables with retry."""
    import time
    import sys
    import hashlib
    global engine, SessionLocal
    
    print("init_db() called", flush=True)
    sys.stdout.flush()
    
    for attempt in range(10):  # More retries
        print(f"Attempting database connection (attempt {attempt + 1}/10)...", flush=True)
        sys.stdout.flush()
        if create_db_engine():
            try:
                Base.metadata.create_all(bind=engine)
                print(f"Database initialized successfully! SessionLocal={SessionLocal}")
                
                # Create default admin user if not exists
                try:
                    db = SessionLocal()
                    existing_admin = db.query(AdminUser).filter(AdminUser.username == "admin").first()
                    if not existing_admin:
                        password_hash = hashlib.sha256("admin123".encode()).hexdigest()
                        admin_user = AdminUser(
                            username="admin",
                            password_hash=password_hash,
                            is_active=True
                        )
                        db.add(admin_user)
                        db.commit()
                        print("Default admin user created (admin/admin123)")
                    else:
                        print("Admin user already exists")
                    db.close()
                except Exception as e:
                    print(f"Failed to create admin user: {e}")
                
                return True
            except Exception as e:
                print(f"Failed to create tables: {e}")
                engine = None
                SessionLocal = None
        print(f"Retrying database connection in 3 seconds...")
        time.sleep(3)
    
    print("WARNING: Could not connect to database after 10 attempts")
    return False


def get_db():
    """Get database session."""
    if SessionLocal is None:
        # Try to reconnect
        create_db_engine()
    if SessionLocal is None:
        raise Exception("Database not available")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
