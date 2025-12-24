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
    mexc_symbol = Column(String(50), nullable=True)  # MEXC base symbol (e.g., "ARCSOL" for ARC)
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
    """Default tokens for new users - references Token table."""
    __tablename__ = "default_tokens"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    token_id = Column(Integer, ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to Token
    token = relationship("Token", backref="default_token_ref")


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
            pool_size=20,
            max_overflow=30,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_timeout=60,  # Wait longer for connection
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


def migrate_default_tokens(db):
    """Migrate default_tokens table to new structure with token_id reference."""
    from sqlalchemy import inspect
    
    inspector = inspect(db.bind)
    
    # Check if default_tokens table exists
    if 'default_tokens' in inspector.get_table_names():
        columns = [col['name'] for col in inspector.get_columns('default_tokens')]
        
        # If table has token_id column, clean up any NULL values
        if 'token_id' in columns:
            try:
                result = db.execute(text("DELETE FROM default_tokens WHERE token_id IS NULL"))
                if result.rowcount > 0:
                    db.commit()
                    print(f"Cleaned up {result.rowcount} default_tokens with NULL token_id")
            except Exception as e:
                print(f"Cleanup error: {e}")
                db.rollback()
        
        # If table has old structure (has 'name' column but not 'token_id')
        if 'name' in columns and 'token_id' not in columns:
            print("Migrating default_tokens table to new structure...")
            
            try:
                # Get all old default tokens
                old_tokens = db.execute(text("SELECT * FROM default_tokens")).fetchall()
                print(f"Found {len(old_tokens)} old default tokens to migrate")
                
                # Drop old table
                db.execute(text("DROP TABLE default_tokens CASCADE"))
                db.commit()
                print("Old default_tokens table dropped")
                
                # Create new table (will be created by Base.metadata.create_all)
                Base.metadata.create_all(bind=db.bind)
                print("New default_tokens table created")
                
                # Migrate data: for each old default token, find or create Token and create DefaultToken
                for old_token in old_tokens:
                    # old_token is a Row object, access by index or column name
                    token_name = old_token[1] if len(old_token) > 1 else None  # name is usually second column
                    if not token_name:
                        continue
                    
                    # Find existing Token by name
                    existing_token = db.query(Token).filter(Token.name == token_name).first()
                    
                    if existing_token:
                        # Check if DefaultToken already exists for this token
                        existing_default = db.query(DefaultToken).filter(DefaultToken.token_id == existing_token.id).first()
                        if not existing_default:
                            new_default = DefaultToken(token_id=existing_token.id)
                            db.add(new_default)
                            print(f"Migrated default token: {token_name} -> token_id={existing_token.id}")
                    else:
                        print(f"Token {token_name} not found in tokens table, skipping")
                
                db.commit()
                print("Migration completed successfully!")
                
            except Exception as e:
                print(f"Migration error: {e}")
                db.rollback()
                # If migration fails, just recreate the table
                try:
                    db.execute(text("DROP TABLE IF EXISTS default_tokens CASCADE"))
                    db.commit()
                    Base.metadata.create_all(bind=db.bind)
                    print("Recreated default_tokens table after migration error")
                except Exception as e2:
                    print(f"Failed to recreate table: {e2}")


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
                # Run migration before creating tables
                db = SessionLocal()
                migrate_default_tokens(db)
                db.close()
                
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
