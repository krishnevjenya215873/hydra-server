"""
HYDRA Server - Main FastAPI Application
Handles API endpoints, WebSocket connections, and background price fetching.
"""

import asyncio
import logging
import time
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status, Query, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from models import init_db, get_db, Token, SpreadHistory, Proxy, AdminUser, ServerSettings, ProductKey, DefaultToken
import models  # For accessing SessionLocal after init_db()
from schemas import (
    TokenCreate, TokenUpdate, TokenResponse,
    ProxyCreate, ProxyBulkCreate, ProxyResponse,
    SpreadHistoryResponse, SpreadHistoryPoint,
    AdminLogin, AdminToken, ServerStats,
    ProductKeyCreate, ProductKeyResponse, ProductKeyVerify,
    DefaultTokenCreate, DefaultTokenResponse
)
from websocket_manager import connection_manager, handle_websocket_message
from worker import price_worker
from admin_routes import router as admin_router
from proxy_manager import proxy_health_checker, proxy_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Server start time for uptime calculation
SERVER_START_TIME = time.time()

# Solana RPC for getting token decimals
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"

# Cache for token decimals to avoid repeated RPC calls
DECIMALS_CACHE: Dict[str, int] = {}

def get_solana_token_decimals(mint_address: str) -> int:
    """Get token decimals from Solana RPC with caching. Returns 6 as default if fails."""
    # Check cache first
    if mint_address in DECIMALS_CACHE:
        return DECIMALS_CACHE[mint_address]
    
    import httpx
    try:
        response = httpx.post(
            SOLANA_RPC_URL,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenSupply",
                "params": [mint_address]
            },
            timeout=3.0  # Reduced timeout for faster response
        )
        if response.status_code == 200:
            data = response.json()
            decimals = data.get("result", {}).get("value", {}).get("decimals")
            if decimals is not None:
                DECIMALS_CACHE[mint_address] = int(decimals)
                logger.info(f"Solana RPC: decimals for {mint_address} = {decimals}")
                return int(decimals)
    except Exception as e:
        logger.warning(f"Solana RPC error for {mint_address}: {e}")
    
    # Default to 6 (most common for Solana tokens)
    DECIMALS_CACHE[mint_address] = 6
    logger.warning(f"Using default decimals=6 for {mint_address}")
    return 6

# Simple token-based auth for admin
ADMIN_TOKENS: Dict[str, datetime] = {}
TOKEN_EXPIRY_HOURS = 24


def normalize_token_name(name: str) -> str:
    """Normalize token name to prevent duplicates.
    
    - Removes extra spaces
    - Converts to uppercase
    - Ensures format is BASE-QUOTE (e.g., FARTCOIN-USDT)
    """
    # Remove extra spaces and strip
    name = ' '.join(name.split()).strip()
    # Convert to uppercase
    name = name.upper()
    # Replace space-dash or dash-space with just dash
    name = name.replace(' -', '-').replace('- ', '-')
    return name


def normalize_symbol(symbol: str) -> str:
    """Normalize token symbol (base currency).
    
    - Removes spaces
    - Converts to uppercase
    """
    return symbol.replace(' ', '').strip().upper()


def hash_password(password: str) -> str:
    """Hash password with SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_admin_token(token: str) -> bool:
    """Verify admin token is valid and not expired."""
    if token not in ADMIN_TOKENS:
        return False
    expiry = ADMIN_TOKENS[token]
    if datetime.utcnow() > expiry:
        del ADMIN_TOKENS[token]
        return False
    return True


def get_admin_token(request: Request) -> str:
    """Extract and verify admin token from header."""
    authorization = request.headers.get("Authorization") or request.headers.get("authorization")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
    token = authorization[7:]
    if not verify_admin_token(token):
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return token


# Callback for worker to broadcast updates
# Global event loop reference for cross-thread callbacks
_main_loop: Optional[asyncio.AbstractEventLoop] = None


def on_price_update(data: Dict[str, Any]):
    """Called by worker when new price data is available.
    
    This is called from a background thread, so we need to use
    call_soon_threadsafe to schedule the coroutine on the main event loop.
    """
    global _main_loop
    if _main_loop is None or _main_loop.is_closed():
        return
    
    try:
        # Schedule the coroutine on the main event loop from another thread
        asyncio.run_coroutine_threadsafe(
            connection_manager.broadcast_update(data),
            _main_loop
        )
    except Exception as e:
        logger.error(f"Error scheduling broadcast: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _main_loop
    
    # Startup
    logger.info("Starting HYDRA server...")
    
    # Store reference to main event loop for cross-thread callbacks
    _main_loop = asyncio.get_running_loop()
    
    # Run init_db in a thread pool to not block event loop
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as pool:
        db_connected = await asyncio.get_event_loop().run_in_executor(pool, init_db)
    
    logger.info(f"Database connection result: {db_connected}, SessionLocal: {models.SessionLocal is not None}")
    
    # Create default admin if not exists (only if DB connected)
    if db_connected and models.SessionLocal:
        try:
            db = models.SessionLocal()
            try:
                admin = db.query(AdminUser).filter(AdminUser.username == "admin").first()
                if not admin:
                    admin = AdminUser(
                        username="admin",
                        password_hash=hash_password("admin123")  # Change this!
                    )
                    db.add(admin)
                    db.commit()
                    logger.info("Default admin user created (username: admin, password: admin123)")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Failed to create admin user: {e}")
    else:
        logger.warning("Database not connected, skipping admin user creation")
    
    # Start price worker only if DB is connected
    if db_connected and models.SessionLocal:
        # Initialize proxy cache BEFORE starting worker
        # This ensures parallel threads can use cached proxies without DB sessions
        try:
            db = models.SessionLocal()
            try:
                proxy_manager.force_refresh_cache(db)
                logger.info(f"Proxy cache initialized: {len(proxy_manager._proxy_cache)} proxies")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Failed to initialize proxy cache: {e}")
        
        price_worker.register_callback(on_price_update)
        price_worker.start()
        
        # Start proxy health checker (проверка каждый час)
        proxy_health_checker.start()
    else:
        logger.error("Database not connected, price worker and health checker will not start")
    
    yield
    
    # Shutdown
    logger.info("Shutting down HYDRA server...")
    price_worker.stop()
    proxy_health_checker.stop()


app = FastAPI(
    title="HYDRA Server",
    description="Server for cryptocurrency spread monitoring",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
# Include admin panel routes
app.include_router(admin_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== Health Check ==============

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "timestamp": time.time()}


@app.get("/stats", response_model=ServerStats)
async def get_stats(db: Session = Depends(get_db)):
    """Get server statistics."""
    return ServerStats(
        total_tokens=db.query(Token).count(),
        active_tokens=db.query(Token).filter(Token.is_active == True).count(),
        total_proxies=db.query(Proxy).count(),
        active_proxies=db.query(Proxy).filter(Proxy.is_active == True).count(),
        connected_clients=connection_manager.get_connection_count(),
        uptime_seconds=time.time() - SERVER_START_TIME
    )


# ============== WebSocket ==============

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time price updates."""
    await connection_manager.connect(websocket)
    
    # НЕ отправляем initial_data сразу!
    # Клиент должен сначала подписаться на нужные токены,
    # и только потом получит данные через broadcast
    
    try:
        while True:
            message = await websocket.receive_text()
            await handle_websocket_message(websocket, message)
    except WebSocketDisconnect:
        await connection_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await connection_manager.disconnect(websocket)


# ============== Token Endpoints ==============

@app.get("/api/tokens", response_model=List[TokenResponse])
async def list_tokens(
    active_only: bool = Query(default=False),
    db: Session = Depends(get_db)
):
    """List all tokens."""
    query = db.query(Token)
    if active_only:
        query = query.filter(Token.is_active == True)
    return query.all()


@app.get("/api/tokens/{token_name}", response_model=TokenResponse)
async def get_token(token_name: str, db: Session = Depends(get_db)):
    """Get token by name."""
    token = db.query(Token).filter(Token.name == token_name).first()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    return token


@app.post("/api/tokens", response_model=TokenResponse)
async def create_token(token_data: TokenCreate, db: Session = Depends(get_db)):
    """Create new token (from client app)."""
    # Normalize token name and base symbol
    normalized_name = normalize_token_name(token_data.name)
    normalized_base = normalize_symbol(token_data.base)
    
    # Check if token already exists (by normalized name)
    existing = db.query(Token).filter(Token.name == normalized_name).first()
    if existing:
        # If exists but inactive, reactivate it
        if not existing.is_active:
            existing.is_active = True
            for key, value in token_data.dict(exclude_unset=True).items():
                if hasattr(existing, key) and key not in ['name', 'base']:
                    setattr(existing, key, value)
            db.commit()
            db.refresh(existing)
            return existing
        raise HTTPException(status_code=400, detail="Token already exists")
    
    # Create token with normalized name
    token_dict = token_data.dict()
    token_dict['name'] = normalized_name
    token_dict['base'] = normalized_base
    token = Token(**token_dict)
    db.add(token)
    db.commit()
    db.refresh(token)
    logger.info(f"Token created: {token.name}")
    return token


@app.put("/api/tokens/{token_name}", response_model=TokenResponse)
async def update_token(
    token_name: str,
    token_data: TokenUpdate,
    db: Session = Depends(get_db)
):
    """Update token settings."""
    token = db.query(Token).filter(Token.name == token_name).first()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    
    for key, value in token_data.dict(exclude_unset=True).items():
        if hasattr(token, key):
            setattr(token, key, value)
    
    db.commit()
    db.refresh(token)
    return token


# Note: No public delete endpoint - only admin can delete


# ============== Spread History ==============

@app.get("/api/history/{token_name}/{dex_name}", response_model=SpreadHistoryResponse)
async def get_spread_history(
    token_name: str,
    dex_name: str,
    hours: int = Query(default=48, ge=1, le=48),
    db: Session = Depends(get_db)
):
    """Get spread history for token/dex pair."""
    token = db.query(Token).filter(Token.name == token_name).first()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    
    cutoff = time.time() - (hours * 3600)
    
    history = db.query(SpreadHistory).filter(
        SpreadHistory.token_id == token.id,
        SpreadHistory.dex_name == dex_name,
        SpreadHistory.timestamp >= cutoff
    ).order_by(SpreadHistory.timestamp.asc()).all()
    
    return SpreadHistoryResponse(
        token_name=token_name,
        dex_name=dex_name,
        history=[
            SpreadHistoryPoint(
                timestamp=h.timestamp,
                direct_spread=h.direct_spread,
                reverse_spread=h.reverse_spread
            )
            for h in history
        ]
    )


@app.get("/api/history/{token_name}")
async def get_all_spread_history(
    token_name: str,
    hours: int = Query(default=48, ge=1, le=48),
    db: Session = Depends(get_db)
):
    """Get spread history for all DEXes for a token."""
    token = db.query(Token).filter(Token.name == token_name).first()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    
    cutoff = time.time() - (hours * 3600)
    
    history = db.query(SpreadHistory).filter(
        SpreadHistory.token_id == token.id,
        SpreadHistory.timestamp >= cutoff
    ).order_by(SpreadHistory.timestamp.asc()).all()
    
    # Group by dex
    result: Dict[str, List] = {}
    for h in history:
        if h.dex_name not in result:
            result[h.dex_name] = []
        result[h.dex_name].append([h.timestamp, h.direct_spread, h.reverse_spread])
    
    return {token_name: result}


# ============== Admin Authentication ==============

@app.post("/api/admin/login", response_model=AdminToken)
async def admin_login(credentials: AdminLogin, db: Session = Depends(get_db)):
    """Admin login endpoint."""
    admin = db.query(AdminUser).filter(
        AdminUser.username == credentials.username,
        AdminUser.is_active == True
    ).first()
    
    if not admin or admin.password_hash != hash_password(credentials.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Generate token
    token = secrets.token_urlsafe(32)
    ADMIN_TOKENS[token] = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRY_HOURS)
    
    return AdminToken(access_token=token)


# ============== Admin: Token Management ==============

@app.delete("/api/admin/tokens/{token_name}")
async def admin_delete_token(
    token_name: str,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Delete token (admin only)."""
    
    token = db.query(Token).filter(Token.name == token_name).first()
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    
    token_id = token.id
    
    # First delete from default_tokens (if linked)
    try:
        db.execute(
            text("DELETE FROM default_tokens WHERE token_id = :token_id"),
            {"token_id": token_id}
        )
    except Exception as e:
        logger.warning(f"Error deleting default_token for token {token_name}: {e}")
    
    # Then delete spread history directly via SQL (faster than ORM cascade)
    try:
        db.execute(
            text("DELETE FROM spread_history WHERE token_id = :token_id"),
            {"token_id": token_id}
        )
    except Exception as e:
        logger.warning(f"Error deleting spread history for token {token_name}: {e}")
    
    # Now delete the token itself
    db.delete(token)
    db.commit()
    logger.info(f"Token deleted by admin: {token_name}")
    return {"status": "deleted", "token": token_name}


# ============== Admin: Proxy Management ==============

@app.get("/api/admin/proxies", response_model=List[ProxyResponse])
async def admin_list_proxies(
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """List all proxies (admin only)."""
    return db.query(Proxy).all()


@app.post("/api/admin/proxies", response_model=ProxyResponse)
async def admin_add_proxy(
    proxy_data: ProxyCreate,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Add single proxy (admin only)."""
    
    proxy = Proxy(**proxy_data.dict())
    db.add(proxy)
    db.commit()
    db.refresh(proxy)
    return proxy


@app.post("/api/admin/proxies/bulk")
async def admin_add_proxies_bulk(
    data: ProxyBulkCreate,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Add multiple proxies at once (admin only)."""
    
    added = 0
    for proxy_string in data.proxies:
        proxy_string = proxy_string.strip()
        if not proxy_string:
            continue
        
        # Check if already exists
        existing = db.query(Proxy).filter(Proxy.proxy_string == proxy_string).first()
        if existing:
            continue
        
        proxy = Proxy(
            proxy_string=proxy_string,
            protocol=data.protocol
        )
        db.add(proxy)
        added += 1
    
    db.commit()
    logger.info(f"Added {added} proxies")
    return {"status": "ok", "added": added}


@app.delete("/api/admin/proxies/{proxy_id}")
async def admin_delete_proxy(
    proxy_id: int,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Delete proxy (admin only)."""
    
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    db.delete(proxy)
    db.commit()
    return {"status": "deleted", "proxy_id": proxy_id}


@app.put("/api/admin/proxies/{proxy_id}/toggle")
async def admin_toggle_proxy(
    proxy_id: int,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Toggle proxy active status (admin only)."""
    
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    proxy.is_active = not proxy.is_active
    db.commit()
    return {"status": "ok", "is_active": proxy.is_active}


@app.delete("/api/admin/proxies")
async def admin_clear_proxies(
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Clear all proxies (admin only)."""
    
    count = db.query(Proxy).delete()
    db.commit()
    logger.info(f"Cleared {count} proxies")
    return {"status": "ok", "deleted": count}


@app.get("/api/admin/proxies/health")
async def admin_get_proxy_health(
    admin_token: str = Depends(get_admin_token)
):
    """Получить результаты последней проверки прокси."""
    return proxy_health_checker.get_last_results()


@app.post("/api/admin/proxies/health/check")
async def admin_force_proxy_check(
    admin_token: str = Depends(get_admin_token)
):
    """Принудительно запустить проверку всех прокси (admin only)."""
    results = proxy_health_checker.force_check()
    return {
        "status": "ok",
        "results": results,
        "working": len([r for r in results if r["working"]]),
        "total": len(results)
    }


@app.post("/api/admin/proxies/reset")
async def admin_reset_proxies(
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Reset fail_count for all proxies and activate them (admin only)."""
    
    # Reset all proxies: set fail_count=0 and is_active=True
    count = db.query(Proxy).update({Proxy.fail_count: 0, Proxy.is_active: True})
    db.commit()
    
    logger.info(f"Reset {count} proxies (fail_count=0, is_active=True)")
    
    # Force refresh proxy cache immediately
    from proxy_manager import proxy_manager
    proxy_manager._cache_updated = None  # Force cache refresh on next request
    proxy_manager._refresh_cache(db)  # Refresh cache now
    
    return {"status": "ok", "reset_count": count}


# ============== Admin: Settings ==============

@app.get("/api/admin/settings")
async def admin_get_settings(
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Get server settings (admin only)."""
    
    settings = db.query(ServerSettings).all()
    return {s.key: s.value for s in settings}


@app.put("/api/admin/settings/{key}")
async def admin_set_setting(
    key: str,
    value: str,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Set server setting (admin only)."""
    
    setting = db.query(ServerSettings).filter(ServerSettings.key == key).first()
    if setting:
        setting.value = value
    else:
        setting = ServerSettings(key=key, value=value)
        db.add(setting)
    
    db.commit()
    
    # Apply setting if it's interval
    if key == "poll_interval":
        try:
            price_worker.set_interval(float(value))
        except ValueError:
            pass
    
    return {"status": "ok", "key": key, "value": value}


# ============== Admin: Product Keys Management ==============

def generate_product_key() -> str:
    """Generate random product key in format XXXX-XXXX-XXXX-XXXX."""
    import string
    import random
    chars = string.ascii_uppercase + string.digits
    parts = [''.join(random.choices(chars, k=4)) for _ in range(4)]
    return '-'.join(parts)


@app.get("/api/admin/keys", response_model=List[ProductKeyResponse])
async def admin_list_keys(
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """List all product keys (admin only)."""
    return db.query(ProductKey).order_by(ProductKey.created_at.desc()).all()


@app.post("/api/admin/keys", response_model=ProductKeyResponse)
async def admin_create_key(
    data: ProductKeyCreate,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Create new product key (admin only)."""
    # Generate unique key
    key = generate_product_key()
    while db.query(ProductKey).filter(ProductKey.key == key).first():
        key = generate_product_key()
    
    product_key = ProductKey(
        username=data.username,
        key=key
    )
    db.add(product_key)
    db.commit()
    db.refresh(product_key)
    logger.info(f"Product key created for user: {data.username}")
    return product_key


@app.delete("/api/admin/keys/{key_id}")
async def admin_delete_key(
    key_id: int,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Delete product key (admin only)."""
    key = db.query(ProductKey).filter(ProductKey.id == key_id).first()
    if not key:
        raise HTTPException(status_code=404, detail="Key not found")
    
    db.delete(key)
    db.commit()
    return {"status": "deleted", "key_id": key_id}


@app.put("/api/admin/keys/{key_id}/toggle")
async def admin_toggle_key(
    key_id: int,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Toggle product key active status (admin only)."""
    key = db.query(ProductKey).filter(ProductKey.id == key_id).first()
    if not key:
        raise HTTPException(status_code=404, detail="Key not found")
    
    key.is_active = not key.is_active
    db.commit()
    return {"status": "ok", "is_active": key.is_active}


# ============== Admin: Default Tokens Management ==============
# Default Tokens теперь ссылаются на Tokens таблицу:
# - При добавлении в Default Tokens: сначала создаётся Token (если не существует), потом DefaultToken ссылается на него
# - При удалении Token: автоматически удаляется DefaultToken (CASCADE)
# - При удалении DefaultToken: Token не трогается


def _build_default_token_response(default_token, token):
    """Helper to build response from DefaultToken and Token."""
    return {
        "id": default_token.id,
        "token_id": default_token.token_id,
        "created_at": default_token.created_at,
        "name": token.name,
        "base": token.base,
        "quote": token.quote,
        "dexes": token.dexes,
        "jupiter_mint": token.jupiter_mint,
        "jupiter_decimals": token.jupiter_decimals,
        "bsc_address": token.bsc_address,
        "mexc_price_scale": token.mexc_price_scale,
        "matcha_address": token.matcha_address,
        "matcha_decimals": token.matcha_decimals,
        "cg_id": token.cg_id,
        "spread_direct": token.spread_direct,
        "spread_reverse": token.spread_reverse,
        "spread_threshold": token.spread_threshold,
        "spread_direct_threshold": token.spread_direct_threshold,
        "spread_reverse_threshold": token.spread_reverse_threshold,
        "is_active": token.is_active
    }


@app.get("/api/admin/default-tokens")
async def admin_list_default_tokens(
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Список всех токенов по умолчанию (только админ)."""
    default_tokens = db.query(DefaultToken).order_by(DefaultToken.created_at.desc()).all()
    result = []
    for dt in default_tokens:
        token = db.query(Token).filter(Token.id == dt.token_id).first()
        if token:
            result.append(_build_default_token_response(dt, token))
    return result


@app.post("/api/admin/default-tokens")
async def admin_create_default_token(
    data: DefaultTokenCreate,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Добавить токен по умолчанию (только админ).
    
    Логика:
    1. Нормализуем имя токена
    2. Проверяем есть ли токен в Tokens
    3. Если нет - создаём его
    4. Проверяем нет ли уже в DefaultTokens
    5. Создаём ссылку в DefaultTokens
    """
    # Нормализуем имя и символ
    normalized_name = normalize_token_name(data.name)
    normalized_base = normalize_symbol(data.base)
    
    # Проверяем есть ли токен в основной таблице Tokens
    token = db.query(Token).filter(Token.name == normalized_name).first()
    
    if not token:
        # Создаём токен в основной таблице
        token = Token(
            name=normalized_name,
            base=normalized_base,
            quote=data.quote.upper(),
            dexes=data.dexes,
            jupiter_mint=data.jupiter_mint,
            jupiter_decimals=data.jupiter_decimals,
            bsc_address=data.bsc_address,
            mexc_price_scale=data.mexc_price_scale,
            matcha_address=data.matcha_address,
            matcha_decimals=data.matcha_decimals,
            cg_id=data.cg_id,
            spread_direct=data.spread_direct,
            spread_reverse=data.spread_reverse,
            spread_threshold=data.spread_threshold,
            spread_direct_threshold=data.spread_direct_threshold,
            spread_reverse_threshold=data.spread_reverse_threshold,
            is_active=True
        )
        db.add(token)
        db.commit()
        db.refresh(token)
        logger.info(f"Created new token: {normalized_name}")
    
    # Проверяем нет ли уже в DefaultTokens
    existing_default = db.query(DefaultToken).filter(DefaultToken.token_id == token.id).first()
    if existing_default:
        raise HTTPException(status_code=400, detail="Токен уже добавлен в токены по умолчанию")
    
    # Создаём ссылку в DefaultTokens
    default_token = DefaultToken(token_id=token.id)
    db.add(default_token)
    db.commit()
    db.refresh(default_token)
    
    logger.info(f"Added token to defaults: {normalized_name}")
    return _build_default_token_response(default_token, token)


@app.post("/api/admin/default-tokens/bulk")
async def admin_bulk_add_default_tokens(
    request: Request,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Массовое добавление токенов по умолчанию по адресам контрактов."""
    import httpx
    
    body = await request.json()
    addresses = body.get("addresses", [])
    dex = body.get("dex", "jupiter")  # jupiter, pancake, matcha
    
    if not addresses:
        raise HTTPException(status_code=400, detail="Список адресов пуст")
    
    results = {
        "success": [],
        "failed": []
    }
    
    for address in addresses:
        address = address.strip()
        if not address:
            continue
        
        try:
            # Получаем информацию о токене через DexScreener
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{address}")
                
                if resp.status_code != 200:
                    results["failed"].append({"address": address, "error": f"DexScreener HTTP {resp.status_code}"})
                    continue
                
                data = resp.json()
                pairs = data.get("pairs") or []
                
                if not pairs:
                    results["failed"].append({"address": address, "error": "Токен не найден на DexScreener"})
                    continue
                
                # Берём первую пару с наибольшей ликвидностью
                best_pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
                
                base_token = best_pair.get("baseToken") or {}
                token_symbol = normalize_symbol(base_token.get("symbol", "UNKNOWN"))
                token_name = f"{token_symbol}-USDT"  # Already normalized
                
                # Определяем параметры в зависимости от сети
                chain_id = best_pair.get("chainId", "")
                
                jupiter_mint = None
                jupiter_decimals = None
                bsc_address = None
                matcha_address = None
                matcha_decimals = None
                dexes = []
                
                # Solana / Jupiter
                if chain_id == "solana" or dex == "jupiter":
                    jupiter_mint = address
                    jupiter_decimals = get_solana_token_decimals(address)
                    dexes.append("jupiter")
                
                # BSC / PancakeSwap
                if chain_id == "bsc" or dex == "pancake":
                    bsc_address = address
                    dexes.append("pancake")
                
                # Base / Matcha
                if chain_id == "base" or dex == "matcha":
                    matcha_address = address
                    matcha_decimals = 18
                    dexes.append("matcha")
                
                # Если DEX не определился автоматически
                if not dexes:
                    if dex == "jupiter":
                        jupiter_mint = address
                        jupiter_decimals = get_solana_token_decimals(address)
                        dexes = ["jupiter"]
                    elif dex == "pancake":
                        bsc_address = address
                        dexes = ["pancake"]
                    elif dex == "matcha":
                        matcha_address = address
                        matcha_decimals = 18
                        dexes = ["matcha"]
                
                # Проверяем/создаём Token (имя уже нормализовано)
                token = db.query(Token).filter(Token.name == token_name).first()
                if not token:
                    token = Token(
                        name=token_name,
                        base=token_symbol,
                        quote="USDT",
                        dexes=dexes,
                        jupiter_mint=jupiter_mint,
                        jupiter_decimals=jupiter_decimals,
                        bsc_address=bsc_address,
                        matcha_address=matcha_address,
                        matcha_decimals=matcha_decimals,
                        is_active=True
                    )
                    db.add(token)
                    db.commit()
                    db.refresh(token)
                
                # Проверяем нет ли уже в DefaultTokens
                existing_default = db.query(DefaultToken).filter(DefaultToken.token_id == token.id).first()
                if existing_default:
                    results["failed"].append({"address": address, "error": f"Токен {token_name} уже в списке по умолчанию"})
                    continue
                
                # Создаём ссылку в DefaultTokens
                default_token = DefaultToken(token_id=token.id)
                db.add(default_token)
                db.commit()
                
                results["success"].append({
                    "address": address,
                    "name": token_name,
                    "symbol": token_symbol,
                    "dexes": dexes
                })
                
        except Exception as e:
            results["failed"].append({"address": address, "error": str(e)})
    
    return results


@app.delete("/api/admin/default-tokens/{token_id}")
async def admin_delete_default_token(
    token_id: int,
    admin_token: str = Depends(get_admin_token),
    db: Session = Depends(get_db)
):
    """Удалить токен из списка по умолчанию (только админ).
    
    Удаляет только ссылку из DefaultTokens, сам Token остаётся.
    """
    default_token = db.query(DefaultToken).filter(DefaultToken.id == token_id).first()
    if not default_token:
        raise HTTPException(status_code=404, detail="Токен не найден в списке по умолчанию")
    
    db.delete(default_token)
    db.commit()
    return {"status": "deleted", "token_id": token_id}


# ============== Public: Get Default Tokens ==============

@app.get("/api/default-tokens")
async def get_default_tokens(db: Session = Depends(get_db)):
    """Получить список токенов по умолчанию (публичный эндпоинт для клиента)."""
    default_tokens = db.query(DefaultToken).all()
    result = []
    for dt in default_tokens:
        token = db.query(Token).filter(Token.id == dt.token_id, Token.is_active == True).first()
        if token:
            result.append({
                "name": token.name,
                "base": token.base,
                "quote": token.quote,
                "dexes": token.dexes or [],
                "jupiter_mint": token.jupiter_mint,
                "jupiter_decimals": token.jupiter_decimals,
                "bsc_address": token.bsc_address,
                "mexc_price_scale": token.mexc_price_scale,
                "matcha_address": token.matcha_address,
                "matcha_decimals": token.matcha_decimals,
                "cg_id": token.cg_id,
                "spread_direct": token.spread_direct,
                "spread_reverse": token.spread_reverse,
                "spread_threshold": token.spread_threshold,
                "spread_direct_threshold": token.spread_direct_threshold,
                "spread_reverse_threshold": token.spread_reverse_threshold
            })
    return {"tokens": result}


# ============== Public: Product Key Verification ==============

@app.post("/api/verify-key")
async def verify_product_key(
    data: ProductKeyVerify,
    db: Session = Depends(get_db)
):
    """Verify product key (public endpoint for client)."""
    key = db.query(ProductKey).filter(
        ProductKey.key == data.key,
        ProductKey.is_active == True
    ).first()
    
    if not key:
        raise HTTPException(status_code=401, detail="Invalid or inactive key")
    
    # Mark key as used if not already
    if not key.is_used:
        key.is_used = True
        key.used_at = datetime.utcnow()
        db.commit()
    
    return {
        "status": "valid",
        "username": key.username,
        "key": key.key
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
