"""
Pydantic schemas for API request/response validation.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ============== Token Schemas ==============

class TokenBase(BaseModel):
    name: str = Field(..., description="Token pair name, e.g., SOL-USDT")
    base: str = Field(..., description="Base currency symbol")
    quote: str = Field(default="USDT", description="Quote currency symbol")
    dexes: Optional[List[str]] = Field(default=None, description="List of DEXes to use")
    jupiter_mint: Optional[str] = None
    jupiter_decimals: Optional[int] = None
    bsc_address: Optional[str] = None
    mexc_price_scale: Optional[int] = None
    matcha_address: Optional[str] = None
    matcha_decimals: Optional[int] = None
    cg_id: Optional[str] = None
    spread_direct: bool = True
    spread_reverse: bool = True
    spread_threshold: Optional[float] = None
    spread_direct_threshold: Optional[float] = None
    spread_reverse_threshold: Optional[float] = None


class TokenCreate(TokenBase):
    pass


class TokenUpdate(BaseModel):
    name: Optional[str] = None
    base: Optional[str] = None
    quote: Optional[str] = None
    dexes: Optional[List[str]] = None
    jupiter_mint: Optional[str] = None
    jupiter_decimals: Optional[int] = None
    bsc_address: Optional[str] = None
    mexc_price_scale: Optional[int] = None
    matcha_address: Optional[str] = None
    matcha_decimals: Optional[int] = None
    cg_id: Optional[str] = None
    spread_direct: Optional[bool] = None
    spread_reverse: Optional[bool] = None
    spread_threshold: Optional[float] = None
    spread_direct_threshold: Optional[float] = None
    spread_reverse_threshold: Optional[float] = None
    is_active: Optional[bool] = None


class TokenResponse(TokenBase):
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ============== Proxy Schemas ==============

class ProxyBase(BaseModel):
    proxy_string: str = Field(..., description="Proxy in format login:pass@ip:port")
    protocol: str = Field(default="socks5", description="Protocol: socks5 or http")


class ProxyCreate(ProxyBase):
    pass


class ProxyBulkCreate(BaseModel):
    proxies: List[str] = Field(..., description="List of proxy strings")
    protocol: str = Field(default="socks5", description="Protocol for all proxies")


class ProxyResponse(ProxyBase):
    id: int
    is_active: bool
    last_used: Optional[datetime]
    fail_count: int
    created_at: datetime
    
    class Config:
        from_attributes = True


# ============== Spread Data Schemas ==============

class SpreadData(BaseModel):
    direct: Optional[float] = None
    reverse: Optional[float] = None
    dex_price: Optional[float] = None
    cex_bid: Optional[float] = None
    cex_ask: Optional[float] = None


class TokenSpreadUpdate(BaseModel):
    """Real-time spread update for a token."""
    token_name: str
    mexc_price: tuple = Field(default=(None, None), description="(bid, ask)")
    spreads: Dict[str, SpreadData] = Field(default_factory=dict)
    timestamp: float


class SpreadHistoryPoint(BaseModel):
    timestamp: float
    direct_spread: Optional[float]
    reverse_spread: Optional[float]


class SpreadHistoryResponse(BaseModel):
    token_name: str
    dex_name: str
    history: List[SpreadHistoryPoint]


# ============== WebSocket Messages ==============

class WSMessage(BaseModel):
    type: str  # "subscribe", "unsubscribe", "data", "error"
    payload: Any


class WSSubscribe(BaseModel):
    tokens: List[str]  # List of token names to subscribe


class WSDataUpdate(BaseModel):
    type: str = "data"
    data: Dict[str, TokenSpreadUpdate]


# ============== Admin Schemas ==============

class AdminLogin(BaseModel):
    username: str
    password: str


class AdminToken(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ============== Product Key Schemas ==============

class ProductKeyCreate(BaseModel):
    username: str = Field(..., description="Username for the key")


class ProductKeyResponse(BaseModel):
    id: int
    username: str
    key: str
    is_active: bool
    is_used: bool
    used_at: Optional[datetime]
    created_at: datetime
    
    class Config:
        from_attributes = True


class ProductKeyVerify(BaseModel):
    key: str = Field(..., description="Product key to verify")


class ServerStats(BaseModel):
    total_tokens: int
    active_tokens: int
    total_proxies: int
    active_proxies: int
    connected_clients: int
    uptime_seconds: float
