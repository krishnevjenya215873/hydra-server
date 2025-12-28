"""
Price fetcher module - handles all API requests to exchanges.
Runs on server side with proxy support.
OPTIMIZED: Parallel requests to CEX and DEX for maximum speed.
"""

import logging
import time
from decimal import Decimal
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import cloudscraper
# curl_cffi removed - using cloudscraper with older OpenSSL
from sqlalchemy.orm import Session

from models import Token, SpreadHistory
from proxy_manager import proxy_manager

logger = logging.getLogger(__name__)

# API Configuration
DEXSCREENER_TIMEOUT = 10.0
DEXSCREENER_TOKENS_URL = "https://api.dexscreener.com/latest/dex/tokens"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"

JUPITER_QUOTE_URL = "https://ultra-api.jup.ag/order"
JUPITER_USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
JUPITER_USDT_DECIMALS = 6
JUPITER_USDT_AMOUNT = Decimal("100")

# Jupiter price cache
_jupiter_price_cache: Dict[str, Tuple[float, float]] = {}  # mint -> (price, timestamp)
JUPITER_CACHE_TTL = 1.0  # Cache valid for 1 second

MEXC_FUTURES_BASE = "https://contract.mexc.com"

MATCHA_JWT_URL = "https://matcha.xyz/api/jwt"
MATCHA_PRICE_URL = "https://matcha.xyz/api/gasless/price"
MATCHA_USDT = "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"
MATCHA_USDT_AMOUNT = Decimal("100")
MATCHA_CHAIN_ID = 8453
MATCHA_USDT_DECIMALS = 6
MATCHA_DEFAULT_SELL_DECIMALS = 18

MATCHA_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Priority": "u=0, i",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": '"Google Chrome";v="143", "Chromium";v="143", "Not:A-Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}


class PriceFetcher:
    """Fetches prices from various exchanges with proxy support."""
    
    def __init__(self):
        self._last_request_time = 0
        self._min_request_interval = 0.0  # NO throttling - parallel requests use different proxies
        
        # Matcha JWT token cache
        self._matcha_jwt_token: Optional[str] = None
        self._matcha_jwt_exp: float = 0  # Unix timestamp when token expires
        self._matcha_scraper = None  # Reusable scraper for Matcha
    
    def _get_client(self, db: Session):
        """Get HTTP client with proxy configuration - creates new client each time for proper proxy rotation."""
        # Get proxy for this request
        proxy_url = proxy_manager.get_proxy_url(db)
        
        # Create new scraper with proxy
        client = cloudscraper.create_scraper(
            browser={
                "browser": "chrome",
                "platform": "windows",
                "mobile": False
            }
        )
        
        if proxy_url:
            client.proxies = {"http": proxy_url, "https": proxy_url}
            logger.debug(f"Using proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
        else:
            logger.warning("No proxy available, making direct request")
        
        return client
    
    def _get_client_cached(self):
        """Get HTTP client with proxy from cache - NO DB session required.
        OPTIMIZED: Uses httpx instead of cloudscraper to reduce memory usage.
        """
        # Get proxy from cache (no DB required)
        proxy_url = proxy_manager.get_proxy_url_cached()
        
        # Use lightweight httpx client instead of heavy cloudscraper
        transport = None
        if proxy_url:
            transport = httpx.HTTPTransport(proxy=proxy_url)
        
        return httpx.Client(
            timeout=10.0,
            transport=transport,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
    
    def _throttle(self):
        """Throttling disabled - each request uses its own proxy."""
        pass  # No throttling needed with proxy rotation
    
    # MEXC price cache for batch fetching
    _mexc_prices_cache: Dict[str, Tuple[float, float]] = {}
    _mexc_cache_time: float = 0
    _mexc_cache_ttl: float = 1.0  # Cache valid for 1 second
    
    # MEXC contract details cache (for limits)
    _mexc_contracts_cache: Dict[str, Dict[str, Any]] = {}
    _mexc_contracts_cache_time: float = 0
    _mexc_contracts_cache_ttl: float = 60.0  # Cache valid for 60 seconds (contract details don't change often)
    
    def get_all_mexc_prices(self, max_retries: int = 2) -> Dict[str, Tuple[float, float]]:
        """Get ALL futures prices from MEXC in ONE request. Returns dict of symbol -> (bid, ask).
        OPTIMIZED: No DB session required - uses cached proxy list.
        """
        current_time = time.time()
        
        # Return cached data if still valid
        if current_time - self._mexc_cache_time < self._mexc_cache_ttl and self._mexc_prices_cache:
            return self._mexc_prices_cache
        
        for attempt in range(max_retries):
            try:
                # Get proxy from cache (no DB required)
                proxy_url = proxy_manager.get_proxy_url_cached()
                transport = None
                if proxy_url:
                    transport = httpx.HTTPTransport(proxy=proxy_url)
                
                with httpx.Client(timeout=10.0, transport=transport) as client:
                    r = client.get(f"{MEXC_FUTURES_BASE}/api/v1/contract/ticker")
                
                if r.status_code != 200:
                    logger.warning(f"MEXC batch: HTTP {r.status_code} (attempt {attempt + 1}/{max_retries})")
                    continue
                
                j = r.json()
                
                if j.get("success") and j.get("code") == 0 and j.get("data"):
                    prices = {}
                    for item in j["data"]:
                        symbol = item.get("symbol")
                        bid = item.get("bid1")
                        ask = item.get("ask1")
                        if symbol and bid is not None and ask is not None:
                            prices[symbol] = (float(bid), float(ask))
                    
                    # Update cache
                    self._mexc_prices_cache = prices
                    self._mexc_cache_time = current_time
                    logger.debug(f"MEXC batch: fetched {len(prices)} prices")
                    return prices
                
                logger.warning(f"MEXC batch: unsuccessful response: {j}")
                return {}
                
            except Exception as e:
                logger.error(f"MEXC batch: error: {e}")
                if attempt < max_retries - 1:
                    continue
        
        return {}
    
    def get_all_mexc_contracts(self, max_retries: int = 2) -> Dict[str, Dict[str, Any]]:
        """Get ALL futures contract details from MEXC in ONE request.
        Returns dict of symbol -> {contractSize, minVol, maxVol, volUnit}.
        Used to calculate order limits in USDT.
        """
        current_time = time.time()
        
        # Return cached data if still valid
        if current_time - self._mexc_contracts_cache_time < self._mexc_contracts_cache_ttl and self._mexc_contracts_cache:
            return self._mexc_contracts_cache
        
        for attempt in range(max_retries):
            try:
                # Get proxy from cache (no DB required)
                proxy_url = proxy_manager.get_proxy_url_cached()
                transport = None
                if proxy_url:
                    transport = httpx.HTTPTransport(proxy=proxy_url)
                
                with httpx.Client(timeout=10.0, transport=transport) as client:
                    r = client.get(f"{MEXC_FUTURES_BASE}/api/v1/contract/detail")
                
                if r.status_code != 200:
                    logger.warning(f"MEXC contracts: HTTP {r.status_code} (attempt {attempt + 1}/{max_retries})")
                    continue
                
                j = r.json()
                
                if j.get("success") and j.get("code") == 0 and j.get("data"):
                    contracts = {}
                    data = j["data"]
                    
                    # API returns single contract or list depending on whether symbol param was passed
                    if isinstance(data, dict):
                        data = [data]
                    
                    for item in data:
                        symbol = item.get("symbol")
                        if symbol:
                            contracts[symbol] = {
                                "contractSize": float(item.get("contractSize", 1)),
                                "minVol": int(item.get("minVol", 1)),
                                "maxVol": int(item.get("maxVol", 1000000)),
                                "volUnit": int(item.get("volUnit", 1)),
                            }
                    
                    # Update cache
                    self._mexc_contracts_cache = contracts
                    self._mexc_contracts_cache_time = current_time
                    logger.debug(f"MEXC contracts: fetched {len(contracts)} contract details")
                    return contracts
                
                logger.warning(f"MEXC contracts: unsuccessful response: {j}")
                return {}
                
            except Exception as e:
                logger.error(f"MEXC contracts: error: {e}")
                if attempt < max_retries - 1:
                    continue
        
        return {}
    
    def get_mexc_limit_usdt(self, base: str, quote: str = "USDT", current_price: Optional[float] = None) -> Optional[float]:
        """Calculate minimum order limit in USDT for a MEXC futures contract.
        
        Formula: minVol * contractSize * price
        
        Returns minimum order size in USDT, or None if data unavailable.
        """
        import re
        # Clean base symbol
        clean_base = re.sub(r'[$#@!%^&*()\-+=/\\|<>?~`]', '', base).strip()
        symbol = f"{clean_base.upper()}_{quote.upper()}"
        
        # Get contract details from cache
        contracts = self.get_all_mexc_contracts()
        if symbol not in contracts:
            logger.debug(f"MEXC limit: no contract data for {symbol}")
            return None
        
        contract = contracts[symbol]
        contract_size = contract.get("contractSize", 1)
        min_vol = contract.get("minVol", 1)
        
        # Get current price if not provided
        if current_price is None:
            prices = self._mexc_prices_cache
            if symbol in prices:
                bid, ask = prices[symbol]
                current_price = (bid + ask) / 2 if bid and ask else bid or ask
        
        if current_price is None or current_price <= 0:
            logger.debug(f"MEXC limit: no price for {symbol}")
            return None
        
        # Calculate minimum order in USDT
        min_usdt = min_vol * contract_size * current_price
        
        logger.debug(f"MEXC limit: {symbol} minVol={min_vol} contractSize={contract_size} price={current_price:.6f} -> min={min_usdt:.2f} USDT")
        return round(min_usdt, 2)
    
    def get_mexc_price_from_cache(self, base: str, quote: str = "USDT", price_scale: Optional[int] = None) -> Tuple[Optional[float], Optional[float]]:
        """Get MEXC price from cached batch data.
        Автоматически очищает специальные символы ($, # и т.д.) из имени токена.
        """
        import re
        # Удаляем специальные символы из имени токена
        clean_base = re.sub(r'[$#@!%^&*()\-+=/\\|<>?~`]', '', base).strip()
        symbol = f"{clean_base.upper()}_{quote.upper()}"
        if symbol in self._mexc_prices_cache:
            bid, ask = self._mexc_prices_cache[symbol]
            if isinstance(price_scale, int) and price_scale >= 0:
                bid = round(bid, price_scale)
                ask = round(ask, price_scale)
            return bid, ask
        return None, None
    
    def get_mexc_price(
        self, 
        db: Session,
        base: str, 
        quote: str = "USDT", 
        price_scale: Optional[int] = None,
        max_retries: int = 2
    ) -> Tuple[Optional[float], Optional[float]]:
        """Get futures price from MEXC (bid, ask). Now uses batch cache."""
        # First try to get from cache
        cached = self.get_mexc_price_from_cache(base, quote, price_scale)
        if cached[0] is not None:
            return cached
        
        # If not in cache, fetch all prices and try again
        self.get_all_mexc_prices(db, max_retries)
        return self.get_mexc_price_from_cache(base, quote, price_scale)
    
    def get_mexc_price_single(
        self, 
        db: Session,
        base: str, 
        quote: str = "USDT", 
        price_scale: Optional[int] = None,
        max_retries: int = 2
    ) -> Tuple[Optional[float], Optional[float]]:
        """Get futures price from MEXC (bid, ask) - single request fallback. Uses httpx with proxy."""
        symbol = f"{base.upper()}_{quote.upper()}"
        
        for attempt in range(max_retries):
            try:
                # Get proxy for this request
                proxy_url = proxy_manager.get_proxy_url(db)
                
                # Configure httpx with proxy
                transport = None
                if proxy_url:
                    transport = httpx.HTTPTransport(proxy=proxy_url)
                
                with httpx.Client(timeout=5.0, transport=transport) as client:
                    r = client.get(
                        f"{MEXC_FUTURES_BASE}/api/v1/contract/ticker",
                        params={"symbol": symbol},
                    )
                
                # Check HTTP status first
                if r.status_code != 200:
                    logger.warning(f"MEXC: HTTP {r.status_code} for {symbol} (attempt {attempt + 1}/{max_retries})")
                    # Removed: proxy deactivation moved to health checker
                    continue
                
                j = r.json()
                
                if j.get("success") and j.get("code") == 0 and j.get("data"):
                    data = j["data"]
                    bid = data.get("bid1")
                    ask = data.get("ask1")
                    
                    bid_val = float(bid) if bid is not None else None
                    ask_val = float(ask) if ask is not None else None
                    
                    if isinstance(price_scale, int) and price_scale >= 0:
                        if bid_val is not None:
                            bid_val = round(bid_val, price_scale)
                        if ask_val is not None:
                            ask_val = round(ask_val, price_scale)
                    
                    proxy_manager.mark_proxy_success(db)
                    logger.debug(f"MEXC: {symbol} bid={bid_val}, ask={ask_val}")
                    return bid_val, ask_val
                
                # API error (rate limit, etc.)
                error_code = j.get("code")
                if error_code == 510:  # Rate limit
                    logger.warning(f"MEXC: rate limit for {symbol} (attempt {attempt + 1}/{max_retries})")
                    # Removed: proxy deactivation moved to health checker
                    continue
                
                logger.warning(f"MEXC: unsuccessful response for {symbol}: {j}")
                return None, None
                
            except Exception as e:
                logger.error(f"MEXC: error for {symbol}: {e}")
                # Removed: proxy deactivation moved to health checker
                if attempt < max_retries - 1:
                    continue
        
        return None, None
    
    def get_pancake_price_usdt(self, token_address: str, max_retries: int = 2) -> Optional[float]:
        """Get token price in USDT via DexScreener (PancakeSwap). Retries with different proxy on failure.
        OPTIMIZED: No DB session required - uses cached proxy list.
        """
        addr = (token_address or "").strip()
        if not addr:
            return None
        
        url = f"{DEXSCREENER_TOKENS_URL}/{addr}"
        
        for attempt in range(max_retries):
            try:
                client = self._get_client_cached()  # No DB required
                try:
                    resp = client.get(url, timeout=DEXSCREENER_TIMEOUT)
                finally:
                    client.close()  # Always close to free memory
                    
                if resp.status_code != 200:
                    logger.warning(f"Pancake: HTTP {resp.status_code} for {addr}, switching proxy (attempt {attempt + 1}/{max_retries})")
                    continue
            
                data = resp.json()
                pairs = data.get("pairs") or []
                
                if not isinstance(pairs, list) or not pairs:
                    logger.debug(f"Pancake: no markets for token {addr}")
                    return None
                
                def liq_usd_val(p: dict) -> float:
                    liq = p.get("liquidity") or {}
                    try:
                        return float(liq.get("usd") or 0.0)
                    except Exception:
                        return 0.0
                
                pancake_pairs = []
                best_any_pair = None
                
                for pair in pairs:
                    dex_id = str(pair.get("dexId", "")).lower()
                    price_str = pair.get("priceUsd")
                    if price_str is None:
                        continue
                    
                    try:
                        price_val = float(price_str)
                    except Exception:
                        continue
                    
                    if price_val <= 0 or price_val > 1_000_000:
                        continue
                    
                    liq_val = liq_usd_val(pair)
                    if liq_val <= 0:
                        continue
                    
                    if "pancake" in dex_id:
                        pancake_pairs.append(pair)
                    
                    if best_any_pair is None or liq_val > liq_usd_val(best_any_pair):
                        best_any_pair = pair
                
                if pancake_pairs:
                    best = max(pancake_pairs, key=liq_usd_val)
                elif best_any_pair is not None:
                    best = best_any_pair
                else:
                    return None
                
                return float(best.get("priceUsd"))
                
            except Exception as e:
                logger.error(f"Pancake: error for {addr}: {e}")
                if attempt < max_retries - 1:
                    continue
        
        return None
    
    def get_jupiter_price_usdt(
        self, 
        mint: str, 
        decimals: int,
        max_retries: int = 2
    ) -> Optional[float]:
        """Get token price in USDT via Jupiter Quote API. Uses httpx with proxy and caching.
        OPTIMIZED: No DB session required - uses cached proxy list.
        """
        mint = (mint or "").strip()
        if not mint:
            return None
        
        # Check cache first
        current_time = time.time()
        if mint in _jupiter_price_cache:
            cached_price, cached_time = _jupiter_price_cache[mint]
            if current_time - cached_time < JUPITER_CACHE_TTL:
                return cached_price
        
        try:
            usdt_amount_raw = int(JUPITER_USDT_AMOUNT * (Decimal(10) ** JUPITER_USDT_DECIMALS))
        except Exception as e:
            logger.error(f"Jupiter: error preparing USDT amount: {e}")
            return None
        
        # Use httpx with proxy (faster than cloudscraper)
        for attempt in range(max_retries):
            try:
                # Get proxy from cache (no DB required)
                proxy_url = proxy_manager.get_proxy_url_cached()
                
                # Configure httpx with proxy
                transport = None
                if proxy_url:
                    transport = httpx.HTTPTransport(proxy=proxy_url)
                
                with httpx.Client(timeout=5.0, transport=transport) as client:
                    resp = client.get(
                        JUPITER_QUOTE_URL,
                        params={
                            "inputMint": JUPITER_USDT_MINT,
                            "outputMint": mint,
                            "amount": str(usdt_amount_raw),
                            "swapMode": "ExactIn",
                        },
                    )
                
                if resp.status_code != 200:
                    logger.warning(f"Jupiter: HTTP {resp.status_code} for mint={mint} (attempt {attempt + 1}/{max_retries})")
                    continue
                
                data = resp.json()
                out_amount_str = data.get("outAmount")
                
                if not out_amount_str:
                    logger.debug(f"Jupiter: no outAmount for mint={mint}")
                    return None
                
                # Check priceImpact - if > 100%, it's an anomaly (liquidity issue)
                price_impact = data.get("priceImpact", 0)
                if price_impact and float(price_impact) > 100:
                    logger.warning(f"Jupiter ANOMALY (priceImpact): mint={mint[:12]}... priceImpact={price_impact}% > 100% - using cached")
                    if mint in _jupiter_price_cache:
                        cached_price, _ = _jupiter_price_cache[mint]
                        if cached_price > 0.0000001:
                            return cached_price
                    return None
                
                out_amount_raw = int(out_amount_str)
                if out_amount_raw <= 0:
                    return None
                
                token_amount = Decimal(out_amount_raw) / (Decimal(10) ** decimals)
                if token_amount <= 0:
                    return None
                
                price = JUPITER_USDT_AMOUNT / token_amount
                price_float = float(price)
                
                # ABSOLUTE validation - reject obviously wrong prices
                # If price is less than $0.0000001 (10^-7), it's almost certainly an anomaly
                if price_float < 0.0000001:
                    logger.warning(f"Jupiter ANOMALY (absolute): mint={mint[:12]}... price=${price_float:.12f} is too low - skipping")
                    # Return cached value if available, otherwise None
                    if mint in _jupiter_price_cache:
                        cached_price, _ = _jupiter_price_cache[mint]
                        if cached_price > 0.0000001:
                            return cached_price
                    return None
                
                # Cache the result - relative validation moved to fetch_token_data
                # where we have access to MEXC price for cross-validation
                _jupiter_price_cache[mint] = (price_float, time.time())
                
                # Detailed logging for debugging decimals issues
                logger.info(f"Jupiter PRICE: mint={mint[:12]}... decimals={decimals} outAmount={out_amount_raw} price=${price_float:.8f}")
                return price_float
                
            except Exception as e:
                logger.error(f"Jupiter: error for mint={mint}: {e}")
                if attempt < max_retries - 1:
                    continue
        
        return None
    
    def _get_matcha_scraper(self):
        """Get or create reusable cloudscraper client for Matcha requests."""
        if self._matcha_scraper is None:
            self._matcha_scraper = cloudscraper.create_scraper(
                browser={
                    "browser": "chrome",
                    "platform": "windows",
                    "mobile": False
                }
            )
        return self._matcha_scraper
    
    def _refresh_matcha_jwt(self) -> bool:
        """Refresh JWT token from Matcha API using cloudscraper with proxy.
        OPTIMIZED: No DB session required - uses cached proxy list.
        """
        # Get proxy from cache (no DB required)
        proxy_url = proxy_manager.get_proxy_url_cached()
        proxies = None
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}
        
        # Create FRESH scraper for each JWT request
        scraper = cloudscraper.create_scraper(
            browser={
                "browser": "chrome",
                "platform": "windows",
                "mobile": False
            }
        )
        
        try:
            resp = scraper.get(
                MATCHA_JWT_URL,
                headers=MATCHA_HEADERS,
                proxies=proxies,
                timeout=30.0,
            )
            
            if resp.status_code != 200:
                logger.warning(f"Matcha JWT: HTTP {resp.status_code}")
                return False
            
            data = resp.json()
            token = data.get("token")
            exp = data.get("exp", 0)
            
            if token:
                self._matcha_jwt_token = token
                self._matcha_jwt_exp = exp - 10
                logger.info(f"Matcha JWT: obtained new token (valid for ~{exp - time.time():.0f}s)")
                return True
            else:
                logger.warning("Matcha JWT: no token in response")
                return False
                
        except Exception as e:
            logger.error(f"Matcha JWT: error getting token: {e}")
            return False
    
    def _get_matcha_jwt(self) -> Optional[str]:
        """Get cached JWT token, refreshing if expired.
        OPTIMIZED: No DB session required.
        """
        current_time = time.time()
        
        # Check if token is still valid
        if self._matcha_jwt_token and current_time < self._matcha_jwt_exp:
            return self._matcha_jwt_token
        
        # Token expired or doesn't exist - refresh
        if self._refresh_matcha_jwt():
            return self._matcha_jwt_token
        
        return None
    
    def get_matcha_price_usdt(
        self,
        token_address: str,
        token_decimals: int = MATCHA_DEFAULT_SELL_DECIMALS,
        max_retries: int = 3
    ) -> Optional[float]:
        """Get token price in USDT via Matcha. Uses cached JWT token.
        OPTIMIZED: No DB session required - uses cached proxy list.
        """
        token_address = (token_address or "").strip()
        if not token_address:
            return None
        
        try:
            usdt_amount_raw = int(MATCHA_USDT_AMOUNT * (Decimal(10) ** MATCHA_USDT_DECIMALS))
        except Exception as e:
            logger.error(f"Matcha: error preparing USDT amount: {e}")
            return None
        
        for attempt in range(max_retries):
            try:
                # Get cached or fresh JWT token (no DB required)
                jwt_token = self._get_matcha_jwt()
                if not jwt_token:
                    logger.warning(f"Matcha: failed to get JWT (attempt {attempt + 1}/{max_retries})")
                    # Reset scraper and try again
                    self._matcha_scraper = None
                    time.sleep(1)
                    continue
                
                # Make price request with JWT in header using cloudscraper with proxy
                scraper = self._get_matcha_scraper()
                headers = MATCHA_HEADERS.copy()
                headers["X-Matcha-Jwt"] = jwt_token
                
                # Get proxy from cache (no DB required)
                proxy_url = proxy_manager.get_proxy_url_cached()
                proxies = None
                if proxy_url:
                    proxies = {"http": proxy_url, "https": proxy_url}
                
                resp = scraper.get(
                    MATCHA_PRICE_URL,
                    params={
                        "chainId": MATCHA_CHAIN_ID,
                        "sellToken": MATCHA_USDT,
                        "buyToken": token_address,
                        "sellAmount": str(usdt_amount_raw),
                    },
                    headers=headers,
                    proxies=proxies,
                    timeout=15.0,
                )
                
                # If 401/403, token might be invalid - force refresh
                if resp.status_code in (401, 403):
                    logger.warning(f"Matcha: HTTP {resp.status_code} - forcing JWT refresh")
                    self._matcha_jwt_token = None
                    self._matcha_jwt_exp = 0
                    time.sleep(0.5)
                    continue
                
                if resp.status_code != 200:
                    logger.warning(f"Matcha: HTTP {resp.status_code} for {token_address} (attempt {attempt + 1}/{max_retries})")
                    time.sleep(1)
                    continue
                
                data = resp.json()
                buy_amount_str = data.get("buyAmount")
                
                if not buy_amount_str:
                    logger.debug(f"Matcha: no buyAmount for {token_address}")
                    return None
                
                buy_amount_raw = int(buy_amount_str)
                if buy_amount_raw <= 0:
                    return None
                
                token_amount = Decimal(buy_amount_raw) / (Decimal(10) ** token_decimals)
                if token_amount <= 0:
                    return None
                
                price = MATCHA_USDT_AMOUNT / token_amount
                
                logger.info(f"Matcha: {token_address} price = {float(price):.8f} USDT")
                return float(price)
                
            except Exception as e:
                logger.error(f"Matcha: error for {token_address}: {e}")
                # Reset scraper on error
                self._matcha_scraper = None
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
        
        return None
    
    def calc_spread(
        self, 
        cex_bid: float, 
        cex_ask: float, 
        dex_price: float
    ) -> Tuple[Optional[float], Optional[float]]:
        """Calculate direct and reverse spread."""
        if dex_price is None or dex_price <= 0:
            return None, None
        
        direct = None
        if cex_bid and cex_bid > 0:
            direct = (cex_bid - dex_price) / dex_price * 100.0
        
        reverse = None
        if cex_ask and cex_ask > 0:
            reverse = (dex_price - cex_ask) / cex_ask * 100.0
        
        return direct, reverse
    
    def fetch_token_data(self, db: Session, token: Token) -> Dict[str, Any]:
        """
        Fetch all price data for a token.
        OPTIMIZED: All API calls (MEXC + DEXes) run in PARALLEL for maximum speed.
        """
        import models  # For SessionLocal
        
        base = token.base.upper()
        quote = token.quote.upper()
        
        # Determine which DEXes to use
        allowed = set(token.dexes) if token.dexes else set()
        
        if not token.dexes:
            if token.bsc_address:
                allowed.add("pancake")
            if token.jupiter_mint:
                allowed.add("jupiter")
            if token.matcha_address:
                allowed.add("matcha")
            if not allowed:
                allowed = {"pancake", "jupiter", "matcha"}
        
        # Prepare all fetch tasks
        results = {}
        
        def fetch_mexc():
            """Get MEXC price from cache (batch fetched in worker)."""
            # OPTIMIZED: MEXC prices are pre-fetched in batch by worker
            # Just get from cache - no network request needed
            # Use mexc_symbol if set, otherwise fall back to base
            mexc_base = token.mexc_symbol if token.mexc_symbol else base
            return self.get_mexc_price_from_cache(mexc_base, quote, token.mexc_price_scale)
        
        def fetch_jupiter():
            """Fetch Jupiter price - NO DB session required."""
            if "jupiter" not in allowed or not token.jupiter_mint or token.jupiter_decimals is None:
                return None
            return self.get_jupiter_price_usdt(token.jupiter_mint, token.jupiter_decimals)
        
        def fetch_pancake():
            """Fetch Pancake price - NO DB session required."""
            if "pancake" not in allowed or not token.bsc_address:
                return None
            return self.get_pancake_price_usdt(token.bsc_address)
        
        def fetch_matcha():
            """Fetch Matcha price - NO DB session required."""
            if "matcha" not in allowed or not token.matcha_address:
                return None
            decimals = token.matcha_decimals or MATCHA_DEFAULT_SELL_DECIMALS
            return self.get_matcha_price_usdt(token.matcha_address, decimals)
        
        # Execute ALL requests in PARALLEL
        cex_bid, cex_ask = None, None
        jupiter_price = None
        pancake_price = None
        matcha_price = None
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(fetch_mexc): "mexc",
                executor.submit(fetch_jupiter): "jupiter",
                executor.submit(fetch_pancake): "pancake",
                executor.submit(fetch_matcha): "matcha",
            }
            
            for future in as_completed(futures):
                task_name = futures[future]
                try:
                    result = future.result(timeout=15)
                    if task_name == "mexc" and result:
                        cex_bid, cex_ask = result
                    elif task_name == "jupiter":
                        jupiter_price = result
                    elif task_name == "pancake":
                        pancake_price = result
                    elif task_name == "matcha":
                        matcha_price = result
                except Exception as e:
                    logger.error(f"Error in parallel fetch {task_name}: {e}")
        
        # Calculate spreads
        spreads = {}
        
        # Cross-validate Jupiter price with MEXC to detect anomalies
        if jupiter_price and cex_bid and cex_ask:
            mexc_mid = (cex_bid + cex_ask) / 2
            if mexc_mid > 0:
                jupiter_vs_mexc_diff = abs(jupiter_price - mexc_mid) / mexc_mid
                # If Jupiter differs from MEXC by more than 50%, it's likely an anomaly
                if jupiter_vs_mexc_diff > 0.5:
                    # Check if we have a cached price that's closer to MEXC
                    mint = token.jupiter_mint
                    if mint and mint in _jupiter_price_cache:
                        cached_price, _ = _jupiter_price_cache[mint]
                        cached_vs_mexc_diff = abs(cached_price - mexc_mid) / mexc_mid
                        # If cached price is closer to MEXC, use it
                        if cached_vs_mexc_diff < jupiter_vs_mexc_diff:
                            logger.warning(f"Jupiter ANOMALY (cross-validation): {token.name} jupiter=${jupiter_price:.8f} mexc_mid=${mexc_mid:.8f} diff={jupiter_vs_mexc_diff*100:.1f}% - using cached=${cached_price:.8f}")
                            jupiter_price = cached_price
                        else:
                            # New price is closer to MEXC - this is a real price change!
                            logger.info(f"Jupiter PRICE CHANGE: {token.name} new=${jupiter_price:.8f} is closer to mexc_mid=${mexc_mid:.8f} than cached=${cached_price:.8f}")
                    else:
                        # No cache - this might be first fetch with anomaly, skip it
                        logger.warning(f"Jupiter ANOMALY (no cache): {token.name} jupiter=${jupiter_price:.8f} mexc_mid=${mexc_mid:.8f} diff={jupiter_vs_mexc_diff*100:.1f}% - skipping")
                        jupiter_price = None
        
        if pancake_price:
            d, r = self.calc_spread(cex_bid, cex_ask, pancake_price)
            spreads["pancake"] = {
                "direct": d,
                "reverse": r,
                "dex_price": pancake_price,
                "cex_bid": cex_bid,
                "cex_ask": cex_ask,
            }
        
        if jupiter_price:
            d, r = self.calc_spread(cex_bid, cex_ask, jupiter_price)
            spreads["jupiter"] = {
                "direct": d,
                "reverse": r,
                "dex_price": jupiter_price,
                "cex_bid": cex_bid,
                "cex_ask": cex_ask,
            }
        
        if matcha_price:
            d, r = self.calc_spread(cex_bid, cex_ask, matcha_price)
            spreads["matcha"] = {
                "direct": d,
                "reverse": r,
                "dex_price": matcha_price,
                "cex_bid": cex_bid,
                "cex_ask": cex_ask,
            }
        
        # Get MEXC order limit in USDT
        mexc_base = token.mexc_symbol if token.mexc_symbol else base
        mexc_mid_price = (cex_bid + cex_ask) / 2 if cex_bid and cex_ask else None
        mexc_limit = self.get_mexc_limit_usdt(mexc_base, quote, mexc_mid_price)
        
        return {
            "token_name": token.name,
            "mexc_price": (cex_bid, cex_ask),
            "mexc_limit": mexc_limit,
            "spreads": spreads,
            "timestamp": time.time(),
        }


# Global price fetcher instance
price_fetcher = PriceFetcher()
