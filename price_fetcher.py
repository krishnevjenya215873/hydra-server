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
    
    def _throttle(self):
        """Throttling disabled - each request uses its own proxy."""
        pass  # No throttling needed with proxy rotation
    
    def get_mexc_price(
        self, 
        db: Session,
        base: str, 
        quote: str = "USDT", 
        price_scale: Optional[int] = None,
        max_retries: int = 2
    ) -> Tuple[Optional[float], Optional[float]]:
        """Get futures price from MEXC (bid, ask). Uses httpx with proxy."""
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
    
    def get_pancake_price_usdt(self, db: Session, token_address: str, max_retries: int = 2) -> Optional[float]:
        """Get token price in USDT via DexScreener (PancakeSwap). Retries with different proxy on failure."""
        addr = (token_address or "").strip()
        if not addr:
            return None
        
        url = f"{DEXSCREENER_TOKENS_URL}/{addr}"
        
        for attempt in range(max_retries):
            client = self._get_client(db)
            self._throttle()
            
            try:
                resp = client.get(url, timeout=DEXSCREENER_TIMEOUT)
                if resp.status_code != 200:
                    logger.warning(f"Pancake: HTTP {resp.status_code} for {addr}, switching proxy (attempt {attempt + 1}/{max_retries})")
                    # Removed: proxy deactivation moved to health checker
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
                
                proxy_manager.mark_proxy_success(db)
                return float(best.get("priceUsd"))
                
            except Exception as e:
                logger.error(f"Pancake: error for {addr}: {e}")
                # Removed: proxy deactivation moved to health checker
                if attempt < max_retries - 1:
                    continue
        
        return None
    
    def get_jupiter_price_usdt(
        self, 
        db: Session,
        mint: str, 
        decimals: int,
        max_retries: int = 2
    ) -> Optional[float]:
        """Get token price in USDT via Jupiter. Uses httpx with proxy (no cloudscraper)."""
        mint = (mint or "").strip()
        if not mint or decimals is None or decimals < 0:
            return None
        
        try:
            usdt_amount_raw = int(JUPITER_USDT_AMOUNT * (Decimal(10) ** JUPITER_USDT_DECIMALS))
        except Exception as e:
            logger.error(f"Jupiter: error preparing USDT amount: {e}")
            return None
        
        # Use httpx with proxy (faster than cloudscraper)
        for attempt in range(max_retries):
            try:
                # Get proxy for this request
                proxy_url = proxy_manager.get_proxy_url(db)
                
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
                    # Removed: proxy deactivation moved to health checker
                    continue
                
                data = resp.json()
                out_amount_str = data.get("outAmount")
                
                if not out_amount_str:
                    logger.debug(f"Jupiter: no outAmount for mint={mint}")
                    return None
                
                out_amount_raw = int(out_amount_str)
                if out_amount_raw <= 0:
                    return None
                
                token_amount = Decimal(out_amount_raw) / (Decimal(10) ** decimals)
                if token_amount <= 0:
                    return None
                
                price = JUPITER_USDT_AMOUNT / token_amount
                
                proxy_manager.mark_proxy_success(db)
                logger.debug(f"Jupiter: 1 TOKEN ({mint}) = {float(price):.8f} USDT")
                return float(price)
                
            except Exception as e:
                logger.error(f"Jupiter: error for mint={mint}: {e}")
                # Removed: proxy deactivation moved to health checker
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
    
    def _refresh_matcha_jwt(self, db: Session) -> bool:
        """Refresh JWT token from Matcha API using cloudscraper with proxy. Returns True if successful."""
        # Get proxy for Matcha request FIRST
        proxy_url = proxy_manager.get_proxy_url(db)
        if not proxy_url:
            logger.warning("Matcha JWT: No proxy available, skipping request")
            return False
        
        proxies = {"http": proxy_url, "https": proxy_url}
        
        # DEBUG: Log all request details
        logger.info(f"=== MATCHA JWT REQUEST DEBUG ===")
        logger.info(f"URL: {MATCHA_JWT_URL}")
        logger.info(f"Proxy URL: {proxy_url}")
        logger.info(f"Proxies dict: {proxies}")
        logger.info(f"Headers: {MATCHA_HEADERS}")
        
        # Create FRESH scraper for each JWT request to avoid session contamination
        scraper = cloudscraper.create_scraper(
            browser={
                "browser": "chrome",
                "platform": "windows",
                "mobile": False
            },
            interpreter='native'
        )
        # Log cloudscraper version and interpreter info
        import ssl
        import sys
        logger.info(f"Python version: {sys.version}")
        logger.info(f"OpenSSL version: {ssl.OPENSSL_VERSION}")
        logger.info(f"Cloudscraper version: {cloudscraper.__version__}")
        logger.info(f"Scraper created: browser=chrome, platform=windows, mobile=False, interpreter=native")
        
        try:
            logger.info(f"Making GET request to {MATCHA_JWT_URL}...")
            resp = scraper.get(
                MATCHA_JWT_URL,
                headers=MATCHA_HEADERS,
                proxies=proxies,
                timeout=30.0,
            )
            logger.info(f"Response received: status={resp.status_code}, body_len={len(resp.text)}")
            logger.info(f"Response body (first 500 chars): {resp.text[:500]}")
            logger.info(f"=== END MATCHA JWT REQUEST DEBUG ===")
            
            if resp.status_code != 200:
                logger.warning(f"Matcha JWT: HTTP {resp.status_code}")
                # Removed: proxy deactivation moved to health checker
                return False
            
            data = resp.json()
            token = data.get("token")
            exp = data.get("exp", 0)
            
            if token:
                self._matcha_jwt_token = token
                # Refresh 10 seconds before expiry to be safe
                self._matcha_jwt_exp = exp - 10
                proxy_manager.mark_proxy_success(db)
                logger.info(f"Matcha JWT: obtained new token (valid for ~{exp - time.time():.0f}s)")
                return True
            else:
                logger.warning("Matcha JWT: no token in response")
                return False
                
        except Exception as e:
            logger.error(f"Matcha JWT: error getting token: {e}")
            # Removed: proxy deactivation moved to health checker
            return False
    
    def _get_matcha_jwt(self, db: Session) -> Optional[str]:
        """Get cached JWT token, refreshing if expired."""
        current_time = time.time()
        
        # Check if token is still valid
        if self._matcha_jwt_token and current_time < self._matcha_jwt_exp:
            return self._matcha_jwt_token
        
        # Token expired or doesn't exist - refresh
        if self._refresh_matcha_jwt(db):
            return self._matcha_jwt_token
        
        return None
    
    def get_matcha_price_usdt(
        self,
        db: Session,
        token_address: str,
        token_decimals: int = MATCHA_DEFAULT_SELL_DECIMALS,
        max_retries: int = 3
    ) -> Optional[float]:
        """Get token price in USDT via Matcha. Uses cached JWT token."""
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
                # Get cached or fresh JWT token
                jwt_token = self._get_matcha_jwt(db)
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
                
                # Get proxy for Matcha request
                proxy_url = proxy_manager.get_proxy_url(db)
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
            """Fetch MEXC price in separate thread with own DB session."""
            thread_db = models.SessionLocal()
            try:
                return self.get_mexc_price(thread_db, base, quote, token.mexc_price_scale)
            finally:
                thread_db.close()
        
        def fetch_jupiter():
            """Fetch Jupiter price in separate thread with own DB session."""
            if "jupiter" not in allowed or not token.jupiter_mint or token.jupiter_decimals is None:
                return None
            thread_db = models.SessionLocal()
            try:
                return self.get_jupiter_price_usdt(thread_db, token.jupiter_mint, token.jupiter_decimals)
            finally:
                thread_db.close()
        
        def fetch_pancake():
            """Fetch Pancake price in separate thread with own DB session."""
            if "pancake" not in allowed or not token.bsc_address:
                return None
            thread_db = models.SessionLocal()
            try:
                return self.get_pancake_price_usdt(thread_db, token.bsc_address)
            finally:
                thread_db.close()
        
        def fetch_matcha():
            """Fetch Matcha price in separate thread with own DB session."""
            if "matcha" not in allowed or not token.matcha_address:
                return None
            thread_db = models.SessionLocal()
            try:
                decimals = token.matcha_decimals or MATCHA_DEFAULT_SELL_DECIMALS
                return self.get_matcha_price_usdt(thread_db, token.matcha_address, decimals)
            finally:
                thread_db.close()
        
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
        
        return {
            "token_name": token.name,
            "mexc_price": (cex_bid, cex_ask),
            "spreads": spreads,
            "timestamp": time.time(),
        }


# Global price fetcher instance
price_fetcher = PriceFetcher()
